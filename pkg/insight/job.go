package insight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	esSinkClient "github.com/kaytu-io/kaytu-engine/services/es-sink/client"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-errors/errors"
	awsmodel "github.com/kaytu-io/kaytu-aws-describer/aws/model"
	azuremodel "github.com/kaytu-io/kaytu-azure-describer/azure/model"
	authApi "github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	describeClient "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	"github.com/kaytu-io/kaytu-engine/pkg/httpclient"
	"github.com/kaytu-io/kaytu-engine/pkg/insight/api"
	"github.com/kaytu-io/kaytu-engine/pkg/insight/es"
	inventoryClient "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
	onboardApi "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	"github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-util/pkg/config"
	es2 "github.com/kaytu-io/kaytu-util/pkg/es"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"go.uber.org/zap"
)

var DoInsightJobsCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "kaytu",
	Subsystem: "insight_worker",
	Name:      "do_insight_jobs_total",
	Help:      "Count of done insight jobs in insight-worker service",
}, []string{"queryid", "status"})

var DoInsightJobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "kaytu",
	Subsystem: "insight_worker",
	Name:      "do_insight_jobs_duration_seconds",
	Help:      "Duration of done insight jobs in insight-worker service",
	Buckets:   []float64{5, 60, 300, 600, 1800, 3600, 7200, 36000},
}, []string{"queryid", "status"})

type Job struct {
	JobID       uint
	InsightID   uint
	SourceID    string
	AccountID   string
	SourceType  source.Type
	Internal    bool
	Query       string
	Description string
	ExecutedAt  int64

	ResourceCollectionId *string
}

type JobResult struct {
	JobID  uint
	Status api.InsightJobStatus
	Error  string
}

func (j Job) Do(
	ctx context.Context,
	esConfig config.ElasticSearch,
	steampipePgConfig config.Postgres,
	onboardClient client.OnboardServiceClient,
	inventoryClient inventoryClient.InventoryServiceClient,
	schedulerClient describeClient.SchedulerServiceClient,
	sinkClient esSinkClient.EsSinkServiceClient,
	uploader *s3manager.Uploader,
	bucket string, currentWorkspaceID string,
	logger *zap.Logger,
) (r JobResult) {
	startTime := time.Now().Unix()

	defer func() {
		if err := recover(); err != nil {
			fmt.Println("panic with error:", err)
			fmt.Println(errors.Wrap(err, 2).ErrorStack())

			DoInsightJobsDuration.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Observe(float64(time.Now().Unix() - startTime))
			DoInsightJobsCount.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Inc()
			r = JobResult{
				JobID:  j.JobID,
				Status: api.InsightJobFailed,
				Error:  fmt.Sprintf("panic: %s", err),
			}
		}
	}()

	ctx2 := &httpclient.Context{UserRole: authApi.InternalRole}
	ctx2.Ctx = ctx
	if err := schedulerClient.InsightJobInProgress(ctx2, j.JobID); err != nil {
		logger.Error("failed to update job status", zap.Error(err))
	}

	// Assume it succeeded unless it fails somewhere.
	var (
		status         = api.InsightJobSucceeded
		firstErr error = nil
	)

	fail := func(err error) {
		DoInsightJobsDuration.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Observe(float64(time.Now().Unix() - startTime))
		DoInsightJobsCount.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Inc()
		status = api.InsightJobFailed
		if firstErr == nil {
			firstErr = err
		}
	}
	var count int64
	var (
		locationsMap          map[string]struct{}
		connectionsMap        = make(map[string]string)
		perConnectionCountMap = make(map[string]int64)
	)
	var err error
	var res *steampipe.Result

	connections, err := onboardClient.ListSources(ctx2, []source.Type{j.SourceType})
	if err != nil {
		logger.Error("failed to list sources", zap.Error(err))
		fail(fmt.Errorf("listing sources: %w", err))
		return
	}
	if len(connections) == 0 {
		return JobResult{
			JobID:  j.JobID,
			Status: status,
		}
	}
	for _, conn := range connections {
		if conn.LifecycleState != onboardApi.ConnectionLifecycleStateOnboard {
			continue
		}
		connectionsMap[conn.ID.String()] = conn.ConnectionID
	}

	steampipeSourceId := "all"

	var encodedResourceCollectionFilter *string
	if j.ResourceCollectionId != nil {
		rc, err := inventoryClient.GetResourceCollectionMetadata(ctx2,
			*j.ResourceCollectionId)
		if err != nil {
			fail(err)
			return
		}
		filtersJson, err := json.Marshal(rc.Filters)
		if err != nil {
			fail(err)
			return
		}
		filtersEncoded := base64.StdEncoding.EncodeToString(filtersJson)
		encodedResourceCollectionFilter = &filtersEncoded
	}

	if err := steampipe.PopulateSteampipeConfig(esConfig, j.SourceType); err != nil {
		logger.Error("failed to populate steampipe config", zap.Error(err))
		fail(fmt.Errorf("populating steampipe config: %w", err))
		return
	}
	err = steampipe.PopulateKaytuPluginSteampipeConfig(esConfig, steampipePgConfig, "")

	steampipeConn, err := steampipe.StartSteampipeServiceAndGetConnection(logger)
	if err != nil {
		logger.Error("failed to start steampipe service", zap.Error(err))
		fail(fmt.Errorf("starting steampipe service: %w", err))
		return
	}
	defer steampipeConn.Conn().Close()
	defer steampipe.StopSteampipeService(logger)
	logger.Info("Initialized steampipe")

	err = steampipeConn.SetConfigTableValue(ctx, steampipe.KaytuConfigKeyAccountID, steampipeSourceId)
	if err != nil {
		logger.Error("failed to set steampipe context config for account id", zap.Error(err), zap.String("account_id", steampipeSourceId))
		fail(err)
		return
	}
	defer steampipeConn.UnsetConfigTableValue(ctx, steampipe.KaytuConfigKeyAccountID)

	err = steampipeConn.SetConfigTableValue(ctx, steampipe.KaytuConfigKeyClientType, "insight")
	if err != nil {
		logger.Error("failed to set steampipe context config for client type", zap.Error(err), zap.String("client_type", "insight"))
		fail(err)
		return
	}
	defer steampipeConn.UnsetConfigTableValue(ctx, steampipe.KaytuConfigKeyClientType)

	if encodedResourceCollectionFilter != nil {
		err = steampipeConn.SetConfigTableValue(ctx, steampipe.KaytuConfigKeyResourceCollectionFilters, *encodedResourceCollectionFilter)
		if err != nil {
			logger.Error("failed to set steampipe context config for resource collection filters", zap.Error(err), zap.String("resource_collection_filters", *encodedResourceCollectionFilter))
			fail(err)
			return
		}
		defer steampipeConn.UnsetConfigTableValue(ctx, steampipe.KaytuConfigKeyResourceCollectionFilters)
	}

	logger.Info("running insight query", zap.Uint("insightId", j.InsightID), zap.String("connectionId", j.SourceID), zap.String("query", j.Query))

	res, err = steampipeConn.QueryAll(ctx, j.Query)
	steampipeConn.Conn().Close()
	if res != nil {
		count = int64(len(res.Data))
		for colNo, col := range res.Headers {
			switch strings.ToLower(col) {
			case "kaytu_metadata":
				for _, row := range res.Data {
					cell := row[colNo]
					if cell == nil {
						continue
					}
					switch j.SourceType {
					case source.CloudAWS:
						var metadata awsmodel.Metadata
						err = json.Unmarshal([]byte(cell.(string)), &metadata)
						if err != nil {
							break
						}
						if locationsMap == nil {
							locationsMap = make(map[string]struct{})
						}
						locationsMap[metadata.Region] = struct{}{}
					case source.CloudAzure:
						var metadata azuremodel.Metadata
						err = json.Unmarshal([]byte(cell.(string)), &metadata)
						if err != nil {
							break
						}
						if locationsMap == nil {
							locationsMap = make(map[string]struct{})
						}
						locationsMap[metadata.Location] = struct{}{}
					}
				}
			case "kaytu_account_id":
				for _, row := range res.Data {
					cell := row[colNo]
					if cell == nil {
						continue
					}
					if connectionIdStr, ok := cell.(string); ok {
						perConnectionCountMap[connectionIdStr]++
					}
				}
			default:
				continue
			}
		}
	}

	if err == nil {
		logger.Info("Got the results, uploading to s3")
		objectName := fmt.Sprintf("%d-%d.out", j.InsightID, j.JobID)
		content, err := json.Marshal(res)
		if err == nil {
			result, err := uploader.Upload(&s3manager.UploadInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(objectName),
				Body:   strings.NewReader(string(content)),
			})
			if err == nil {
				var locations []string = nil
				if locationsMap != nil {
					locations = make([]string, 0, len(locationsMap))
					for location := range locationsMap {
						locations = append(locations, location)
					}
				}
				var connections []es.InsightConnection = nil
				if connectionsMap != nil {
					connections = make([]es.InsightConnection, 0, len(connectionsMap))
					for connectionID, originalID := range connectionsMap {
						connections = append(connections, es.InsightConnection{
							ConnectionID: connectionID,
							OriginalID:   originalID,
						})
					}
				}

				var resources []es2.Doc
				resourceTypeList := []es.InsightResourceType{es.InsightResourceProviderHistory, es.InsightResourceProviderLast}
				for _, resourceType := range resourceTypeList {
					item := es.InsightResource{
						JobID:               j.JobID,
						InsightID:           j.InsightID,
						Query:               j.Query,
						Internal:            j.Internal,
						Description:         j.Description,
						SourceID:            j.SourceID,
						AccountID:           j.AccountID,
						Provider:            j.SourceType,
						ExecutedAt:          time.Now().UnixMilli(),
						Result:              count,
						ResourceType:        resourceType,
						Locations:           locations,
						IncludedConnections: connections,
						PerConnectionCount:  perConnectionCountMap,
						S3Location:          result.Location,
						ResourceCollection:  j.ResourceCollectionId,
					}
					keys, idx := item.KeysAndIndex()
					item.EsID = es2.HashOf(keys...)
					item.EsIndex = idx

					resources = append(resources, item)
				}

				logger.Info("sending docs to nats", zap.Int("count", len(resources)))

				if err := sinkClient.Ingest(&httpclient.Context{UserRole: authApi.InternalRole}, resources); err != nil {
					fail(fmt.Errorf("send to elastic: %w", err))
				}
			} else {
				logger.Error("failed to upload to s3", zap.Error(err))
				fail(fmt.Errorf("uploading to s3: %w", err))
			}
		} else {
			logger.Error("failed to marshal content", zap.Error(err))
			fail(fmt.Errorf("building content: %w", err))
		}
	} else {
		logger.Error("failed to query", zap.Error(err))
		fail(fmt.Errorf("describe resources: %w", err))
	}

	errMsg := ""
	if firstErr != nil {
		errMsg = firstErr.Error()
	}
	if status == api.InsightJobSucceeded {
		DoInsightJobsDuration.WithLabelValues(strconv.Itoa(int(j.InsightID)), "successful").Observe(float64(time.Now().Unix() - startTime))
		DoInsightJobsCount.WithLabelValues(strconv.Itoa(int(j.InsightID)), "successful").Inc()
	}

	return JobResult{
		JobID:  j.JobID,
		Status: status,
		Error:  errMsg,
	}
}
