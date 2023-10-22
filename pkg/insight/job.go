package insight

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	inventoryClient "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
	"github.com/kaytu-io/kaytu-util/pkg/config"
	"strconv"
	"strings"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpclient"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-errors/errors"
	azuremodel "github.com/kaytu-io/kaytu-azure-describer/azure/model"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	awsmodel "github.com/kaytu-io/kaytu-aws-describer/aws/model"
	"github.com/kaytu-io/kaytu-engine/pkg/insight/api"
	"github.com/kaytu-io/kaytu-engine/pkg/insight/es"
	onboardApi "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	"github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"go.uber.org/zap"

	authApi "github.com/kaytu-io/kaytu-engine/pkg/auth/api"
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
	IsStack     bool

	ResourceCollectionId *string
}

type JobResult struct {
	JobID  uint
	Status api.InsightJobStatus
	Error  string
}

func (j Job) Do(esConfig config.ElasticSearch,
	onboardClient client.OnboardServiceClient, inventoryClient inventoryClient.InventoryServiceClient,
	producer *confluent_kafka.Producer, uploader *s3manager.Uploader, bucket,
	currentWorkspaceID string,
	topic string, logger *zap.Logger) (r JobResult) {
	startTime := time.Now().Unix()
	defer func() {
		if err := recover(); err != nil {
			fmt.Println("paniced with error:", err)
			fmt.Println(errors.Wrap(err, 2).ErrorStack())

			DoInsightJobsDuration.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Observe(float64(time.Now().Unix() - startTime))
			DoInsightJobsCount.WithLabelValues(strconv.Itoa(int(j.InsightID)), "failure").Inc()
			r = JobResult{
				JobID:  j.JobID,
				Status: api.InsightJobFailed,
				Error:  fmt.Sprintf("paniced: %s", err),
			}
		}
	}()

	// Assume it succeeded unless it fails somewhere
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

	srcs, err := onboardClient.ListSources(&httpclient.Context{
		UserRole: authApi.InternalRole,
	}, []source.Type{j.SourceType})
	if err != nil {
		logger.Error("failed to list sources", zap.Error(err))
		fail(fmt.Errorf("listing sources: %w", err))
		return
	}
	if len(srcs) == 0 {
		return JobResult{
			JobID:  j.JobID,
			Status: status,
		}
	}
	for _, src := range srcs {
		if src.LifecycleState != onboardApi.ConnectionLifecycleStateOnboard {
			continue
		}
		connectionsMap[src.ID.String()] = src.ConnectionID
	}

	steampipeSourceId := "all"
	if j.IsStack == true {
		esConfig, err = steampipe.GetStackElasticConfig(currentWorkspaceID, j.SourceID)
		if err != nil {
			logger.Error("failed to get stack elastic config", zap.Error(err))
			fail(fmt.Errorf("getting stack elastic config: %w", err))
			return
		}
		steampipeSourceId = j.SourceID
	}

	var encodedResourceCollectionFilter *string
	if j.ResourceCollectionId != nil {
		rc, err := inventoryClient.GetResourceCollection(&httpclient.Context{UserRole: authApi.InternalRole},
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

	err = steampipe.PopulateSteampipeConfig(esConfig, j.SourceType)
	if err != nil {
		logger.Error("failed to populate steampipe config", zap.Error(err))
		fail(fmt.Errorf("populating steampipe config: %w", err))
		return
	}

	steampipeConn, err := steampipe.StartSteampipeServiceAndGetConnection(logger)
	if err != nil {
		logger.Error("failed to start steampipe service", zap.Error(err))
		fail(fmt.Errorf("starting steampipe service: %w", err))
		return
	}
	defer steampipeConn.Conn().Close()
	defer steampipe.StopSteampipeService(logger)
	logger.Info("Initialized steampipe")

	err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyAccountID, steampipeSourceId)
	if err != nil {
		logger.Error("failed to set steampipe context config for account id", zap.Error(err), zap.String("account_id", steampipeSourceId))
		fail(err)
		return
	}
	if encodedResourceCollectionFilter != nil {
		err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyResourceCollectionFilters, *encodedResourceCollectionFilter)
		if err != nil {
			logger.Error("failed to set steampipe context config for resource collection filters", zap.Error(err), zap.String("resource_collection_filters", *encodedResourceCollectionFilter))
			fail(err)
			return
		}
	}

	logger.Info("running insight query", zap.Uint("insightId", j.InsightID), zap.String("connectionId", j.SourceID), zap.String("query", j.Query))

	res, err = steampipeConn.QueryAll(context.TODO(), j.Query)
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

				var resources []kafka.Doc
				resourceTypeList := []es.InsightResourceType{es.InsightResourceProviderHistory, es.InsightResourceProviderLast}
				for _, resourceType := range resourceTypeList {
					resources = append(resources, es.InsightResource{
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
					})
				}

				logger.Info("sending docs to kafka", zap.Any("producer", producer), zap.String("topic", topic), zap.Int("count", len(resources)))
				if err := kafka.DoSend(producer, topic, -1, resources, logger, nil); err != nil {
					fail(fmt.Errorf("send to kafka: %w", err))
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
