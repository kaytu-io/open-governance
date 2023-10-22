package worker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	inventoryClient "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	authApi "github.com/kaytu-io/kaytu-engine/pkg/auth/api"
	complianceApi "github.com/kaytu-io/kaytu-engine/pkg/compliance/api"
	complianceClient "github.com/kaytu-io/kaytu-engine/pkg/compliance/client"
	"github.com/kaytu-io/kaytu-engine/pkg/compliance/es"
	describeClient "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpclient"
	onboardApi "github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	onboardClient "github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-engine/pkg/types"
	"github.com/kaytu-io/kaytu-util/pkg/config"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"
	"github.com/kaytu-io/kaytu-util/pkg/kaytu-es-sdk"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"go.uber.org/zap"
)

type Job struct {
	JobID         uint
	ScheduleJobID uint
	DescribedAt   int64
	EvaluatedAt   int64

	ConnectionID string
	BenchmarkID  string

	Connector source.Type
	IsStack   bool

	ResourceCollectionId *string
}

type JobResult struct {
	JobID           uint
	Status          complianceApi.ComplianceReportJobStatus
	ReportCreatedAt int64
	Error           string
}

func (j *Job) Do(
	complianceClient complianceClient.ComplianceServiceClient,
	onboardClient onboardClient.OnboardServiceClient,
	scheduleClient describeClient.SchedulerServiceClient,
	inventoryClient inventoryClient.InventoryServiceClient,
	elasticSearchConfig config.ElasticSearch,
	kfkProducer *confluent_kafka.Producer,
	kfkTopic string,
	currentWorkspaceId string,
	logger *zap.Logger,
) JobResult {
	result := JobResult{
		JobID:           j.JobID,
		Status:          complianceApi.ComplianceReportJobCompleted,
		ReportCreatedAt: time.Now().UnixMilli(),
		Error:           "",
	}

	if err := j.Run(complianceClient, onboardClient, scheduleClient, inventoryClient,
		elasticSearchConfig, kfkProducer, kfkTopic, currentWorkspaceId, logger); err != nil {
		result.Error = err.Error()
		result.Status = complianceApi.ComplianceReportJobCompletedWithFailure
	}
	result.ReportCreatedAt = time.Now().UnixMilli()
	return result
}

func (j *Job) RunBenchmark(logger *zap.Logger, esk kaytu.Client, benchmarkID string, parentBenchmarks []string, complianceClient complianceClient.ComplianceServiceClient, steampipeConn *steampipe.Database, connector source.Type) ([]types.Finding, error) {
	ctx := &httpclient.Context{
		UserRole: authApi.AdminRole,
	}

	benchmark, err := complianceClient.GetBenchmark(ctx, benchmarkID)
	if err != nil {
		return nil, err
	}

	fmt.Println("+++++++++ Running Benchmark:", benchmarkID)

	var findings []types.Finding
	for _, childBenchmarkID := range benchmark.Children {
		f, err := j.RunBenchmark(logger, esk, childBenchmarkID, append(parentBenchmarks, benchmarkID), complianceClient, steampipeConn, connector)
		if err != nil {
			return nil, err
		}

		findings = append(findings, f...)
	}

	for _, policyID := range benchmark.Policies {
		fmt.Println("+++++++++++ Running Policy:", policyID)
		policy, err := complianceClient.GetPolicy(ctx, policyID)
		if err != nil {
			return nil, err
		}
		if policy.QueryID == nil {
			continue
		}
		var f []types.Finding
		if !policy.ManualVerification {
			query, err := complianceClient.GetQuery(ctx, *policy.QueryID)
			if err != nil {
				logger.Error("failed to get query", zap.Error(err))
				return nil, err
			}

			if query.Connector != string(connector) {
				return nil, errors.New("connector doesn't match")
			}

			res, err := steampipeConn.QueryAll(context.TODO(), query.QueryToExecute)
			if err != nil {
				logger.Error("failed to execute query",
					zap.Error(err),
					zap.String("policyID", policyID),
					zap.Uint("jobID", j.JobID))
				continue
			}

			f, err = j.ExtractFindings(esk, benchmark, policy, query, res)
			if err != nil {
				return nil, err
			}
		}

		if !j.IsStack {
			findingsFiltered, err := j.FilterFindings(esk, policyID, f)
			if err != nil {
				return nil, err
			}
			fmt.Println("++++++ findingsFiltered len: ", len(findingsFiltered))
			f = findingsFiltered
		}

		for idx, finding := range f {
			finding.ParentBenchmarks = append(parentBenchmarks, benchmarkID)
			f[idx] = finding
		}
		findings = append(findings, f...)
	}

	return findings, nil
}

func (j *Job) Run(complianceClient complianceClient.ComplianceServiceClient,
	onboardClient onboardClient.OnboardServiceClient,
	schedulerClient describeClient.SchedulerServiceClient,
	inventoryClient inventoryClient.InventoryServiceClient,
	elasticSearchConfig config.ElasticSearch, kfkProducer *confluent_kafka.Producer, kfkTopic string, currentWorkspaceId string, logger *zap.Logger) error {

	ctx := &httpclient.Context{
		UserRole: authApi.AdminRole,
	}
	var accountId string
	var connector source.Type
	var esk kaytu.Client
	if j.IsStack == true {
		stack, err := schedulerClient.GetStack(ctx, j.ConnectionID)
		if err != nil {
			return err
		}
		accountId = stack.AccountIDs[0]
		connector = stack.SourceType

		eskConfig, err := steampipe.GetStackElasticConfig(currentWorkspaceId, stack.StackID)
		esk, err = kaytu.NewClient(kaytu.ClientConfig{
			Addresses: []string{eskConfig.Address},
			Username:  &eskConfig.Username,
			Password:  &eskConfig.Password,
			AccountID: &accountId,
		})
		if err != nil {
			return err
		}
	} else {
		if len(j.ConnectionID) > 0 && j.ConnectionID != "all" {
			src, err := onboardClient.GetSource(ctx, j.ConnectionID)
			if err != nil {
				return err
			}
			accountId = src.ConnectionID
			connector = src.Connector
			if src.LifecycleState != onboardApi.ConnectionLifecycleStateOnboard {
				return errors.New("connection not healthy")
			}
		} else {
			accountId = "all"
			connector = j.Connector
		}
		var err error
		esk, err = kaytu.NewClient(kaytu.ClientConfig{
			Addresses: []string{elasticSearchConfig.Address},
			Username:  &elasticSearchConfig.Username,
			Password:  &elasticSearchConfig.Password,
			AccountID: &accountId,
		})
		if err != nil {
			return err
		}
	}

	logger.Info("+++++ New elasticSearch Client created")

	var encodedResourceCollectionFilter *string
	if j.ResourceCollectionId != nil {
		rc, err := inventoryClient.GetResourceCollection(&httpclient.Context{UserRole: authApi.InternalRole},
			*j.ResourceCollectionId)
		if err != nil {
			return err
		}
		filtersJson, err := json.Marshal(rc.Filters)
		if err != nil {
			return err
		}
		filtersEncoded := base64.StdEncoding.EncodeToString(filtersJson)
		encodedResourceCollectionFilter = &filtersEncoded
	}

	err := steampipe.PopulateSteampipeConfig(elasticSearchConfig, j.Connector)
	if err != nil {
		logger.Error("failed to populate steampipe config", zap.Error(err))
		return err
	}

	steampipeConn, err := steampipe.StartSteampipeServiceAndGetConnection(logger)
	if err != nil {
		logger.Error("failed to start steampipe service", zap.Error(err))
		return err
	}
	defer steampipeConn.Conn().Close()
	defer steampipe.StopSteampipeService(logger)

	err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyAccountID, accountId)
	if err != nil {
		logger.Error("failed to set steampipe context config for account id", zap.Error(err), zap.String("account_id", accountId))
		return err
	}
	if encodedResourceCollectionFilter != nil {
		err = steampipeConn.SetConfigTableValue(context.TODO(), steampipe.KaytuConfigKeyResourceCollectionFilters, *encodedResourceCollectionFilter)
		if err != nil {
			logger.Error("failed to set steampipe context config for resource collection filters", zap.Error(err), zap.String("resource_collection_filters", *encodedResourceCollectionFilter))
			return err
		}
	}

	findings, err := j.RunBenchmark(logger, esk, j.BenchmarkID, nil, complianceClient, steampipeConn, connector)
	if err != nil {
		return err
	}
	steampipeConn.Conn().Close()
	logger.Info("++++++ findings ", zap.Int("len", len(findings)))
	var docs []kafka.Doc
	for _, finding := range findings {
		docs = append(docs, finding)
	}
	return kafka.DoSend(kfkProducer, kfkTopic, -1, docs, logger, nil)
}

func (j *Job) FilterFindings(esClient kaytu.Client, policyID string, findings []types.Finding) ([]types.Finding, error) {
	// get all active findings from ES page by page
	// go through the ones extracted and remove duplicates
	// if a finding fetched from es is not duplicated disable it
	from := 0
	const esFetchSize = 1000
	for {
		resp, err := es.GetActiveFindings(esClient, policyID, from, esFetchSize)
		if err != nil {
			return nil, err
		}
		fmt.Println("+++++++++ active old findings:", len(resp.Hits.Hits))
		from += esFetchSize

		if len(resp.Hits.Hits) == 0 {
			break
		}

		for _, hit := range resp.Hits.Hits {
			dup := false

			for idx, finding := range findings {
				if finding.ResourceID == hit.Source.ResourceID && finding.PolicyID == hit.Source.PolicyID {
					dup = true
					fmt.Println("+++++++++ removing dup:", finding.ID, hit.Source.ID)
					findings = append(findings[:idx], findings[idx+1:]...)
					break
				}
			}

			if !dup {
				f := hit.Source
				f.StateActive = false
				fmt.Println("+++++++++ making this disabled:", f.ID)
				findings = append(findings, f)
			}
		}
	}
	return findings, nil
}

func (j *Job) ExtractFindings(client kaytu.Client, benchmark *complianceApi.Benchmark, policy *complianceApi.Policy, query *complianceApi.Query, res *steampipe.Result) ([]types.Finding, error) {
	var findings []types.Finding
	resourceType := ""
	for _, record := range res.Data {
		if len(record) != len(res.Headers) {
			return nil, fmt.Errorf("invalid record length, record=%d headers=%d", len(record), len(res.Headers))
		}
		recordValue := make(map[string]any)
		for idx, header := range res.Headers {
			value := record[idx]
			recordValue[header] = value
		}

		var resourceID, resourceName, resourceLocation, reason string
		var status types.ComplianceResult
		if v, ok := recordValue["resource"].(string); ok {
			resourceID = v
			if resourceType == "" {
				lookupResource, err := es.FetchLookupsByResourceIDWildcard(client, resourceID)
				if err != nil {
					return nil, err
				}
				if len(lookupResource.Hits.Hits) > 0 {
					resourceType = lookupResource.Hits.Hits[0].Source.ResourceType
				}
			}
		}
		if v, ok := recordValue["name"].(string); ok {
			resourceName = v
		}
		if v, ok := recordValue["location"].(string); ok {
			resourceLocation = v
		}
		if v, ok := recordValue["reason"].(string); ok {
			reason = v
		}
		if v, ok := recordValue["status"].(string); ok {
			status = types.ComplianceResult(v)
		}
		fmt.Println("======", recordValue)

		severity := types.FindingSeverityNone
		if status == types.ComplianceResultALARM {
			severity = policy.Severity
		}
		findings = append(findings, types.Finding{
			ID:                 fmt.Sprintf("%s-%s-%d", resourceID, policy.ID, j.ScheduleJobID),
			BenchmarkID:        benchmark.ID,
			PolicyID:           policy.ID,
			ConnectionID:       j.ConnectionID,
			DescribedAt:        j.DescribedAt,
			EvaluatedAt:        j.EvaluatedAt,
			StateActive:        true,
			Result:             status,
			Severity:           severity,
			Evaluator:          query.Engine,
			Connector:          j.Connector,
			ResourceID:         resourceID,
			ResourceName:       resourceName,
			ResourceLocation:   resourceLocation,
			ResourceType:       resourceType,
			Reason:             reason,
			ComplianceJobID:    j.JobID,
			ScheduleJobID:      j.ScheduleJobID,
			ResourceCollection: j.ResourceCollectionId,
		})
	}
	return findings, nil
}
