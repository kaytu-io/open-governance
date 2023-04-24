package worker

import (
	"errors"
	"fmt"
	"os/exec"
	"time"

	"gitlab.com/keibiengine/keibi-engine/pkg/keibi-es-sdk"

	api2 "gitlab.com/keibiengine/keibi-engine/pkg/auth/api"
	"gitlab.com/keibiengine/keibi-engine/pkg/compliance/api"
	"gitlab.com/keibiengine/keibi-engine/pkg/compliance/client"
	"gitlab.com/keibiengine/keibi-engine/pkg/compliance/es"
	"gitlab.com/keibiengine/keibi-engine/pkg/config"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/httpclient"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/vault"
	"gitlab.com/keibiengine/keibi-engine/pkg/kafka"
	client2 "gitlab.com/keibiengine/keibi-engine/pkg/onboard/client"
	"gitlab.com/keibiengine/keibi-engine/pkg/source"
	"gitlab.com/keibiengine/keibi-engine/pkg/steampipe"
	"gitlab.com/keibiengine/keibi-engine/pkg/types"
	"go.uber.org/zap"
	"gopkg.in/Shopify/sarama.v1"
)

type Job struct {
	JobID         uint
	ScheduleJobID uint
	DescribedAt   int64
	EvaluatedAt   int64

	ConnectionID string
	BenchmarkID  string

	ConfigReg string
	Connector source.Type
}

type JobResult struct {
	JobID           uint
	Status          api.ComplianceReportJobStatus
	ReportCreatedAt int64
	Error           string
}

func (j *Job) Do(
	complianceClient client.ComplianceServiceClient,
	onboardClient client2.OnboardServiceClient,
	vault vault.SourceConfig,
	elasticSearchConfig config.ElasticSearch,
	kfkProducer sarama.SyncProducer,
	kfkTopic string,
	logger *zap.Logger,
) JobResult {
	result := JobResult{
		JobID:           j.JobID,
		Status:          api.ComplianceReportJobCompleted,
		ReportCreatedAt: time.Now().UnixMilli(),
		Error:           "",
	}

	if err := j.Run(complianceClient, onboardClient, vault, elasticSearchConfig, kfkProducer, kfkTopic, logger); err != nil {
		result.Error = err.Error()
		result.Status = api.ComplianceReportJobCompletedWithFailure
	}
	result.ReportCreatedAt = time.Now().UnixMilli()
	return result
}

func (j *Job) RunBenchmark(benchmarkID string, complianceClient client.ComplianceServiceClient, steampipeConn *steampipe.Database, connector source.Type) ([]es.Finding, error) {
	ctx := &httpclient.Context{
		UserRole: api2.AdminRole,
	}

	benchmark, err := complianceClient.GetBenchmark(ctx, benchmarkID)
	if err != nil {
		return nil, err
	}

	var findings []es.Finding
	for _, childBenchmarkID := range benchmark.Children {
		f, err := j.RunBenchmark(childBenchmarkID, complianceClient, steampipeConn, connector)
		if err != nil {
			return nil, err
		}

		findings = append(findings, f...)
	}

	for _, policyID := range benchmark.Policies {
		policy, err := complianceClient.GetPolicy(ctx, policyID)
		if err != nil {
			return nil, err
		}

		if policy.QueryID == nil {
			continue
		}

		query, err := complianceClient.GetQuery(ctx, *policy.QueryID)
		if err != nil {
			return nil, err
		}

		if query.Connector != string(connector) {
			return nil, errors.New("connector doesn't match")
		}

		res, err := steampipeConn.QueryAll(query.QueryToExecute)
		if err != nil {
			return nil, err
		}

		f, err := j.ExtractFindings(benchmark, policy, query, res)
		if err != nil {
			return nil, err
		}

		findings = append(findings, f...)
	}
	return findings, nil
}

func (j *Job) Run(complianceClient client.ComplianceServiceClient, onboardClient client2.OnboardServiceClient, vault vault.SourceConfig,
	elasticSearchConfig config.ElasticSearch, kfkProducer sarama.SyncProducer, kfkTopic string, logger *zap.Logger) error {

	ctx := &httpclient.Context{
		UserRole: api2.AdminRole,
	}

	src, err := onboardClient.GetSource(ctx, j.ConnectionID)
	if err != nil {
		return err
	}

	if src.HealthState != source.HealthStatusHealthy {
		return errors.New("connection not healthy")
	}

	defaultAccountID := "default"
	esk, err := keibi.NewClient(keibi.ClientConfig{
		Addresses: []string{elasticSearchConfig.Address},
		Username:  &elasticSearchConfig.Username,
		Password:  &elasticSearchConfig.Password,
		AccountID: &defaultAccountID,
	})
	if err != nil {
		return err
	}

	err = j.PopulateSteampipeConfig(vault, elasticSearchConfig)
	if err != nil {
		return err
	}

	cmd := exec.Command("steampipe", "service", "stop")
	_ = cmd.Run()

	cmd = exec.Command("steampipe", "service", "start", "--database-listen", "network", "--database-port",
		"9193", "--database-password", "abcd")
	_ = cmd.Run()

	time.Sleep(5 * time.Second)

	steampipeConn, err := steampipe.NewSteampipeDatabase(steampipe.Option{
		Host: "localhost",
		Port: "9193",
		User: "steampipe",
		Pass: "abcd",
		Db:   "steampipe",
	})
	if err != nil {
		return err
	}

	findings, err := j.RunBenchmark(j.BenchmarkID, complianceClient, steampipeConn, src.Type)
	if err != nil {
		return err
	}
	fmt.Println("++++++ findings len: ", len(findings))
	findingsFiltered, err := j.FilterFindings(esk, findings)
	if err != nil {
		return err
	}
	fmt.Println("++++++ findingsFiltered len: ", len(findingsFiltered))

	var docs []kafka.Doc
	for _, finding := range findingsFiltered {
		docs = append(docs, finding)
	}
	return kafka.DoSend(kfkProducer, kfkTopic, docs, logger)
}

func (j *Job) FilterFindings(esClient keibi.Client, findings []es.Finding) ([]es.Finding, error) {
	// get all active findings from ES page by page
	// go through the ones extracted and remove duplicates
	// if a finding fetched from es is not duplicated disable it
	from := 0
	for {
		resp, err := es.GetActiveFindings(esClient, from, 1000)
		if err != nil {
			return nil, err
		}
		fmt.Println("+++++++++ active old findings:", len(resp.Hits.Hits))
		from += 1000

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

func (j *Job) ExtractFindings(benchmark *api.Benchmark, policy *api.Policy, query *api.Query, res *steampipe.Result) ([]es.Finding, error) {
	var findings []es.Finding
	for _, record := range res.Data {
		if len(record) != len(res.Headers) {
			return nil, fmt.Errorf("invalid record length, record=%d headers=%d", len(record), len(res.Headers))
		}
		recordValue := map[string]interface{}{}
		for idx, header := range res.Headers {
			value := record[idx]
			recordValue[header] = value
		}

		var resourceID, resourceName, resourceType, resourceLocation, reason string
		var status types.ComplianceResult
		if v, ok := recordValue["resource"].(string); ok {
			resourceID = v
		}
		if v, ok := recordValue["name"].(string); ok {
			resourceName = v
		}
		if v, ok := recordValue["resourceType"].(string); ok {
			resourceType = v
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

		severity := types.SeverityNone
		if status == types.ComplianceResultALARM {
			severity = policy.Severity
		}
		findings = append(findings, es.Finding{
			ID:               fmt.Sprintf("%s-%s-%d", resourceID, policy.ID, j.ScheduleJobID),
			BenchmarkID:      benchmark.ID,
			PolicyID:         policy.ID,
			ConnectionID:     j.ConnectionID,
			DescribedAt:      j.DescribedAt,
			EvaluatedAt:      j.EvaluatedAt,
			StateActive:      true,
			Result:           status,
			Severity:         severity,
			Evaluator:        query.Engine,
			Connector:        j.Connector,
			ResourceID:       resourceID,
			ResourceName:     resourceName,
			ResourceLocation: resourceLocation,
			ResourceType:     resourceType,
			Reason:           reason,
			ComplianceJobID:  j.JobID,
			ScheduleJobID:    j.ScheduleJobID,
		})
	}
	return findings, nil
}
