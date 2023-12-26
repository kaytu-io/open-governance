package compliance

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/kaytu-io/kaytu-engine/pkg/compliance/summarizer"
	types2 "github.com/kaytu-io/kaytu-engine/pkg/compliance/summarizer/types"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/db/model"
	"github.com/kaytu-io/kaytu-engine/pkg/types"
	kafka2 "github.com/kaytu-io/kaytu-util/pkg/kafka"
	"go.uber.org/zap"
	"time"
)

const SummarizerSchedulingInterval = 1 * time.Minute

type SankDocumentCountResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		}
	}
}

func (s *JobScheduler) getSankDocumentCountBenchmark(benchmarkId string, parentJobID uint) (int, error) {
	request := make(map[string]any)
	filters := make([]map[string]any, 0)
	filters = append(filters, map[string]any{
		"term": map[string]any{
			"benchmarkID": benchmarkId,
		},
	})
	filters = append(filters, map[string]any{
		"term": map[string]any{
			"parentComplianceJobID": parentJobID,
		},
	})
	request["query"] = map[string]any{
		"bool": map[string]any{
			"filter": filters,
		},
	}
	request["size"] = 0

	query, err := json.Marshal(request)
	if err != nil {
		s.logger.Error("failed to marshal request", zap.Error(err))
		return 0, err
	}

	s.logger.Info("GetSankDocumentCountBenchmark", zap.String("benchmarkId", benchmarkId), zap.String("query", string(query)))

	sankDocumentCountResponse := SankDocumentCountResponse{}
	err = s.esClient.SearchWithTrackTotalHits(
		context.TODO(), types.FindingsIndex,
		string(query),
		nil,
		&sankDocumentCountResponse, true,
	)
	if err != nil {
		s.logger.Error("failed to get sank document count", zap.Error(err), zap.String("benchmarkId", benchmarkId))
		return 0, err
	}

	return sankDocumentCountResponse.Hits.Total.Value, nil
}

func (s *JobScheduler) runSummarizer() error {
	s.logger.Info("checking for benchmarks to summarize")

	err := s.db.SetJobToRunnersInProgress()
	if err != nil {
		s.logger.Error("failed to set jobs to runners in progress", zap.Error(err))
		return err
	}

	jobs, err := s.db.ListJobsWithRunnersCompleted()
	for _, job := range jobs {
		sankDocCount, err := s.getSankDocumentCountBenchmark(job.BenchmarkID, job.ID)
		if err != nil {
			s.logger.Error("failed to get sank document count", zap.Error(err), zap.String("benchmarkId", job.BenchmarkID))
			return err
		}
		totalDocCount, err := s.db.FetchTotalFindingCountForComplianceJob(job.ID)
		if err != nil {
			s.logger.Error("failed to get total document count", zap.Error(err), zap.String("benchmarkId", job.BenchmarkID))
			return err
		}

		lastUpdatedRunner, err := s.db.GetLastUpdatedRunnerForParent(job.ID)
		if err != nil {
			s.logger.Error("failed to get last updated runner", zap.Error(err), zap.String("benchmarkId", job.BenchmarkID))
			return err
		}

		if time.Now().Add(-1*time.Hour).Before(lastUpdatedRunner.UpdatedAt) &&
			(float64(sankDocCount) < float64(totalDocCount)*0.9) {
			continue
		}
		err = s.createSummarizer(job)
		if err != nil {
			s.logger.Error("failed to create summarizer", zap.Error(err), zap.String("benchmarkId", job.BenchmarkID))
			return err
		}
	}

	createds, err := s.db.FetchCreatedSummarizers()
	if err != nil {
		return err
	}

	for _, job := range createds {
		err = s.triggerSummarizer(job)
		if err != nil {
			return err
		}
	}

	jobs, err = s.db.ListJobsToFinish()
	for _, job := range jobs {
		err = s.finishComplianceJob(job)
		if err != nil {
			return err
		}
	}

	err = s.db.RetryFailedSummarizers()
	if err != nil {
		s.logger.Error("failed to retry failed runners", zap.Error(err))
		return err
	}

	return nil
}

func (s *JobScheduler) finishComplianceJob(job model.ComplianceJob) error {
	failedRunners, err := s.db.ListFailedRunnersWithParentID(job.ID)
	if err != nil {
		return err
	}

	if len(failedRunners) > 0 {
		return s.db.UpdateComplianceJob(job.ID, model.ComplianceJobFailed, fmt.Sprintf("%d runners failed", len(failedRunners)))
	}

	failedSummarizers, err := s.db.ListFailedSummarizersWithParentID(job.ID)
	if err != nil {
		return err
	}

	if len(failedSummarizers) > 0 {
		return s.db.UpdateComplianceJob(job.ID, model.ComplianceJobFailed, fmt.Sprintf("%d summarizers failed", len(failedSummarizers)))
	}

	return s.db.UpdateComplianceJob(job.ID, model.ComplianceJobSucceeded, "")
}

func (s *JobScheduler) createSummarizer(job model.ComplianceJob) error {
	// run summarizer
	dbModel := model.ComplianceSummarizer{
		BenchmarkID: job.BenchmarkID,
		ParentJobID: job.ID,
		StartedAt:   time.Now(),
		Status:      summarizer.ComplianceSummarizerCreated,
	}
	err := s.db.CreateSummarizerJob(&dbModel)
	if err != nil {
		return err
	}

	return s.db.UpdateComplianceJob(job.ID, model.ComplianceJobSummarizerInProgress, "")
}

func (s *JobScheduler) triggerSummarizer(job model.ComplianceSummarizer) error {
	summarizerJob := types2.Job{
		ID:          job.ID,
		BenchmarkID: job.BenchmarkID,
		CreatedAt:   job.CreatedAt,
	}
	jobJson, err := json.Marshal(summarizerJob)
	if err != nil {
		_ = s.db.UpdateSummarizerJob(job.ID, summarizer.ComplianceSummarizerFailed, job.CreatedAt, err.Error())
		return err
	}

	msg := kafka2.Msg(fmt.Sprintf("job-%d", job.ID), jobJson, "", summarizer.JobQueue, kafka.PartitionAny)
	_, err = kafka2.SyncSend(s.logger, s.kafkaProducer, []*kafka.Message{msg}, nil)
	if err != nil {
		_ = s.db.UpdateSummarizerJob(job.ID, summarizer.ComplianceSummarizerFailed, job.CreatedAt, err.Error())
		return err
	}

	return s.db.UpdateSummarizerJob(job.ID, summarizer.ComplianceSummarizerInProgress, job.CreatedAt, "")
}
