package summarizer

import (
	"fmt"
	"strconv"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/kaytu-io/kaytu-aws-describer/aws"
	"github.com/kaytu-io/kaytu-azure-describer/azure"
	"github.com/kaytu-io/kaytu-util/pkg/kafka"

	"github.com/kaytu-io/kaytu-engine/pkg/inventory"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"

	"github.com/kaytu-io/kaytu-engine/pkg/summarizer/resourcebuilder"

	"github.com/kaytu-io/kaytu-engine/pkg/summarizer/es"

	"github.com/kaytu-io/kaytu-util/pkg/keibi-es-sdk"

	"github.com/kaytu-io/kaytu-engine/pkg/summarizer/api"

	"github.com/go-errors/errors"
	"go.uber.org/zap"
)

const MaxKafkaSendBatchSize = 10000

var DoResourceSummarizerJobsCount = promauto.NewCounterVec(prometheus.CounterOpts{
	Namespace: "keibi",
	Subsystem: "summarizer_worker",
	Name:      "do_resource_summarizer_jobs_total",
	Help:      "Count of done summarizer jobs in summarizer-worker service",
}, []string{"queryid", "status"})

var DoResourceSummarizerJobsDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "keibi",
	Subsystem: "summarizer_worker",
	Name:      "do_resource_summarizer_jobs_duration_seconds",
	Help:      "Duration of done summarizer jobs in summarizer-worker service",
	Buckets:   []float64{5, 60, 300, 600, 1800, 3600, 7200, 36000},
}, []string{"queryid", "status"})

type ResourceJob struct {
	JobID uint

	LastDayScheduleJobID     uint
	LastWeekScheduleJobID    uint
	LastQuarterScheduleJobID uint
	LastYearScheduleJobID    uint

	JobType JobType
}

type ResourceJobResult struct {
	JobID  uint
	Status api.SummarizerJobStatus
	Error  string

	JobType JobType
}

func (j ResourceJob) DoMustSummarizer(client keibi.Client, db inventory.Database, producer *confluent_kafka.Producer, topic string, logger *zap.Logger) (r ResourceJobResult) {
	logger.Info("Starting must summarizing", zap.Int("jobID", int(j.JobID)))
	startTime := time.Now().Unix()
	defer func() {
		if err := recover(); err != nil {
			logger.Error(fmt.Sprintf("paniced with error: %v", err), zap.Int("jobID", int(j.JobID)))
			fmt.Println("paniced with error:", err)
			fmt.Println(errors.Wrap(err, 2).ErrorStack())

			DoResourceSummarizerJobsDuration.WithLabelValues(strconv.Itoa(int(j.JobID)), "failure").Observe(float64(time.Now().Unix() - startTime))
			DoResourceSummarizerJobsCount.WithLabelValues(strconv.Itoa(int(j.JobID)), "failure").Inc()
			r = ResourceJobResult{
				JobID:   j.JobID,
				Status:  api.SummarizerJobFailed,
				Error:   fmt.Sprintf("paniced: %s", err),
				JobType: j.JobType,
			}
		}
	}()

	// Assume it succeeded unless it fails somewhere
	var (
		status         = api.SummarizerJobSucceeded
		firstErr error = nil
	)

	fail := func(err error) {
		logger.Info("failed due to", zap.Error(err))
		DoResourceSummarizerJobsDuration.WithLabelValues(strconv.Itoa(int(j.JobID)), "failure").Observe(float64(time.Now().Unix() - startTime))
		DoResourceSummarizerJobsCount.WithLabelValues(strconv.Itoa(int(j.JobID)), "failure").Inc()
		status = api.SummarizerJobFailed
		if firstErr == nil {
			firstErr = err
		}
	}

	resourceTypes := make([]string, 0)
	resourceTypes = append(resourceTypes, aws.ListSummarizeResourceTypes()...)
	resourceTypes = append(resourceTypes, azure.ListSummarizeResourceTypes()...)

	builders := []resourcebuilder.Builder{
		resourcebuilder.NewResourceSummaryBuilder(client, j.JobID),
		resourcebuilder.NewTrendSummaryBuilder(client, j.JobID),
		resourcebuilder.NewLocationSummaryBuilder(client, j.JobID),
		resourcebuilder.NewResourceTypeSummaryBuilder(client, logger, db, j.JobID),
	}
	var searchAfter []interface{}
	for {
		lookups, err := es.FetchLookupByResourceTypes(client, resourceTypes, searchAfter, es.EsFetchPageSize)
		if err != nil {
			fail(fmt.Errorf("Failed to fetch lookups: %v ", err))
			break
		}

		if len(lookups.Hits.Hits) == 0 {
			break
		}

		logger.Info("got a batch of lookup resources", zap.Int("count", len(lookups.Hits.Hits)))
		for _, lookup := range lookups.Hits.Hits {
			for _, b := range builders {
				b.Process(lookup.Source)
			}
			searchAfter = lookup.Sort
		}
	}

	costBuilder := resourcebuilder.NewCostSummaryBuilder(client, j.JobID)
	costResourceTypes := make([]string, 0)
	for _, t := range es.CostResourceTypeList {
		costResourceTypes = append(costResourceTypes, t.String())
	}
	searchAfter = nil
	for {
		lookups, err := es.FetchLookupByResourceTypes(client, costResourceTypes, searchAfter, es.EsFetchPageSize)
		if err != nil {
			fail(fmt.Errorf("Failed to fetch cost lookups: %v ", err))
			break
		}

		if len(lookups.Hits.Hits) == 0 {
			break
		}

		logger.Info("got a batch of cost lookup resources", zap.Int("count", len(lookups.Hits.Hits)))
		for _, lookup := range lookups.Hits.Hits {
			costBuilder.Process(lookup.Source)
			searchAfter = lookup.Sort
		}
	}
	logger.Info("processed cost lookup resources")
	err := costBuilder.PopulateHistory(j.LastDayScheduleJobID, j.LastWeekScheduleJobID, j.LastQuarterScheduleJobID, j.LastYearScheduleJobID)
	if err != nil {
		fail(fmt.Errorf("Failed to populate history: %v ", err))
	}
	logger.Info("history populated")

	var msgs []kafka.Doc
	msgs = append(msgs, costBuilder.Build()...)
	for _, b := range builders {
		msgs = append(msgs, b.Build()...)
	}
	logger.Info("built messages", zap.Int("count", len(msgs)))

	err = costBuilder.Cleanup(j.JobID)
	if err != nil {
		fail(fmt.Errorf("Failed to cleanup: %v ", err))
	}
	for _, b := range builders {
		err := b.Cleanup(j.JobID)
		if err != nil {
			fail(fmt.Errorf("Failed to cleanup: %v ", err))
		}
	}
	logger.Info("cleanup done")

	if len(msgs) > 0 {
		for i := 0; i < len(msgs); i += MaxKafkaSendBatchSize {
			end := i + MaxKafkaSendBatchSize
			if end > len(msgs) {
				end = len(msgs)
			}
			err := kafka.DoSend(producer, topic, -1, msgs[i:end], logger)
			if err != nil {
				fail(fmt.Errorf("Failed to send to kafka: %v ", err))
			}
		}
		logger.Info("sent to kafka")
	}

	errMsg := ""
	if firstErr != nil {
		errMsg = firstErr.Error()
	}
	if status == api.SummarizerJobSucceeded {
		DoResourceSummarizerJobsDuration.WithLabelValues(strconv.Itoa(int(j.JobID)), "successful").Observe(float64(time.Now().Unix() - startTime))
		DoResourceSummarizerJobsCount.WithLabelValues(strconv.Itoa(int(j.JobID)), "successful").Inc()
	}

	return ResourceJobResult{
		JobID:   j.JobID,
		Status:  status,
		Error:   errMsg,
		JobType: j.JobType,
	}
}
