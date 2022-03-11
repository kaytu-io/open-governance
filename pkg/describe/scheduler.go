package describe

import (
	"encoding/json"
	"fmt"
	"time"

	"gitlab.com/keibiengine/keibi-engine/pkg/aws"
	"gitlab.com/keibiengine/keibi-engine/pkg/azure"
	compliancereport "gitlab.com/keibiengine/keibi-engine/pkg/compliance-report"
	"gitlab.com/keibiengine/keibi-engine/pkg/describe/api"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/queue"
	"go.uber.org/zap"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const (
	JobCompletionInterval       = 1 * time.Minute
	JobSchedulingInterval       = 1 * time.Minute
	JobComplianceReportInterval = 1 * time.Minute
	JobTimeoutCheckInterval     = 15 * time.Minute
)

type Scheduler struct {
	id         string
	db         Database
	httpServer *HttpServer
	// jobQueue is used to publish jobs to be performed by the workers.
	jobQueue queue.Interface
	// jobResultQueue is used to consume the job results returned by the workers.
	jobResultQueue queue.Interface
	// sourceQueue is used to consume source updates by the onboarding service.
	sourceQueue queue.Interface

	complianceReportJobQueue       queue.Interface
	complianceReportJobResultQueue queue.Interface

	logger *zap.Logger
}

func InitializeScheduler(
	id string,
	rabbitMQUsername string,
	rabbitMQPassword string,
	rabbitMQHost string,
	rabbitMQPort int,
	describeJobQueue string,
	describeJobResultQueue string,
	complianceReportJobQueue string,
	complianceReportJobResultQueue string,
	sourceQueue string,
	postgresUsername string,
	postgresPassword string,
	postgresHost string,
	postgresPort string,
	postgresDb string,
	httpServerAddress string,
) (s *Scheduler, err error) {
	if id == "" {
		return nil, fmt.Errorf("'id' must be set to a non empty string")
	}

	s = &Scheduler{id: id}
	defer func() {
		if err != nil && s != nil {
			s.Stop()
		}
	}()

	s.logger, err = zap.NewProduction()
	if err != nil {
		return nil, err
	}

	s.logger.Info("Initializing the scheduler")

	qCfg := queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = describeJobQueue
	qCfg.Queue.Durable = true
	qCfg.Producer.ID = s.id
	describeQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the describe jobs queue", zap.String("queue", describeJobQueue))
	s.jobQueue = describeQueue

	qCfg = queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = describeJobResultQueue
	qCfg.Queue.Durable = true
	qCfg.Consumer.ID = s.id
	describeResultsQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the describe job results queue", zap.String("queue", describeJobResultQueue))
	s.jobResultQueue = describeResultsQueue

	qCfg = queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = sourceQueue
	qCfg.Queue.Durable = true
	qCfg.Consumer.ID = s.id
	sourceEventsQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the source events queue", zap.String("queue", sourceQueue))
	s.sourceQueue = sourceEventsQueue

	qCfg = queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = complianceReportJobQueue
	qCfg.Queue.Durable = true
	qCfg.Consumer.ID = s.id
	complianceReportJobsQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the compliance report jobs queue", zap.String("queue", complianceReportJobQueue))
	s.complianceReportJobQueue = complianceReportJobsQueue

	qCfg = queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = complianceReportJobResultQueue
	qCfg.Queue.Durable = true
	qCfg.Consumer.ID = s.id
	complianceReportJobsResultQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the compliance report jobs result queue", zap.String("queue", complianceReportJobResultQueue))
	s.complianceReportJobResultQueue = complianceReportJobsResultQueue

	dsn := fmt.Sprintf(`host=%s port=%s user=%s password=%s dbname=%s sslmode=disable TimeZone=GMT`,
		postgresHost,
		postgresPort,
		postgresUsername,
		postgresPassword,
		postgresDb,
	)

	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	s.logger.Info("Connected to the postgres database: ", zap.String("db", postgresDb))
	s.db = Database{orm: db}

	s.httpServer = NewHTTPServer(httpServerAddress, s.db)

	return s, nil
}

func (s *Scheduler) Run() error {
	err := s.db.orm.AutoMigrate(&Source{}, &DescribeSourceJob{}, &DescribeResourceJob{}, &ComplianceReportJob{})
	if err != nil {
		return err
	}

	go s.RunJobCompletionUpdater()
	go s.RunDescribeScheduler()
	go s.RunComplianceReportScheduler()

	go func() {
		s.logger.Fatal("SourceEvent consumer exited", zap.Error(s.RunSourceEventsConsumer()))
	}()

	go func() {
		s.logger.Fatal("DescribeJobResult consumer exited", zap.Error(s.RunJobResultsConsumer()))
	}()

	go func() {
		s.logger.Fatal("ComplianceReportJobResult consumer exited", zap.Error(s.RunComplianceReportJobResultsConsumer()))
	}()

	// httpServer.Initialize() shouldn't return.
	// If it does indicates a failure HTTP server.
	// If it does, indicates a failure with consume
	return s.httpServer.Initialize()
}

func (s *Scheduler) RunJobCompletionUpdater() {
	t := time.NewTicker(JobCompletionInterval)
	defer t.Stop()

	for ; ; <-t.C {
		results, err := s.db.QueryInProgressDescribedSourceJobGroupByDescribeResourceJobStatus()
		if err != nil {
			s.logger.Error("Failed to find DescribeSourceJobs", zap.Error(err))
			continue
		}

		jobIDToStatus := make(map[uint]map[api.DescribeResourceJobStatus]int)
		for _, v := range results {
			if _, ok := jobIDToStatus[v.DescribeSourceJobID]; !ok {
				jobIDToStatus[v.DescribeSourceJobID] = map[api.DescribeResourceJobStatus]int{
					api.DescribeResourceJobCreated:   0,
					api.DescribeResourceJobQueued:    0,
					api.DescribeResourceJobFailed:    0,
					api.DescribeResourceJobSucceeded: 0,
				}
			}

			jobIDToStatus[v.DescribeSourceJobID][v.DescribeResourceJobStatus] = v.DescribeResourceJobCount
		}

		for id, status := range jobIDToStatus {
			// If any CREATED or QUEUED, job is still in progress
			if status[api.DescribeResourceJobCreated] > 0 ||
				status[api.DescribeResourceJobQueued] > 0 {
				continue
			}

			// If any FAILURE, job is completed with failure
			if status[api.DescribeResourceJobFailed] > 0 {
				err := s.db.UpdateDescribeSourceJob(id, api.DescribeSourceJobCompletedWithFailure)
				if err != nil {
					s.logger.Error("Failed to update DescribeSourceJob status\n",
						zap.Uint("jobId", id),
						zap.String("status", string(api.DescribeSourceJobCompletedWithFailure)),
						zap.Error(err),
					)
				}

				continue
			}

			// If the rest is SUCCEEDED, job has completed with no failure
			if status[api.DescribeResourceJobSucceeded] > 0 {
				err := s.db.UpdateDescribeSourceJob(id, api.DescribeSourceJobCompleted)
				if err != nil {
					s.logger.Error("Failed to update DescribeSourceJob status\n",
						zap.Uint("jobId", id),
						zap.String("status", string(api.DescribeSourceJobCompleted)),
						zap.Error(err),
					)
				}

				continue
			}
		}
	}
}

func (s *Scheduler) RunDescribeScheduler() {
	s.logger.Info("Scheduling describe jobs on a timer")
	t := time.NewTicker(JobSchedulingInterval)
	defer t.Stop()

	for ; ; <-t.C {
		sources, err := s.db.QuerySourcesDueForDescribe()
		if err != nil {
			s.logger.Error("Failed to find the next sources to create DescribeSourceJob", zap.Error(err))
			continue
		}

		for _, source := range sources {
			s.logger.Info("Source is due for a describe. Creating a job now", zap.String("sourceId", source.ID.String()))

			daj := newDescribeSourceJob(source)
			err := s.db.CreateDescribeSourceJob(&daj)
			if err != nil {
				s.logger.Error("Failed to create DescribeSourceJob",
					zap.Uint("jobId", daj.ID),
					zap.String("sourceId", source.ID.String()),
					zap.Error(err),
				)
				continue
			}

			enqueueDescribeResourceJobs(s.logger, s.db, s.jobQueue, source, daj)

			err = s.db.UpdateDescribeSourceJob(daj.ID, api.DescribeSourceJobInProgress)
			if err != nil {
				s.logger.Error("Failed to update DescribeSourceJob",
					zap.Uint("jobId", daj.ID),
					zap.String("sourceId", source.ID.String()),
					zap.Error(err),
				)
			}
			daj.Status = api.DescribeSourceJobInProgress

			err = s.db.UpdateSourceDescribed(source.ID)
			if err != nil {
				s.logger.Error("Failed to update Source",
					zap.String("sourceId", source.ID.String()),
					zap.Error(err),
				)
			}
			daj.Status = api.DescribeSourceJobInProgress
		}
	}
}

// Consume events from the source queue. Based on the action of the event,
// update the list of sources that need to be described. Either create a source
// or update/delete the source.
func (s *Scheduler) RunSourceEventsConsumer() error {
	s.logger.Info("Consuming messages from SourceEvents queue")
	msgs, err := s.sourceQueue.Consume()
	if err != nil {
		return err
	}

	for msg := range msgs {
		var event SourceEvent
		if err := json.Unmarshal(msg.Body, &event); err != nil {
			s.logger.Error("Failed to unmarshal SourceEvent", zap.Error(err))
			msg.Nack(false, false)
			continue
		}

		err := ProcessSourceAction(s.db, event)
		if err != nil {
			s.logger.Error("Failed to process event for Source",
				zap.String("sourceId", event.SourceID.String()),
				zap.Error(err),
			)
			msg.Nack(false, false)
			continue
		}

		msg.Ack(false)
	}

	return fmt.Errorf("source events queue channel is closed")
}

// RunJobResultsConsumer consumes messages from the jobResult queue.
// It will update the status of the jobs in the database based on the message.
// It will also update the jobs status that are not completed in certain time to FAILED
func (s *Scheduler) RunJobResultsConsumer() error {
	s.logger.Info("Consuming messages from the JobResults queue")

	msgs, err := s.jobResultQueue.Consume()
	if err != nil {
		return err
	}

	t := time.NewTicker(JobTimeoutCheckInterval)
	defer t.Stop()

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("tasks channel is closed")
			}

			var result JobResult
			if err := json.Unmarshal(msg.Body, &result); err != nil {
				s.logger.Error("Failed to unmarshal DescribeResourceJob results\n", zap.Error(err))
				msg.Nack(false, false)
				continue
			}

			s.logger.Info("Processing JobResult for Job",
				zap.Uint("jobId", result.JobID),
				zap.String("status", string(result.Status)),
			)
			err := s.db.UpdateDescribeResourceJobStatus(result.JobID, result.Status, result.Error)
			if err != nil {
				s.logger.Error("Failed to update the status of DescribeResourceJob",
					zap.Uint("jobId", result.JobID),
					zap.Error(err),
				)
				msg.Nack(false, true)
				continue
			}

			msg.Ack(false)
		case <-t.C:
			err := s.db.UpdateDescribeResourceJobsTimedOut()
			if err != nil {
				s.logger.Error("Failed to update timed out DescribeResourceJobs", zap.Error(err))
			}
		}
	}
}

func (s *Scheduler) RunComplianceReportScheduler() {
	s.logger.Info("Scheduling ComplianceReport jobs on a timer")
	t := time.NewTicker(JobComplianceReportInterval)
	defer t.Stop()

	for ; ; <-t.C {
		sources, err := s.db.QuerySourcesDueForComplianceReport()
		if err != nil {
			s.logger.Error("Failed to find the next sources to create ComplianceReportJob", zap.Error(err))
			continue
		}

		for _, source := range sources {
			s.logger.Error("Source is due for a steampipe check. Creating a ComplianceReportJob now", zap.String("sourceId", source.ID.String()))

			crj := newComplianceReportJob(source)
			err := s.db.CreateComplianceReportJob(&crj)
			if err != nil {
				s.logger.Error("Failed to create ComplianceReportJob for Source",
					zap.Uint("jobId", crj.ID),
					zap.String("sourceId", source.ID.String()),
					zap.Error(err),
				)
				continue
			}

			enqueueComplianceReportJobs(s.logger, s.db, s.complianceReportJobQueue, source, &crj)

			err = s.db.UpdateSourceReportGenerated(source.ID)
			if err != nil {
				s.logger.Error("Failed to update report job of Source: %s\n", zap.String("sourceId", source.ID.String()), zap.Error(err))
			}
		}
	}
}

// RunComplianceReportJobResultsConsumer consumes messages from the complianceReportJobResultQueue queue.
// It will update the status of the jobs in the database based on the message.
// It will also update the jobs status that are not completed in certain time to FAILED
func (s *Scheduler) RunComplianceReportJobResultsConsumer() error {
	s.logger.Info("Consuming messages from the ComplianceReportJobResultQueue queue")

	msgs, err := s.complianceReportJobResultQueue.Consume()
	if err != nil {
		return err
	}

	t := time.NewTicker(JobTimeoutCheckInterval)
	defer t.Stop()

	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return fmt.Errorf("tasks channel is closed")
			}

			var result compliancereport.JobResult
			if err := json.Unmarshal(msg.Body, &result); err != nil {
				s.logger.Error("Failed to unmarshal ComplianceReportJob results", zap.Error(err))
				msg.Nack(false, false)
				continue
			}

			s.logger.Info("Processing ReportJobResult for Job",
				zap.Uint("jobId", result.JobID),
				zap.String("status", string(result.Status)),
			)
			err := s.db.UpdateComplianceReportJob(result.JobID, result.Status, result.Error, result.S3ResultURL)
			if err != nil {
				s.logger.Error("Failed to update the status of ComplianceReportJob",
					zap.Uint("jobId", result.JobID),
					zap.Error(err))
				msg.Nack(false, true)
				continue
			}

			msg.Ack(false)
		case <-t.C:
			err := s.db.UpdateComplianceReportJobsTimedOut()
			if err != nil {
				s.logger.Error("Failed to update timed out ComplianceReportJob", zap.Error(err))
			}
		}
	}
}

func (s *Scheduler) Stop() {
	if s.jobQueue != nil {
		s.jobQueue.Close()
		s.jobQueue = nil
	}

	if s.jobResultQueue != nil {
		s.jobResultQueue.Close()
		s.jobResultQueue = nil
	}

	if s.sourceQueue != nil {
		s.sourceQueue.Close()
		s.sourceQueue = nil
	}
}

func newDescribeSourceJob(a Source) DescribeSourceJob {
	daj := DescribeSourceJob{
		SourceID:             a.ID,
		DescribeResourceJobs: []DescribeResourceJob{},
		Status:               api.DescribeSourceJobCreated,
	}

	switch sType := api.SourceType(a.Type); sType {
	case api.SourceCloudAWS:
		for _, rType := range aws.ListResourceTypes() {
			daj.DescribeResourceJobs = append(daj.DescribeResourceJobs, DescribeResourceJob{
				ResourceType: rType,
				Status:       api.DescribeResourceJobCreated,
			})
		}
	case api.SourceCloudAzure:
		for _, rType := range azure.ListResourceTypes() {
			daj.DescribeResourceJobs = append(daj.DescribeResourceJobs, DescribeResourceJob{
				ResourceType: rType,
				Status:       api.DescribeResourceJobCreated,
			})
		}
	default:
		panic(fmt.Errorf("unsupported source type: %s", sType))
	}

	return daj
}

func newComplianceReportJob(a Source) ComplianceReportJob {
	return ComplianceReportJob{
		SourceID: a.ID,
		Status:   compliancereport.ComplianceReportJobCreated,
	}
}

func enqueueDescribeResourceJobs(logger *zap.Logger, db Database, q queue.Interface, a Source, daj DescribeSourceJob) {
	for i, drj := range daj.DescribeResourceJobs {
		nextStatus := api.DescribeResourceJobQueued
		errMsg := ""

		err := q.Publish(Job{
			JobID:        drj.ID,
			ParentJobID:  daj.ID,
			SourceType:   a.Type,
			ResourceType: drj.ResourceType,
			ConfigReg:    a.ConfigRef,
		})
		if err != nil {
			logger.Error("Failed to queue DescribeResourceJob",
				zap.Uint("jobId", drj.ID),
				zap.Error(err),
			)

			nextStatus = api.DescribeResourceJobFailed
			errMsg = fmt.Sprintf("queue: %s", err.Error())
		}

		err = db.UpdateDescribeResourceJobStatus(drj.ID, nextStatus, errMsg)
		if err != nil {
			logger.Error("Failed to update DescribeResourceJob",
				zap.Uint("jobId", drj.ID),
				zap.Error(err),
			)
		}

		daj.DescribeResourceJobs[i].Status = nextStatus
	}
}

func enqueueComplianceReportJobs(logger *zap.Logger, db Database, q queue.Interface, a Source, crj *ComplianceReportJob) {
	nextStatus := compliancereport.ComplianceReportJobInProgress
	errMsg := ""

	err := q.Publish(compliancereport.Job{
		JobID:      crj.ID,
		SourceType: compliancereport.SourceType(a.Type),
		ConfigReg:  a.ConfigRef,
	})
	if err != nil {
		logger.Error("Failed to queue ComplianceReportJob",
			zap.Uint("jobId", crj.ID),
			zap.Error(err),
		)

		nextStatus = compliancereport.ComplianceReportJobCompletedWithFailure
		errMsg = fmt.Sprintf("queue: %s", err.Error())
	}

	err = db.UpdateComplianceReportJob(crj.ID, nextStatus, errMsg, "")
	if err != nil {
		logger.Error("Failed to update ComplianceReportJob",
			zap.Uint("jobId", crj.ID),
			zap.Error(err),
		)
	}

	crj.Status = nextStatus
}
