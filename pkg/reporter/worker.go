package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgtype"
	"github.com/kaytu-io/kaytu-engine/pkg/onboard/client"
	"github.com/kaytu-io/kaytu-util/pkg/postgres"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"github.com/prometheus/client_golang/prometheus/push"
	"go.uber.org/zap"
)

type Worker struct {
	id               string
	logger           *zap.Logger
	db               *Database
	pusher           *push.Pusher
	onboardClient    client.OnboardServiceClient
	kaytuSteampipeDb *steampipe.Database
}

func InitializeWorker(
	id string,
	reporterJobQueue string,
	logger *zap.Logger,
	prometheusPushAddress string,
	postgresHost, postgresPort, postgresDb, postgresUsername, postgresPassword, postgresSSLMode string,
	steampipeHost string, steampipePort string, steampipeDb string, steampipeUsername string, steampipePassword string,
	onboardBaseURL string,
) (*Worker, error) {
	if id == "" {
		return nil, fmt.Errorf("'id' must be set to a non empty string")
	}
	var err error
	w := &Worker{
		id:               id,
		logger:           logger,
		db:               nil,
		pusher:           nil,
		onboardClient:    nil,
		kaytuSteampipeDb: nil,
	}
	defer func() {
		if err != nil && w != nil {
			w.Stop()
		}
	}()

	cfg := postgres.Config{
		Host:    postgresHost,
		Port:    postgresPort,
		User:    postgresUsername,
		Passwd:  postgresPassword,
		DB:      postgresDb,
		SSLMode: postgresSSLMode,
	}
	orm, err := postgres.NewClient(&cfg, logger)
	if err != nil {
		return nil, err
	}
	w.db, err = NewDatabase(orm)
	if err != nil {
		return nil, err
	}

	// setup steampipe connection
	steampipeOption := steampipe.Option{
		Host: steampipeHost,
		Port: steampipePort,
		User: steampipeUsername,
		Pass: steampipePassword,
		Db:   steampipeDb,
	}
	steampipeDatabase, err := steampipe.NewSteampipeDatabase(steampipeOption)
	if err != nil {
		return nil, err
	}
	w.kaytuSteampipeDb = steampipeDatabase

	w.pusher = push.New(prometheusPushAddress, "reporter-worker")
	w.pusher.Collector(ReporterJobsCount)

	w.onboardClient = client.NewOnboardServiceClient(onboardBaseURL)

	return w, nil
}

func (w *Worker) Run(ctx context.Context) error {
	// TODO read from queueing system

	// temporary solution to not crash
	msgBody := []byte("{}")

	var job Job
	if err := json.Unmarshal(msgBody, &job); err != nil {
		w.logger.Error("Failed to unmarshal task", zap.Error(err))
		//err = msg.Nack(false, false)
		//if err != nil {
		//	w.logger.Error("Failed nacking message", zap.Error(err))
		//}
		return err
	}
	w.logger.Info("Processing job", zap.String("connection id", job.ConnectionId), zap.Int("query count", len(job.Queries)))
	results, err := w.Do(ctx, job)
	if err == nil {
		dbRows := make([]WorkerJobResult, len(results))
		for i, result := range results {
			dbRows[i] = WorkerJobResult{
				JobID:              job.ID,
				TotalRows:          result.TotalRows,
				NotMatchingColumns: result.NotMatchingColumns,
				Query:              pgtype.JSONB{},
				Mismatches:         pgtype.JSONB{},
			}
			err = dbRows[i].Query.Set(result.Query)
			if err != nil {
				w.logger.Error("Failed to set query", zap.Error(err))
			}
			err = dbRows[i].Mismatches.Set(result.Mismatches)
			if err != nil {
				w.logger.Error("Failed to set mismatches", zap.Error(err))
			}
		}
		if len(dbRows) > 0 {
			err = w.db.BatchInsertWorkerJobResults(dbRows)
			if err != nil {
				w.logger.Error("Failed to insert worker job results", zap.Error(err))
			}
		}
		err = w.db.UpdateWorkerJobStatus(job.ID, JobStatusSuccessful)
		if err != nil {
			w.logger.Error("Failed to update worker job status", zap.Error(err))
		}
	} else {
		w.logger.Error("Failed to process job", zap.Error(err))
		err = w.db.UpdateWorkerJobStatus(job.ID, JobStatusFailed)
		if err != nil {
			w.logger.Error("Failed to update worker job status", zap.Error(err))
		}
	}

	//if err := msg.Ack(false); err != nil {
	//	w.logger.Error("Failed acking message", zap.Error(err))
	//}

	err = w.pusher.Push()
	if err != nil {
		w.logger.Error("Failed to push metrics", zap.Error(err))
	}

	return nil
}

func (w *Worker) Stop() {
	w.pusher.Push()

	if w.db != nil {
		w.db.Close()
		w.db = nil
	}

	if w.kaytuSteampipeDb != nil {
		w.kaytuSteampipeDb.Conn().Close()
		w.kaytuSteampipeDb = nil
	}
}
