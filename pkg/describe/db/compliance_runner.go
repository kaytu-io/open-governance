package db

import (
	"errors"
	"github.com/kaytu-io/kaytu-engine/pkg/compliance/runner"
	"github.com/kaytu-io/kaytu-engine/pkg/describe/db/model"
	"gorm.io/gorm"
	"time"
)

func (db Database) CreateRunnerJobs(runners []*model.ComplianceRunner) error {
	tx := db.ORM.
		Model(&model.ComplianceRunner{}).
		CreateInBatches(runners, 500)
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) DeleteOldRunnerJob(parentJobId *uint) error {
	tx := db.ORM.Model(&model.ComplianceRunner{})
	if parentJobId != nil {
		tx = tx.Where("parent_job_id = ?", *parentJobId)
	} else {
		tx = tx.Where("created_at < ?", time.Now().Add(-time.Hour*24*2))
	}
	tx = tx.Unscoped().Delete(&model.ComplianceRunner{})
	if tx.Error != nil {
		return tx.Error
	}

	tx = db.ORM.Model(&model.ComplianceRunner{}).
		Where("created_at < ?", time.Now().Add(-time.Hour*24*7)).
		Unscoped().Delete(&model.ComplianceRunner{})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) FetchCreatedRunners() ([]model.ComplianceRunner, error) {
	var jobs []model.ComplianceRunner
	tx := db.ORM.Model(&model.ComplianceRunner{}).
		Where("status = ?", runner.ComplianceRunnerCreated).Order("created_at ASC").Limit(1000).Find(&jobs)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, tx.Error
	}
	return jobs, nil
}

func (db Database) UpdateTimedOutInProgressRunners() error {
	tx := db.ORM.
		Model(&model.ComplianceRunner{}).
		Where("status = ?", runner.ComplianceRunnerInProgress).
		Where("updated_at < NOW() - INTERVAL '1 HOURS'").
		Updates(model.ComplianceRunner{Status: runner.ComplianceRunnerTimeOut, FailureMessage: "Job timed out"})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) RetryFailedRunners() error {
	tx := db.ORM.Exec(`UPDATE compliance_runners 
SET retry_count = retry_count + 1, status = 'CREATED', updated_at = NOW() 
WHERE status IN ? 
AND retry_count < 3 AND updated_at < NOW() - interval '5 minutes'`, []string{string(runner.ComplianceRunnerFailed), string(runner.ComplianceRunnerTimeout)})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) UpdateRunnerJob(
	id uint, status runner.ComplianceRunnerStatus, startedAt *time.Time, totalFindingCount *int, failureMsg string) error {
	crunner := model.ComplianceRunner{
		Status:            status,
		FailureMessage:    failureMsg,
		TotalFindingCount: totalFindingCount,
	}
	if startedAt != nil {
		crunner.StartedAt = *startedAt
	}
	tx := db.ORM.
		Model(&model.ComplianceRunner{}).
		Where("id = ?", id).
		Updates(crunner)
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) UpdateTimeoutQueuedRunnerJobs() error {
	tx := db.ORM.
		Model(&model.ComplianceRunner{}).
		Where("created_at < NOW() - INTERVAL '12 HOURS'").
		Where("status IN ?", []string{string(runner.ComplianceRunnerCreated), string(runner.ComplianceRunnerQueued)}).
		Updates(model.ComplianceRunner{Status: runner.ComplianceRunnerTimeOut, FailureMessage: "Job timed out"})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

func (db Database) ListRunnersWithID(ids []int64) ([]model.ComplianceRunner, error) {
	var jobs []model.ComplianceRunner
	tx := db.ORM.Where("id IN ?", ids).Find(&jobs)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return jobs, nil
}

func (db Database) ListFailedRunnersWithParentID(id uint) ([]model.ComplianceRunner, error) {
	var jobs []model.ComplianceRunner
	tx := db.ORM.Model(&model.ComplianceRunner{}).
		Where("status = ?", runner.ComplianceRunnerFailed).
		Where("parent_job_id = ?", id).
		Find(&jobs)
	if tx.Error != nil {
		if errors.Is(tx.Error, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, tx.Error
	}
	return jobs, nil
}
