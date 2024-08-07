package db

import (
	"github.com/kaytu-io/kaytu-engine/pkg/describe/db/model"
	"gorm.io/gorm"
)

type Database struct {
	ORM *gorm.DB
}

func (db Database) Initialize() error {
	return db.ORM.AutoMigrate(&model.ComplianceJob{}, &model.ComplianceSummarizer{}, &model.ComplianceRunner{}, &model.CheckupJob{},
		&model.AnalyticsJob{}, &model.DescribeConnectionJob{},
		&model.JobSequencer{},
	)
}
