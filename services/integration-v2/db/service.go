package db

import (
	"github.com/opengovern/opengovernance/services/integration-v2/models"
	"gorm.io/gorm"
)

type Database struct {
	Orm *gorm.DB
}

func NewDatabase(orm *gorm.DB) Database {
	return Database{
		Orm: orm,
	}
}

func (db Database) Initialize() error {
	err := db.Orm.AutoMigrate(
		&models.Integration{},
		&models.Credential{},
	)
	if err != nil {
		return err
	}

	return nil
}
