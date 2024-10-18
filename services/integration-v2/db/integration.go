package db

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
	"gorm.io/gorm/clause"
)

// CreateIntegration creates a new integration
func (db Database) CreateIntegration(s *models.Integration) error {
	tx := db.Orm.
		Model(&models.Integration{}).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(s)

	if tx.Error != nil {
		return tx.Error
	} else if tx.RowsAffected == 0 {
		return fmt.Errorf("create spn: didn't create spn due to id conflict")
	}

	return nil
}

// DeleteIntegration deletes a integration
func (db Database) DeleteIntegration(id uuid.UUID) error {
	tx := db.Orm.
		Where("id = ?", id.String()).
		Unscoped().
		Delete(&models.Integration{})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

// ListIntegration list Integration
func (db Database) ListIntegration() ([]models.Integration, error) {
	var integrations []models.Integration
	tx := db.Orm.
		Model(&models.Integration{}).
		Find(&integrations)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return integrations, nil
}

// GetIntegration get a Integration
func (db Database) GetIntegration(id uuid.UUID) (*models.Integration, error) {
	var integration models.Integration
	tx := db.Orm.
		Model(&models.Integration{}).
		Where("id = ?", id.String()).
		First(&integration)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return &integration, nil
}

func (db Database) UpdateIntegration(id uuid.UUID, secret string) error {
	tx := db.Orm.
		Model(&models.Integration{}).
		Where("id = ?", id.String()).Update("secret", secret)

	if tx.Error != nil {
		return tx.Error
	}

	return nil
}
