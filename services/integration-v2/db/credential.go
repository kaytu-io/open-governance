package db

import (
	"fmt"
	"github.com/google/uuid"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
	"gorm.io/gorm/clause"
)

// CreateCredential creates a new credential
func (db Database) CreateCredential(s *models.Credential) error {
	tx := db.Orm.
		Model(&models.Credential{}).
		Clauses(clause.OnConflict{DoNothing: true}).
		Create(s)

	if tx.Error != nil {
		return tx.Error
	} else if tx.RowsAffected == 0 {
		return fmt.Errorf("create spn: didn't create spn due to id conflict")
	}

	return nil
}

// DeleteCredential deletes a credential
func (db Database) DeleteCredential(id uuid.UUID) error {
	tx := db.Orm.
		Where("id = ?", id.String()).
		Unscoped().
		Delete(&models.Credential{})
	if tx.Error != nil {
		return tx.Error
	}

	return nil
}

// ListCredentials list credentials
func (db Database) ListCredentials() ([]models.Credential, error) {
	var credentials []models.Credential
	tx := db.Orm.
		Model(&models.Credential{}).
		Find(&credentials)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return credentials, nil
}

// GetCredential get a credential
func (db Database) GetCredential(id uuid.UUID) (*models.Credential, error) {
	var credential models.Credential
	tx := db.Orm.
		Model(&models.Credential{}).
		Where("id = ?", id.String()).
		First(&credential)
	if tx.Error != nil {
		return nil, tx.Error
	}

	return &credential, nil
}

func (db Database) UpdateCredential(id uuid.UUID, secret string) error {
	tx := db.Orm.
		Model(&models.Credential{}).
		Where("id = ?", id.String()).Update("secret", secret)

	if tx.Error != nil {
		return tx.Error
	}

	return nil
}
