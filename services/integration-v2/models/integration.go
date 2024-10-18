package models

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgtype"
	api "github.com/opengovern/opengovernance/services/integration-v2/api/models"
	"time"
)

type Integration struct {
	IntegrationTracker uuid.UUID `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()"` // Auto-generated UUID
	IntegrationID      string
	IntegrationName    string
	Connector          string
	Type               string
	OnboardDate        time.Time
	Metadata           pgtype.JSONB
	Annotations        pgtype.JSONB

	CredentialID uuid.UUID `gorm:"not null"` // Foreign key to Credential

	Credential Credential `gorm:"constraint:OnDelete:CASCADE;"` // Cascading delete when Integration is deleted

	Lifecycle string
	Reason    string
	LastCheck *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func (i Integration) ToApi() (*api.IntegrationItem, error) {
	if i.Metadata.Status != pgtype.Present {
		return nil, fmt.Errorf("JSONB is not present or invalid")
	}
	var metadata map[string]any
	if err := json.Unmarshal(i.Metadata.Bytes, &metadata); err != nil {
		return nil, err
	}

	return &api.IntegrationItem{
		IntegrationTracker: i.IntegrationTracker.String(),
		IntegrationName:    i.IntegrationName,
		IntegrationID:      i.IntegrationID,
		IntegrationType:    i.Type,
		Connector:          i.Connector,
		OnboardDate:        i.OnboardDate,
		Lifecycle:          i.Lifecycle,
		Reason:             i.Reason,
		LastCheck:          i.LastCheck,
		CreatedAt:          i.CreatedAt,
		UpdatedAt:          i.UpdatedAt,
		Metadata:           metadata,
	}, nil
}
