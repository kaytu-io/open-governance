package models

import (
	"time"
)

type CreateRequest struct {
	IntegrationType string         `json:"integration_type"`
	CredentialType  string         `json:"credential_type"`
	Credentials     map[string]any `json:"credentials"`
}

type UpdateRequest struct {
	Config map[string]any `json:"config"`
}

type CredentialItem struct {
	ID             string `json:"id"`
	CredentialType string `json:"credential_type"`
}

type IntegrationItem struct {
	IntegrationTracker string         `json:"integration_tracker"`
	IntegrationID      string         `json:"integration_id"`
	IntegrationName    string         `json:"integration_name"`
	Connector          string         `json:"connector"`
	IntegrationType    string         `json:"integration_type"`
	OnboardDate        time.Time      `json:"onboard_date"`
	Metadata           map[string]any `json:"metadata"`

	Lifecycle string
	Reason    string
	LastCheck *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
}

type ListResponse struct {
	Integrations []IntegrationItem `json:"integrations"`
	TotalCount   int               `json:"total_count"`
}
