package api

import (
	"time"

	"github.com/google/uuid"
	"github.com/kaytu-io/kaytu-util/pkg/source"
)

type ConnectionState string

const (
	ConnectionState_ENABLED  ConnectionState = "ENABLED"
	ConnectionState_DISABLED ConnectionState = "DISABLED"
)

type ConnectionLifecycleState string

const (
	ConnectionLifecycleStatePending          ConnectionLifecycleState = "pending"
	ConnectionLifecycleStateInitialDiscovery ConnectionLifecycleState = "initial-discovery"
	ConnectionLifecycleStateEnabled          ConnectionLifecycleState = "enabled"
	ConnectionLifecycleStateDisabled         ConnectionLifecycleState = "disabled"
	ConnectionLifecycleStateDeleted          ConnectionLifecycleState = "deleted"
)

type ConnectionCountRequest struct {
	ConnectorsNames []string             `json:"connectors"`
	State           *ConnectionState     `json:"state"`
	Health          *source.HealthStatus `json:"health"`
}

type Connection struct {
	ID                   uuid.UUID                       `json:"id"`
	ConnectionID         string                          `json:"providerConnectionID"`
	ConnectionName       string                          `json:"providerConnectionName"`
	Email                string                          `json:"email"`
	Connector            source.Type                     `json:"connector"`
	Description          string                          `json:"description"`
	CredentialID         string                          `json:"credentialID"`
	CredentialName       *string                         `json:"credentialName,omitempty"`
	OnboardDate          time.Time                       `json:"onboardDate"`
	LifecycleState       ConnectionLifecycleState        `json:"lifecycleState"`
	AssetDiscoveryMethod source.AssetDiscoveryMethodType `json:"assetDiscoveryMethod"`
	HealthState          source.HealthStatus             `json:"healthState"`
	LastHealthCheckTime  time.Time                       `json:"lastHealthCheckTime"`
	HealthReason         *string                         `json:"healthReason,omitempty"`

	LastInventory *time.Time `json:"lastInventory,omitempty"`
	Cost          *float64   `json:"cost,omitempty"`
	ResourceCount *int       `json:"resourceCount,omitempty"`

	Metadata map[string]any `json:"metadata"`
}

type ListConnectionSummaryResponse struct {
	ConnectionCount     int          `json:"connectionCount"`
	TotalCost           float64      `json:"totalCost"`
	TotalResourceCount  int          `json:"TotalResourceCount"`
	TotalUnhealthyCount int          `json:"totalUnhealthyCount"`
	TotalDisabledCount  int          `json:"totalDisabledCount"`
	Connections         []Connection `json:"connections"`
}
