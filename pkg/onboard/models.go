package onboard

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/subscription/mgmt/subscription"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/google/uuid"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"gitlab.com/keibiengine/keibi-engine/pkg/onboard/api"
	"gorm.io/datatypes"
)

type ConnectionLifecycleState string

const (
	ConnectionLifecycleStatePending          ConnectionLifecycleState = "pending"
	ConnectionLifecycleStateInitialDiscovery ConnectionLifecycleState = "initial-discovery"
	ConnectionLifecycleStateEnabled          ConnectionLifecycleState = "enabled"
	ConnectionLifecycleStateDisabled         ConnectionLifecycleState = "disabled"
	ConnectionLifecycleStateDeleted          ConnectionLifecycleState = "deleted"
)

type Source struct {
	ID             uuid.UUID `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()"` // Auto-generated UUID
	SourceId       string    `gorm:"index:idx_source_id,unique"`                      // AWS Account ID, Azure Subscription ID, ...
	Name           string    `gorm:"not null"`
	Email          string
	Type           source.Type `gorm:"not null"`
	Description    string
	CredentialID   uuid.UUID
	Enabled        bool
	LifecycleState ConnectionLifecycleState `gorm:"not null;default:'enabled'"`

	AssetDiscoveryMethod source.AssetDiscoveryMethodType `gorm:"not null;default:'scheduled'"`

	LastHealthCheckTime time.Time           `gorm:"not null;default:now()"`
	HealthState         source.HealthStatus `gorm:"not null;default:'unhealthy'"`
	HealthReason        *string

	Connector  Connector  `gorm:"foreignKey:Type;references:Name;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
	Credential Credential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"-"`

	CreationMethod source.SourceCreationMethod `gorm:"not null;default:'manual'"`

	Metadata datatypes.JSON

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func (s Source) toApi() api.Connection {
	metadata := make(map[string]any)
	if s.Metadata.String() != "" {
		_ = json.Unmarshal(s.Metadata, &metadata)
	}
	apiCon := api.Connection{
		ID:                   s.ID,
		ConnectionID:         s.SourceId,
		ConnectionName:       s.Name,
		Email:                s.Email,
		Connector:            s.Type,
		Description:          s.Description,
		CredentialID:         s.CredentialID.String(),
		CredentialName:       s.Credential.Name,
		OnboardDate:          s.CreatedAt,
		LifecycleState:       api.ConnectionLifecycleState(s.LifecycleState),
		AssetDiscoveryMethod: s.AssetDiscoveryMethod,
		HealthState:          s.HealthState,
		LastHealthCheckTime:  s.LastHealthCheckTime,
		HealthReason:         s.HealthReason,
		Metadata:             metadata,

		ResourceCount: nil,
		Cost:          nil,
		LastInventory: nil,
	}
	return apiCon
}

type AWSConnectionMetadata struct {
	AccountID           string              `json:"account_id"`
	AccountName         string              `json:"account_name"`
	Organization        *types.Organization `json:"account_organization,omitempty"`
	OrganizationAccount *types.Account      `json:"organization_account,omitempty"`
}

func NewAWSConnectionMetadata(account awsAccount) AWSConnectionMetadata {
	metadata := AWSConnectionMetadata{
		AccountID: account.AccountID,
	}
	metadata.Organization = account.Organization
	metadata.OrganizationAccount = account.Account

	return metadata
}

func NewAWSSource(account awsAccount, description string) Source {
	id := uuid.New()
	provider := source.CloudAWS

	metadata := NewAWSConnectionMetadata(account)

	marshalMetadata, err := json.Marshal(metadata)
	if err != nil {
		marshalMetadata = []byte("{}")
	}

	credName := fmt.Sprintf("%s - %s - default credentials", provider, account.AccountID)
	creds := Credential{
		ID:             uuid.New(),
		Name:           &credName,
		ConnectorType:  provider,
		Secret:         "",
		CredentialType: source.CredentialTypeAutoGenerated,
	}

	accountName := account.AccountID
	if account.AccountName != nil {
		accountName = *account.AccountName
	}
	accountEmail := ""
	if account.Account != nil && account.Account.Email != nil {
		accountEmail = *account.Account.Email
	}

	s := Source{
		ID:                   id,
		SourceId:             account.AccountID,
		Name:                 accountName,
		Email:                accountEmail,
		Type:                 provider,
		Description:          description,
		CredentialID:         creds.ID,
		Credential:           creds,
		Enabled:              true,
		LifecycleState:       ConnectionLifecycleStateInitialDiscovery,
		AssetDiscoveryMethod: source.AssetDiscoveryMethodTypeScheduled,
		HealthState:          source.HealthStatusHealthy,
		LastHealthCheckTime:  time.Now(),
		CreationMethod:       source.SourceCreationMethodManual,
		Metadata:             datatypes.JSON(marshalMetadata),
	}

	if len(strings.TrimSpace(s.Name)) == 0 {
		s.Name = s.SourceId
	}

	return s
}

type AzureConnectionMetadata struct {
	SubscriptionID string             `json:"subscription_id"`
	SubModel       subscription.Model `json:"subscription_model"`
}

func NewAzureConnectionMetadata(sub azureSubscription) AzureConnectionMetadata {
	metadata := AzureConnectionMetadata{
		SubscriptionID: sub.SubscriptionID,
		SubModel:       sub.SubModel,
	}

	return metadata
}

func NewAzureSourceWithCredentials(sub azureSubscription, creationMethod source.SourceCreationMethod, description string, creds Credential) Source {
	id := uuid.New()

	name := sub.SubscriptionID
	if sub.SubModel.DisplayName != nil {
		name = *sub.SubModel.DisplayName
	}

	metadata := NewAzureConnectionMetadata(sub)
	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		jsonMetadata = []byte("{}")
	}

	s := Source{
		ID:                   id,
		SourceId:             sub.SubscriptionID,
		Name:                 name,
		Description:          description,
		Type:                 source.CloudAzure,
		CredentialID:         creds.ID,
		Credential:           creds,
		Enabled:              true,
		LifecycleState:       ConnectionLifecycleStateInitialDiscovery,
		AssetDiscoveryMethod: source.AssetDiscoveryMethodTypeScheduled,
		HealthState:          source.HealthStatusHealthy,
		LastHealthCheckTime:  time.Now(),
		CreationMethod:       creationMethod,
		Metadata:             datatypes.JSON(jsonMetadata),
	}

	return s
}

func (s Source) ToSourceResponse() *api.CreateSourceResponse {
	return &api.CreateSourceResponse{
		ID: s.ID,
	}
}

type Connector struct {
	Name                source.Type `gorm:"primaryKey"`
	Label               string
	ShortDescription    string
	Description         string
	Direction           source.ConnectorDirectionType `gorm:"default:'ingress'"`
	Status              source.ConnectorStatus        `gorm:"default:'enabled'"`
	Logo                string                        `gorm:"default:''"`
	AutoOnboardSupport  bool                          `gorm:"default:false"`
	AllowNewConnections bool                          `gorm:"default:true"`
	MaxConnectionLimit  int                           `gorm:"default:25"`
	Tags                datatypes.JSON                `gorm:"default:'{}'"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

type Credential struct {
	ID                 uuid.UUID             `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()" json:"id"`
	Name               *string               `json:"name,omitempty"`
	ConnectorType      source.Type           `gorm:"not null" json:"connectorType"`
	Secret             string                `gorm:"" json:"-"`
	CredentialType     source.CredentialType `gorm:"default:'auto-generated'" json:"credentialType"`
	Enabled            bool                  `gorm:"default:true" json:"enabled"`
	AutoOnboardEnabled bool                  `gorm:"default:false" json:"autoOnboardEnabled"`

	LastHealthCheckTime time.Time           `gorm:"not null;default:now()" json:"lastHealthCheckTime"`
	HealthStatus        source.HealthStatus `gorm:"not null;default:'healthy'" json:"healthStatus"`
	HealthReason        *string             `json:"healthReason,omitempty"`

	Metadata datatypes.JSON `json:"metadata,omitempty"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func NewAzureCredential(name string, credentialType source.CredentialType, metadata *source.AzureCredentialMetadata) (*Credential, error) {
	id := uuid.New()
	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	return &Credential{
		ID:             id,
		Name:           &name,
		ConnectorType:  source.CloudAzure,
		Secret:         fmt.Sprintf("sources/%s/%s", strings.ToLower(string(source.CloudAzure)), id),
		CredentialType: credentialType,
		Metadata:       jsonMetadata,
	}, nil
}

func NewAWSCredential(name string, metadata *source.AWSCredentialMetadata) (*Credential, error) {
	id := uuid.New()
	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		return nil, err
	}
	return &Credential{
		ID:             id,
		Name:           &name,
		ConnectorType:  source.CloudAWS,
		Secret:         fmt.Sprintf("sources/%s/%s", strings.ToLower(string(source.CloudAWS)), id),
		CredentialType: source.CredentialTypeManual,
		Metadata:       jsonMetadata,
	}, nil
}
