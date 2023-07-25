package onboard

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/profiles/latest/subscription/mgmt/subscription"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/google/uuid"
	"github.com/kaytu-io/kaytu-engine/pkg/onboard/api"
	"github.com/kaytu-io/kaytu-util/pkg/source"
	"gorm.io/datatypes"
)

type ConnectionLifecycleState string

const (
	ConnectionLifecycleStateNotOnboard ConnectionLifecycleState = "NOT_ONBOARD"
	ConnectionLifecycleStateInProgress ConnectionLifecycleState = "IN_PROGRESS"
	ConnectionLifecycleStateOnboard    ConnectionLifecycleState = "ONBOARD"
	ConnectionLifecycleStateUnhealthy  ConnectionLifecycleState = "UNHEALTHY"
	ConnectionLifecycleStateArchived   ConnectionLifecycleState = "ARCHIVED"
)

func (c ConnectionLifecycleState) IsEnabled() bool {
	enabledStates := []ConnectionLifecycleState{ConnectionLifecycleStateOnboard, ConnectionLifecycleStateInProgress, ConnectionLifecycleStateUnhealthy}
	for _, state := range enabledStates {
		if c == state {
			return true
		}
	}
	return false
}

func (c ConnectionLifecycleState) ToApi() api.ConnectionLifecycleState {
	return api.ConnectionLifecycleState(c)
}

func ConnectionLifecycleStateFromApi(state api.ConnectionLifecycleState) ConnectionLifecycleState {
	return ConnectionLifecycleState(state)
}

type Source struct {
	ID           uuid.UUID `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()"` // Auto-generated UUID
	SourceId     string    `gorm:"index:idx_source_id,unique"`                      // AWS Account ID, Azure Subscription ID, ...
	Name         string    `gorm:"not null"`
	Email        string
	Type         source.Type `gorm:"not null"`
	Description  string
	CredentialID uuid.UUID

	LifecycleState ConnectionLifecycleState `gorm:"not null;default:'enabled'"`

	AssetDiscoveryMethod source.AssetDiscoveryMethodType `gorm:"not null;default:'scheduled'"`

	LastHealthCheckTime time.Time `gorm:"not null;default:now()"`
	HealthReason        *string

	Connector  Connector  `gorm:"foreignKey:Type;references:Name;constraint:OnUpdate:CASCADE,OnDelete:CASCADE"`
	Credential Credential `gorm:"foreignKey:CredentialID;references:ID;constraint:OnUpdate:CASCADE,OnDelete:SET NULL" json:"-"`

	CreationMethod source.SourceCreationMethod `gorm:"not null;default:'manual'"`

	Metadata datatypes.JSON `gorm:"default:'{}'"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func (s Source) toAPI() api.Connection {
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
		CredentialType:       s.Credential.CredentialType.ToApi(),
		OnboardDate:          s.CreatedAt,
		LifecycleState:       api.ConnectionLifecycleState(s.LifecycleState),
		AssetDiscoveryMethod: s.AssetDiscoveryMethod,
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
	OrganizationTags    map[string]string   `json:"organization_tags,omitempty"`
}

func NewAWSConnectionMetadata(cfg aws.Config, account awsAccount) (AWSConnectionMetadata, error) {
	metadata := AWSConnectionMetadata{
		AccountID: account.AccountID,
	}
	if account.AccountName != nil {
		metadata.AccountName = *account.AccountName
	}
	metadata.Organization = account.Organization
	metadata.OrganizationAccount = account.Account
	if account.Organization != nil && account.Account != nil {
		organizationClient := organizations.NewFromConfig(cfg)
		tags, err := organizationClient.ListTagsForResource(context.TODO(), &organizations.ListTagsForResourceInput{
			ResourceId: account.Account.Id,
		})
		if err == nil {
			return metadata, err
		}
		metadata.OrganizationTags = make(map[string]string)
		for _, tag := range tags.Tags {
			if tag.Key == nil || tag.Value == nil {
				continue
			}
			metadata.OrganizationTags[*tag.Key] = *tag.Value
		}
	}

	return metadata, nil
}

func NewAWSSource(cfg aws.Config, account awsAccount, description string) Source {
	id := uuid.New()
	provider := source.CloudAWS

	metadata, err := NewAWSConnectionMetadata(cfg, account)
	if err != nil {
		// TODO: log error
	}

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
		CredentialType: CredentialTypeAutoAws,
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
		LifecycleState:       ConnectionLifecycleStateInProgress,
		AssetDiscoveryMethod: source.AssetDiscoveryMethodTypeScheduled,
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

func NewAzureConnectionWithCredentials(sub azureSubscription, creationMethod source.SourceCreationMethod, description string, creds Credential) Source {
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
		LifecycleState:       ConnectionLifecycleStateInProgress,
		AssetDiscoveryMethod: source.AssetDiscoveryMethodTypeScheduled,
		LastHealthCheckTime:  time.Now(),
		CreationMethod:       creationMethod,
		Metadata:             datatypes.JSON(jsonMetadata),
	}

	return s
}

func NewAWSConnectionWithCredentials(cfg aws.Config, account awsAccount, creationMethod source.SourceCreationMethod, description string, creds Credential) Source {
	id := uuid.New()

	name := account.AccountID
	if account.AccountName != nil {
		name = *account.AccountName
	}

	metadata, err := NewAWSConnectionMetadata(cfg, account)
	if err != nil {
		// TODO: log error
	}
	jsonMetadata, err := json.Marshal(metadata)
	if err != nil {
		jsonMetadata = []byte("{}")
	}

	s := Source{
		ID:                   id,
		SourceId:             account.AccountID,
		Name:                 name,
		Description:          description,
		Type:                 source.CloudAWS,
		CredentialID:         creds.ID,
		Credential:           creds,
		LifecycleState:       ConnectionLifecycleStateNotOnboard,
		AssetDiscoveryMethod: source.AssetDiscoveryMethodTypeScheduled,
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

type CredentialType string

const (
	CredentialTypeAutoAzure             CredentialType = "auto-azure"
	CredentialTypeAutoAws               CredentialType = "auto-aws"
	CredentialTypeManualAwsOrganization CredentialType = "manual-aws-org"
	CredentialTypeManualAzureSpn        CredentialType = "manual-azure-spn"
)

func (c CredentialType) IsManual() bool {
	for _, t := range GetManualCredentialTypes() {
		if t == c {
			return true
		}
	}
	return false
}

func GetCredentialTypes() []CredentialType {
	return []CredentialType{
		CredentialTypeAutoAzure,
		CredentialTypeAutoAws,
		CredentialTypeManualAwsOrganization,
		CredentialTypeManualAzureSpn,
	}
}

func GetAutoGeneratedCredentialTypes() []CredentialType {
	return []CredentialType{
		CredentialTypeAutoAzure,
		CredentialTypeAutoAws,
	}
}

func GetManualCredentialTypes() []CredentialType {
	return []CredentialType{
		CredentialTypeManualAwsOrganization,
		CredentialTypeManualAzureSpn,
	}
}

func (c CredentialType) ToApi() api.CredentialType {
	return api.CredentialType(c)
}

func ParseCredentialType(s string) CredentialType {
	for _, t := range GetCredentialTypes() {
		if strings.ToLower(string(t)) == strings.ToLower(s) {
			return t
		}
	}
	return ""
}

func ParseCredentialTypes(s []string) []CredentialType {
	var ctypes []CredentialType
	for _, t := range s {
		ctypes = append(ctypes, ParseCredentialType(t))
	}
	return ctypes
}

type Credential struct {
	ID                 uuid.UUID      `gorm:"primaryKey;type:uuid;default:uuid_generate_v4()" json:"id"`
	Name               *string        `json:"name,omitempty"`
	ConnectorType      source.Type    `gorm:"not null" json:"connectorType"`
	Secret             string         `json:"-"`
	CredentialType     CredentialType `json:"credentialType"`
	Enabled            bool           `gorm:"default:true" json:"enabled"`
	AutoOnboardEnabled bool           `gorm:"default:false" json:"autoOnboardEnabled"`

	LastHealthCheckTime time.Time           `gorm:"not null;default:now()" json:"lastHealthCheckTime"`
	HealthStatus        source.HealthStatus `gorm:"not null;default:'healthy'" json:"healthStatus"`
	HealthReason        *string             `json:"healthReason,omitempty"`

	Metadata datatypes.JSON `json:"metadata,omitempty" gorm:"default:'{}'"`

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt sql.NullTime `gorm:"index"`
}

func (credential *Credential) ToAPI() api.Credential {
	metadata := make(map[string]any)
	if string(credential.Metadata) == "" {
		credential.Metadata = []byte("{}")
	}
	_ = json.Unmarshal(credential.Metadata, &metadata)
	apiCredential := api.Credential{
		ID:                  credential.ID.String(),
		Name:                credential.Name,
		ConnectorType:       credential.ConnectorType,
		CredentialType:      credential.CredentialType.ToApi(),
		Enabled:             credential.Enabled,
		AutoOnboardEnabled:  credential.AutoOnboardEnabled,
		OnboardDate:         credential.CreatedAt,
		LastHealthCheckTime: credential.LastHealthCheckTime,
		HealthStatus:        credential.HealthStatus,
		HealthReason:        credential.HealthReason,
		Metadata:            metadata,

		Config: nil,

		Connections:          nil,
		TotalConnections:     nil,
		EnabledConnections:   nil,
		UnhealthyConnections: nil,
	}

	return apiCredential
}

func NewAzureCredential(name string, credentialType CredentialType, metadata *AzureCredentialMetadata) (*Credential, error) {
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

func NewAWSCredential(name string, metadata *AWSCredentialMetadata, credentialType CredentialType) (*Credential, error) {
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
		CredentialType: credentialType,
		Metadata:       jsonMetadata,
	}, nil
}

type AWSCredentialMetadata struct {
	AccountID                          string    `json:"account_id"`
	IamUserName                        *string   `json:"iam_user_name"`
	IamApiKeyCreationDate              time.Time `json:"iam_api_key_creation_date"`
	AttachedPolicies                   []string  `json:"attached_policies"`
	OrganizationID                     *string   `json:"organization_id"`
	OrganizationMasterAccountEmail     *string   `json:"organization_master_account_email"`
	OrganizationMasterAccountId        *string   `json:"organization_master_account_id"`
	OrganizationDiscoveredAccountCount *int      `json:"organization_discovered_account_count"`
}

type AzureCredentialMetadata struct {
	SpnName              string    `json:"spn_name"`
	ObjectId             string    `json:"object_id"`
	SecretId             string    `json:"secret_id"`
	SecretExpirationDate time.Time `json:"secret_expiration_date"`
}

func (m AzureCredentialMetadata) GetExpirationDate() time.Time {
	return m.SecretExpirationDate
}
