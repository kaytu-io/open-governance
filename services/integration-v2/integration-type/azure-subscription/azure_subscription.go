package azure_subscription

import (
	"fmt"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
)

type AzureSubscriptionIntegration struct {
	Credential interfaces.CredentialType
}

func CreateAzureSubscriptionIntegration(credentialType string, jsonData []byte) (interfaces.IntegrationType, map[string]any, error) {
	if _, ok := CredentialTypes[credentialType]; !ok {
		return nil, nil, fmt.Errorf("invalid credential type: %s", credentialType)
	}
	credentialCreator := CredentialTypes[credentialType]
	credential, mapData, err := credentialCreator(jsonData)
	integration := AzureSubscriptionIntegration{
		Credential: credential,
	}
	return &integration, mapData, err
}

var CredentialTypes = map[string]interfaces.CredentialCreator{
	"azure_spn_password":    CreateAzureSPNPasswordCredentials,
	"azure_spn_certificate": CreateAzureSPNCertificateCredentials,
}

func (i *AzureSubscriptionIntegration) GetAnnotations() (map[string]any, error) {
	annotations := make(map[string]any)

	return annotations, nil
}

func (i *AzureSubscriptionIntegration) GetMetadata() (map[string]any, error) {
	annotations := make(map[string]any)

	return annotations, nil
}

func (i *AzureSubscriptionIntegration) HealthCheck() error {
	return i.Credential.HealthCheck()
}

func (i *AzureSubscriptionIntegration) GetIntegrations() ([]models.Integration, error) {
	return i.Credential.GetIntegrations()
}
