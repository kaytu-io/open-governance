package azure_subscription

import (
	"encoding/json"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
)

// AzureSPNCertificateCredentials represents Azure SPN credentials using a certificate.
type AzureSPNCertificateCredentials struct {
	AzureClientID                  string  `json:"azure_client_id" binding:"required"`
	AzureTenantID                  string  `json:"azure_tenant_id" binding:"required"`
	AzureSPNCertificate            string  `json:"azure_spn_certificate" binding:"required"`
	AzureClientCertificatePassword *string `json:"azure_client_certificate_password,omitempty"`
	AzureSPNObjectID               *string `json:"azure_spn_object_id,omitempty"`
}

func CreateAzureSPNCertificateCredentials(jsonData []byte) (interfaces.CredentialType, map[string]any, error) {
	var credentials AzureSPNCertificateCredentials
	err := json.Unmarshal(jsonData, &credentials)
	if err != nil {
		return nil, nil, err
	}

	return &credentials, credentials.ConvertToMap(), nil
}

func (c *AzureSPNCertificateCredentials) HealthCheck() error {
	return nil
}

func (c *AzureSPNCertificateCredentials) GetIntegrations() ([]models.Integration, error) {
	return nil, nil
}

func (c *AzureSPNCertificateCredentials) ToJSON() ([]byte, error) {
	return nil, nil
}

func (c *AzureSPNCertificateCredentials) ParseJSON([]byte) error {
	return nil
}

func (c *AzureSPNCertificateCredentials) ConvertToMap() map[string]any {
	result := map[string]any{
		"azure_client_id":       c.AzureClientID,
		"azure_tenant_id":       c.AzureTenantID,
		"azure_spn_certificate": c.AzureSPNCertificate,
	}

	// Add optional fields if they are not nil
	if c.AzureClientCertificatePassword != nil {
		result["azure_client_certificate_password"] = *c.AzureClientCertificatePassword
	}
	if c.AzureSPNObjectID != nil {
		result["azure_spn_object_id"] = *c.AzureSPNObjectID
	}

	return result
}
