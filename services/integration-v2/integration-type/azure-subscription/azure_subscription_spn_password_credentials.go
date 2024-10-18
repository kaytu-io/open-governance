package azure_subscription

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/resources/armresources"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/subscription/armsubscription"
	"github.com/golang-jwt/jwt"
	"github.com/google/uuid"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
	"github.com/opengovern/opengovernance/services/integration/model"
	"time"
)

// AzureSPNPasswordCredentials represents Azure SPN credentials using a password.
type AzureSPNPasswordCredentials struct {
	AzureClientID       string  `json:"azure_client_id" binding:"required"`
	AzureTenantID       string  `json:"azure_tenant_id" binding:"required"`
	AzureClientPassword string  `json:"azure_client_password" binding:"required"`
	AzureSPNObjectID    *string `json:"azure_spn_object_id,omitempty"`
}

func CreateAzureSPNPasswordCredentials(jsonData []byte) (interfaces.CredentialType, map[string]any, error) {
	var credentials AzureSPNPasswordCredentials
	err := json.Unmarshal(jsonData, &credentials)
	if err != nil {
		return nil, nil, err
	}

	return &credentials, credentials.ConvertToMap(), nil
}

func (c *AzureSPNPasswordCredentials) HealthCheck() error {
	cred, err := azidentity.NewClientSecretCredential(c.AzureTenantID, c.AzureClientID, c.AzureClientPassword, nil)
	if err != nil {
		return fmt.Errorf("failed to create ClientSecretCredential: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	scopes := []string{"https://management.azure.com/.default"}

	token, err := cred.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: scopes,
	})
	if err != nil {
		return fmt.Errorf("failed to acquire token: %v", err)
	}

	_, err = ExtractObjectID(token.Token)
	if err != nil {
		return fmt.Errorf("failed to extract object ID from token: %v", err)
	}

	return nil
}

func (c *AzureSPNPasswordCredentials) GetIntegrations() ([]models.Integration, error) {
	ctx := context.Background()
	identity, err := azidentity.NewClientSecretCredential(
		c.AzureTenantID,
		c.AzureClientID,
		c.AzureClientPassword,
		nil)
	if err != nil {
		return nil, err
	}
	client, err := armsubscription.NewSubscriptionsClient(identity, nil)
	if err != nil {
		return nil, err
	}

	it := client.NewListPager(nil)
	subs := make([]model.AzureSubscription, 0)
	for it.More() {
		page, err := it.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, v := range page.Value {
			if v == nil || v.State == nil {
				continue
			}
			tagsClient, err := armresources.NewTagsClient(*v.SubscriptionID, identity, nil)
			if err != nil {
				return nil, err
			}
			tagIt := tagsClient.NewListPager(nil)
			tagList := make([]armresources.TagDetails, 0)
			for tagIt.More() {
				tagPage, err := tagIt.NextPage(ctx)
				if err != nil {
					return nil, err
				}
				for _, tag := range tagPage.Value {
					tagList = append(tagList, *tag)
				}
			}
			localV := v
			subs = append(subs, model.AzureSubscription{
				SubscriptionID: *v.SubscriptionID,
				SubModel:       *localV,
				SubTags:        tagList,
			})
		}
	}

	var integrations []models.Integration
	for _, sub := range subs {
		var name string
		if sub.SubModel.DisplayName != nil {
			name = *sub.SubModel.DisplayName
		}
		integrations = append(integrations, models.Integration{
			IntegrationTracker: uuid.New(),
			IntegrationID:      sub.SubscriptionID,
			IntegrationName:    name,
			Connector:          "Azure",
			Type:               "azure_subscription_spn_password_credentials",
			OnboardDate:        time.Now(),
		})
	}
	return integrations, nil
}

func (c *AzureSPNPasswordCredentials) ToJSON() ([]byte, error) {
	return nil, nil
}

func (c *AzureSPNPasswordCredentials) ParseJSON([]byte) error {
	return nil
}

func (c *AzureSPNPasswordCredentials) ConvertToMap() map[string]any {
	result := map[string]any{
		"azure_client_id":       c.AzureClientID,
		"azure_tenant_id":       c.AzureTenantID,
		"azure_client_password": c.AzureClientPassword,
	}

	// Add optional field if it is not nil
	if c.AzureSPNObjectID != nil {
		result["azure_spn_object_id"] = *c.AzureSPNObjectID
	}

	return result
}

// ExtractObjectID parses the token and extracts the object ID (oid claim).
func ExtractObjectID(tokenString string) (string, error) {
	token, _, err := new(jwt.Parser).ParseUnverified(tokenString, jwt.MapClaims{})
	if err != nil {
		return "", fmt.Errorf("failed to parse token: %v", err)
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if oid, ok := claims["oid"].(string); ok {
			return oid, nil
		}
		return "", fmt.Errorf("oid claim not found in token")
	}
	return "", fmt.Errorf("failed to parse claims")
}
