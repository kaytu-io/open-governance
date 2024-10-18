package aws_account

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsConfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
)

// AWSSimpleIAMCredentials represents AWS single account credentials.
type AWSSimpleIAMCredentials struct {
	AccessKeyID     string  `json:"access_key_id" binding:"required"`
	AccessKeySecret string  `json:"access_key_secret" binding:"required"`
	RoleToAssume    *string `json:"role_to_assume,omitempty"`
}

func CreateAWSSimpleIAMCredentials(jsonData []byte) (interfaces.CredentialType, map[string]any, error) {
	var creds AWSSimpleIAMCredentials
	err := json.Unmarshal(jsonData, &creds)
	if err != nil {
		return nil, nil, err
	}

	return &creds, creds.ConvertToMap(), nil
}

func (c *AWSSimpleIAMCredentials) HealthCheck() error {
	return nil
}

func (c *AWSSimpleIAMCredentials) GetIntegrations() ([]models.Integration, error) {
	return nil, nil
}

func (c *AWSSimpleIAMCredentials) ToJSON() ([]byte, error) {
	return nil, nil
}

func (c *AWSSimpleIAMCredentials) ParseJSON([]byte) error {
	return nil
}

func (c *AWSSimpleIAMCredentials) ConvertToMap() map[string]any {
	result := map[string]any{
		"access_key_id":     c.AccessKeyID,
		"access_key_secret": c.AccessKeySecret,
	}

	// Add RoleToAssume if it's not nil
	if c.RoleToAssume != nil {
		result["role_to_assume"] = *c.RoleToAssume
	}

	return result
}

func (c *AWSSimpleIAMCredentials) CreateAWSSession() (*aws.Config, error) {
	// Create custom credentials provider
	creds := credentials.NewStaticCredentialsProvider(c.AccessKeyID, c.AccessKeySecret, "")

	// Load the AWS configuration with the custom credentials
	cfg, err := awsConfig.LoadDefaultConfig(context.TODO(),
		awsConfig.WithCredentialsProvider(creds),
		awsConfig.WithRegion("us-east-1"),
	)
	if err != nil {
		return &cfg, fmt.Errorf("unable to load SDK config, %v", err)
	}

	return &cfg, nil
}
