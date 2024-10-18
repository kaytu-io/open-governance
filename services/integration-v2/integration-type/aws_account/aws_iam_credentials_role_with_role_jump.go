package aws_account

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
)

// AWSIAMCredentialsRoleWithRoleJump represents AWS cross-account credentials.
type AWSIAMCredentialsRoleWithRoleJump struct {
	AccessKeyID              string  `json:"access_key_id" binding:"required"`
	AccessKeySecret          string  `json:"access_key_secret" binding:"required"`
	CrossAccountRoleToAssume string  `json:"cross_account_role_to_assume" binding:"required"`
	RoleToAssume             *string `json:"role_to_assume,omitempty"`
	ExternalID               *string `json:"external_id,omitempty"`
}

func CreateAWSIAMCredentialsRoleWithRoleJump(jsonData []byte) (interfaces.CredentialType, map[string]any, error) {
	var credentials AWSIAMCredentialsRoleWithRoleJump
	err := json.Unmarshal(jsonData, &credentials)
	if err != nil {
		return nil, nil, err
	}

	return &credentials, credentials.ConvertToMap(), nil
}

func (c *AWSIAMCredentialsRoleWithRoleJump) HealthCheck() error {
	return nil
}

func (c *AWSIAMCredentialsRoleWithRoleJump) GetIntegrations() ([]models.Integration, error) {
	return nil, nil
}

func (c *AWSIAMCredentialsRoleWithRoleJump) ToJSON() ([]byte, error) {
	return nil, nil
}

func (c *AWSIAMCredentialsRoleWithRoleJump) ParseJSON([]byte) error {
	return nil
}

func (c *AWSIAMCredentialsRoleWithRoleJump) ConvertToMap() map[string]any {
	result := map[string]any{
		"access_key_id":                c.AccessKeyID,
		"access_key_secret":            c.AccessKeySecret,
		"cross_account_role_to_assume": c.CrossAccountRoleToAssume,
	}

	if c.RoleToAssume != nil {
		result["role_to_assume"] = *c.RoleToAssume
	}

	if c.ExternalID != nil {
		result["external_id"] = *c.ExternalID
	}

	return result
}

func (c *AWSIAMCredentialsRoleWithRoleJump) CreateAWSSession() (*aws.Config, error) {
	return nil, nil
}
