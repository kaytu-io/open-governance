package aws_account

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
	"github.com/opengovern/opengovernance/services/integration-v2/models"
)

type AWSCredentialType interface {
	interfaces.CredentialType
	CreateAWSSession() (*aws.Config, error)
}

type AWSAccountIntegration struct {
	Credential AWSCredentialType
}

func CreateAWSAccountIntegration(credentialType string, jsonData []byte) (interfaces.IntegrationType, map[string]any, error) {
	if _, ok := CredentialTypes[credentialType]; !ok {
		return nil, nil, fmt.Errorf("invalid credential type: %s", credentialType)
	}
	credentialCreator := CredentialTypes[credentialType]
	credential, mapData, err := credentialCreator(jsonData)
	awsCredential, ok := credential.(AWSCredentialType)
	if !ok {
		return nil, nil, fmt.Errorf("credential is not of type AWSCredentialType")
	}
	integration := AWSAccountIntegration{
		Credential: awsCredential,
	}
	return &integration, mapData, err
}

var CredentialTypes = map[string]interfaces.CredentialCreator{
	"aws_simple_iam_credentials":              CreateAWSSimpleIAMCredentials,
	"aws_iam_credentials_role_with_role_jump": CreateAWSIAMCredentialsRoleWithRoleJump,
}

func (i *AWSAccountIntegration) GetAnnotations() (map[string]any, error) {
	annotations := make(map[string]any)

	cfg, err := i.Credential.CreateAWSSession()
	if err != nil {
		return nil, err
	}

	// Check if the account is a standalone account (not part of any organization)
	isStandalone, err := CheckStandaloneNonOrganizationAccount(*cfg)
	if err != nil {
		return nil, err
	}
	if isStandalone {
		annotations["aws/account-type"] = "standalone"
		return annotations, nil
	}

	// Check if the account is a member of an AWS Organization
	isMember, err := CheckOrganizationMemberAccount(*cfg)
	if err != nil {
		return nil, err
	}
	if isMember {
		annotations["aws/account-type"] = "organization-member"
		return annotations, nil
	}

	// Check if the account is the master account
	isMaster, err := CheckMasterAccount(*cfg)
	if err != nil {
		return nil, err
	}
	if isMaster {
		annotations["aws/account-type"] = "organization-master"
		return annotations, nil
	}

	return annotations, nil
}

func (i *AWSAccountIntegration) GetMetadata() (map[string]any, error) {
	annotations := make(map[string]any)

	return annotations, nil
}

func (i *AWSAccountIntegration) HealthCheck() error {
	return i.Credential.HealthCheck()
}

func (i *AWSAccountIntegration) GetIntegrations() ([]models.Integration, error) {
	return i.Credential.GetIntegrations()
}
