package integration_type

import (
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/aws_account"
	azure_subscription "github.com/opengovern/opengovernance/services/integration-v2/integration-type/azure-subscription"
	"github.com/opengovern/opengovernance/services/integration-v2/integration-type/interfaces"
)

var IntegrationTypes = map[string]interfaces.IntegrationCreator{
	"aws_account":        aws_account.CreateAWSAccountIntegration,
	"azure_subscription": azure_subscription.CreateAzureSubscriptionIntegration,
}
