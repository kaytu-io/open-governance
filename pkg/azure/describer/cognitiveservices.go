package describer

import (
	"context"
	"strings"

	"github.com/Azure/azure-sdk-for-go/profiles/2020-09-01/monitor/mgmt/insights"
	"github.com/Azure/azure-sdk-for-go/services/cognitiveservices/mgmt/2021-04-30/cognitiveservices"
	"github.com/Azure/go-autorest/autorest"
	"gitlab.com/keibiengine/keibi-engine/pkg/azure/model"
)

func CognitiveAccount(ctx context.Context, authorizer autorest.Authorizer, subscription string) ([]Resource, error) {
	client := insights.NewDiagnosticSettingsClient(subscription)
	client.Authorizer = authorizer

	accountsClient := cognitiveservices.NewAccountsClient(subscription)
	accountsClient.Authorizer = authorizer

	result, err := accountsClient.List(ctx)
	if err != nil {
		return nil, err
	}

	var values []Resource
	for {
		for _, account := range result.Values() {
			resourceGroup := strings.Split(*account.ID, "/")[4]

			id := *account.ID
			cognitiveservicesListOp, err := client.List(ctx, id)
			if err != nil {
				return nil, err
			}

			values = append(values, Resource{
				ID:       *account.ID,
				Name:     *account.Name,
				Location: *account.Location,
				Description: model.CognitiveAccountDescription{
					Account:                     account,
					DiagnosticSettingsResources: cognitiveservicesListOp.Value,
					ResourceGroup:               resourceGroup,
				},
			})
		}
		if !result.NotDone() {
			break
		}
		err = result.NextWithContext(ctx)
		if err != nil {
			return nil, err
		}
	}
	return values, nil
}
