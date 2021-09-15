package describer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/workspaces"
)

func WorkSpacesConnectionAlias(ctx context.Context, cfg aws.Config) ([]interface{}, error) {
	client := workspaces.NewFromConfig(cfg)

	var values []interface{}
	err := PaginateRetrieveAll(func(prevToken *string) (nextToken *string, err error) {
		output, err := client.DescribeConnectionAliases(ctx, &workspaces.DescribeConnectionAliasesInput{
			NextToken: prevToken,
		})
		if err != nil {
			return nil, err
		}

		for _, v := range output.ConnectionAliases {
			values = append(values, v)
		}

		return output.NextToken, nil
	})
	if err != nil {
		return nil, err
	}

	return values, nil
}

func WorkSpacesWorkspace(ctx context.Context, cfg aws.Config) ([]interface{}, error) {
	client := workspaces.NewFromConfig(cfg)
	paginator := workspaces.NewDescribeWorkspacesPaginator(client, &workspaces.DescribeWorkspacesInput{})

	var values []interface{}
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.Workspaces {
			values = append(values, v)
		}
	}

	return values, nil
}
