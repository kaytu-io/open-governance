package describer

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/rds"
	"github.com/aws/aws-sdk-go-v2/service/rds/types"
)

type RDSDBClusterDescription struct {
	DBCluster types.DBCluster
}

func RDSDBCluster(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBClustersPaginator(client, &rds.DescribeDBClustersInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBClusters {
			values = append(values, Resource{
				ARN: *v.DBClusterArn,
				Description: RDSDBClusterDescription{
					DBCluster: v,
				},
			})
		}
	}

	return values, nil
}

type RDSDBClusterSnapshotDescription struct {
	DBClusterSnapshot types.DBClusterSnapshot
	Attributes        *types.DBClusterSnapshotAttributesResult
}

func RDSDBClusterSnapshot(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBClusterSnapshotsPaginator(client, &rds.DescribeDBClusterSnapshotsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBClusterSnapshots {
			attr, err := client.DescribeDBClusterSnapshotAttributes(ctx, &rds.DescribeDBClusterSnapshotAttributesInput{
				DBClusterSnapshotIdentifier: v.DBClusterSnapshotIdentifier,
			})
			if err != nil {
				return nil, err
			}

			values = append(values, Resource{
				ARN: *v.DBClusterSnapshotArn,
				Description: RDSDBClusterSnapshotDescription{
					DBClusterSnapshot: v,
					Attributes:        attr.DBClusterSnapshotAttributesResult,
				},
			})
		}
	}

	return values, nil
}

func RDSDBClusterParameterGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBClusterParameterGroupsPaginator(client, &rds.DescribeDBClusterParameterGroupsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBClusterParameterGroups {
			values = append(values, Resource{
				ARN:         *v.DBClusterParameterGroupArn,
				Description: v,
			})
		}
	}

	return values, nil
}

type RDSDBInstanceDescription struct {
	DBInstance types.DBInstance
}

func RDSDBInstance(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBInstancesPaginator(client, &rds.DescribeDBInstancesInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBInstances {
			values = append(values, Resource{
				ARN: *v.DBInstanceArn,
				Description: RDSDBInstanceDescription{
					DBInstance: v,
				},
			})
		}
	}

	return values, nil
}

func RDSDBParameterGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBParameterGroupsPaginator(client, &rds.DescribeDBParameterGroupsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBParameterGroups {
			values = append(values, Resource{
				ARN:         *v.DBParameterGroupArn,
				Description: v,
			})
		}
	}

	return values, nil
}

func RDSDBProxy(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBProxiesPaginator(client, &rds.DescribeDBProxiesInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBProxies {
			values = append(values, Resource{
				ARN:         *v.DBProxyArn,
				Description: v,
			})
		}
	}

	return values, nil
}

func RDSDBProxyEndpoint(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBProxyEndpointsPaginator(client, &rds.DescribeDBProxyEndpointsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBProxyEndpoints {
			values = append(values, Resource{
				ARN:         *v.DBProxyEndpointArn,
				Description: v,
			})
		}
	}

	return values, nil
}

func RDSDBProxyTargetGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	proxies, err := RDSDBProxy(ctx, cfg)
	if err != nil {
		return nil, err
	}

	client := rds.NewFromConfig(cfg)

	var values []Resource
	for _, p := range proxies {
		proxy := p.Description.(types.DBProxy)
		paginator := rds.NewDescribeDBProxyTargetGroupsPaginator(client, &rds.DescribeDBProxyTargetGroupsInput{
			DBProxyName: proxy.DBProxyName,
		})

		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, err
			}

			for _, v := range page.TargetGroups {
				values = append(values, Resource{
					ARN:         *v.TargetGroupArn,
					Description: v,
				})
			}
		}
	}

	return values, nil
}

func RDSDBSecurityGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBSecurityGroupsPaginator(client, &rds.DescribeDBSecurityGroupsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBSecurityGroups {
			values = append(values, Resource{
				ARN:         *v.DBSecurityGroupArn,
				Description: v,
			})
		}
	}

	return values, nil
}

func RDSDBSubnetGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBSubnetGroupsPaginator(client, &rds.DescribeDBSubnetGroupsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBSubnetGroups {
			values = append(values, Resource{
				ARN:         *v.DBSubnetGroupArn,
				Description: v,
			})
		}
	}

	return values, nil
}

type RDSDBEventSubscriptionDescription struct {
	EventSubscription types.EventSubscription
}

func RDSDBEventSubscription(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeEventSubscriptionsPaginator(client, &rds.DescribeEventSubscriptionsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.EventSubscriptionsList {
			values = append(values, Resource{
				ARN: *v.EventSubscriptionArn,
				Description: RDSDBEventSubscriptionDescription{
					EventSubscription: v,
				},
			})
		}
	}

	return values, nil
}

func RDSGlobalCluster(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeGlobalClustersPaginator(client, &rds.DescribeGlobalClustersInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.GlobalClusters {
			values = append(values, Resource{
				ARN:         *v.GlobalClusterArn,
				Description: v,
			})
		}
	}

	return values, nil
}

func RDSOptionGroup(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeOptionGroupsPaginator(client, &rds.DescribeOptionGroupsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.OptionGroupsList {
			values = append(values, Resource{
				ARN:         *v.OptionGroupArn,
				Description: v,
			})
		}
	}

	return values, nil
}

type RDSDBSnapshotDescription struct {
	DBSnapshot           types.DBSnapshot
	DBSnapshotAttributes []types.DBSnapshotAttribute
}

func RDSDBSnapshot(ctx context.Context, cfg aws.Config) ([]Resource, error) {
	client := rds.NewFromConfig(cfg)
	paginator := rds.NewDescribeDBSnapshotsPaginator(client, &rds.DescribeDBSnapshotsInput{})

	var values []Resource
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, v := range page.DBSnapshots {
			attrs, err := client.DescribeDBSnapshotAttributes(ctx, &rds.DescribeDBSnapshotAttributesInput{
				DBSnapshotIdentifier: v.DBSnapshotIdentifier,
			})
			if err != nil {
				return nil, err
			}

			values = append(values, Resource{
				ARN: *v.DBSnapshotArn,
				Description: RDSDBSnapshotDescription{
					DBSnapshot:           v,
					DBSnapshotAttributes: attrs.DBSnapshotAttributesResult.DBSnapshotAttributes,
				},
			})
		}
	}

	return values, nil
}
