package transactions

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/api"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/db"
	"strings"
)

var serviceNames = []string{
	"alerting",
	"analytics-worker",
	"checkup-worker",
	"compliance",
	"compliance-report-worker",
	"compliance-summarizer",
	"cost-estimator",
	"insight-worker",
	"inventory",
	"metadata",
	"migrator",
	"onboard",
	"reporter",
	"scheduler",
	"steampipe",
}

var rolePolicies = map[string][]string{
	"scheduler": {"arn:aws:iam::${accountID}:policy/lambda-invoke-policy",
		"arn:aws:iam::${accountID}:policy/kaytu-ingestion-${workspaceID}"},
	"analytics-worker":         {"arn:aws:iam::${accountID}:policy/kaytu-ingestion-${workspaceID}"},
	"compliance-report-worker": {"arn:aws:iam::${accountID}:policy/kaytu-ingestion-${workspaceID}"},
	"compliance-summarizer":    {"arn:aws:iam::${accountID}:policy/kaytu-ingestion-${workspaceID}"},
	"insight-worker":           {"arn:aws:iam::${accountID}:policy/kaytu-ingestion-${workspaceID}"},
}

type CreateServiceAccountRoles struct {
	iam               *iam.Client
	kaytuAWSAccountID string
	kaytuOIDCProvider string
}

func NewCreateServiceAccountRoles(
	iam *iam.Client,
	kaytuAWSAccountID string,
	kaytuOIDCProvider string,
) *CreateServiceAccountRoles {
	return &CreateServiceAccountRoles{
		iam:               iam,
		kaytuAWSAccountID: kaytuAWSAccountID,
		kaytuOIDCProvider: kaytuOIDCProvider,
	}
}

func (t *CreateServiceAccountRoles) Requirements() []api.TransactionID {
	return nil
}

func (t *CreateServiceAccountRoles) ApplyIdempotent(ctx context.Context, workspace db.Workspace) error {
	for _, serviceName := range serviceNames {
		if err := t.createRole(ctx, workspace, serviceName); err != nil {
			return err
		}
	}
	return nil
}

func (t *CreateServiceAccountRoles) RollbackIdempotent(ctx context.Context, workspace db.Workspace) error {
	for _, serviceName := range serviceNames {
		roleName := aws.String(fmt.Sprintf("kaytu-service-%s-%s", workspace.ID, serviceName))

		out, err := t.iam.ListAttachedRolePolicies(ctx, &iam.ListAttachedRolePoliciesInput{
			RoleName: roleName,
		})
		if err == nil && out != nil {
			for _, attachedPolicy := range out.AttachedPolicies {
				_, err := t.iam.DetachRolePolicy(ctx, &iam.DetachRolePolicyInput{
					PolicyArn: attachedPolicy.PolicyArn,
					RoleName:  roleName,
				})
				if err != nil {
					return err
				}
			}
		}

		_, err = t.iam.DeleteRole(ctx, &iam.DeleteRoleInput{
			RoleName: roleName,
		})
		if err != nil {
			if !strings.Contains(err.Error(), "NoSuchEntity") {
				return err
			}
		}
	}
	return nil
}

func (t *CreateServiceAccountRoles) createRole(ctx context.Context, workspace db.Workspace, serviceName string) error {
	roleName := aws.String(fmt.Sprintf("kaytu-service-%s-%s", workspace.ID, serviceName))
	_, err := t.iam.CreateRole(ctx, &iam.CreateRoleInput{
		AssumeRolePolicyDocument: aws.String(fmt.Sprintf(`{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Principal": {
                "Federated": "arn:aws:iam::%[1]s:oidc-provider/%[2]s"
            },
            "Action": "sts:AssumeRoleWithWebIdentity",
            "Condition": {
                "StringLike": {
                    "%[2]s:sub": "system:serviceaccount:%[3]s:%[4]s",
                    "%[2]s:aud": "sts.amazonaws.com"
                }
            }
        }
    ]
}`, t.kaytuAWSAccountID, t.kaytuOIDCProvider, workspace.ID, serviceName)),
		RoleName: roleName,
	})
	if err != nil {
		if !strings.Contains(err.Error(), "EntityAlreadyExists") {
			return err
		}
	}

	_, err = t.iam.CreatePolicy(ctx, &iam.CreatePolicyInput{
		PolicyName: aws.String(fmt.Sprintf("kaytu-ingestion-%s", workspace.ID)),
		PolicyDocument: aws.String(`{
    "Statement": [
        {
            "Action": "osis:Ingest",
            "Effect": "Allow",
            "Resource": "*"
        }
    ],
    "Version": "2012-10-17"
}`),
	})
	if err != nil {
		if !strings.Contains(err.Error(), "EntityAlreadyExists") {
			return err
		}
	}

	if v, ok := rolePolicies[serviceName]; ok && len(v) > 0 {
		for _, policyARN := range v {
			policyARN = strings.ReplaceAll(policyARN, "${accountID}", t.kaytuAWSAccountID)
			policyARN = strings.ReplaceAll(policyARN, "${workspaceID}", workspace.ID)

			_, err = t.iam.AttachRolePolicy(ctx, &iam.AttachRolePolicyInput{
				PolicyArn: aws.String(policyARN),
				RoleName:  roleName,
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}
