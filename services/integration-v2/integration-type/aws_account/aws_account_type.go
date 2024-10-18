package aws_account

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/organizations"
	"github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// getCallerAccountID retrieves the AWS Account ID associated with the provided credentials.
func getCallerAccountID(ctx context.Context, cfg aws.Config) (string, error) {
	stsClient := sts.NewFromConfig(cfg)

	output, err := stsClient.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	if err != nil {
		return "", fmt.Errorf("failed to get caller identity: %v", err)
	}

	return aws.ToString(output.Account), nil
}

// getOrganizationMasterAccountID retrieves the master Account ID of the AWS Organization.
func getOrganizationMasterAccountID(ctx context.Context, cfg aws.Config) (string, error) {
	orgClient := organizations.NewFromConfig(cfg)

	output, err := orgClient.DescribeOrganization(ctx, &organizations.DescribeOrganizationInput{})
	if err != nil {
		return "", fmt.Errorf("failed to describe organization: %v", err)
	}

	masterAccountID := aws.ToString(output.Organization.MasterAccountId)
	return masterAccountID, nil
}

// CheckMasterAccount checks if the provided credentials belong to the AWS Organization's master account.
// It executes independently and does not call other check functions.
func CheckMasterAccount(cfg aws.Config) (bool, error) {
	ctx := context.TODO()

	// Get the caller's Account ID
	callerAccountID, err := getCallerAccountID(ctx, cfg)
	if err != nil {
		return false, err
	}

	// Describe the organization to get the master Account ID
	masterAccountID, err := getOrganizationMasterAccountID(ctx, cfg)
	if err != nil {
		// If the call fails with AWSOrganizationsNotInUseException, the account is not part of an organization
		var orgNotInUseErr *types.AWSOrganizationsNotInUseException
		if errors.As(err, &orgNotInUseErr) {
			// Account is not part of any organization
			return false, nil
		}
		// For other errors, return the error
		return false, fmt.Errorf("error describing organization: %v", err)
	}

	// Compare the two Account IDs
	return callerAccountID == masterAccountID, nil
}

// CheckOrganizationMemberAccount checks if the account is a member (non-master) of an AWS Organization.
// It executes independently and does not call other check functions.
func CheckOrganizationMemberAccount(cfg aws.Config) (bool, error) {
	ctx := context.TODO()

	orgClient := organizations.NewFromConfig(cfg)

	// Attempt to describe the organization
	output, err := orgClient.DescribeOrganization(ctx, &organizations.DescribeOrganizationInput{})
	if err != nil {
		var orgNotInUseErr *types.AWSOrganizationsNotInUseException
		if errors.As(err, &orgNotInUseErr) {
			// Account is not part of any organization
			return false, nil
		}
		// For other errors, return the error
		return false, fmt.Errorf("error describing organization: %v", err)
	}

	// Get the caller's Account ID
	callerAccountID, err := getCallerAccountID(ctx, cfg)
	if err != nil {
		return false, err
	}

	// Compare the caller's Account ID with the MasterAccountId
	// If they are not the same, the account is a member (not master)
	if callerAccountID != aws.ToString(output.Organization.MasterAccountId) {
		return true, nil
	}

	// If the caller Account ID is the same as MasterAccountId, it's the master, not a member.
	return false, nil
}

// CheckStandaloneNonOrganizationAccount checks if the account is a standalone account that is not part of any AWS Organization.
// It executes independently and does not call other check functions.
func CheckStandaloneNonOrganizationAccount(cfg aws.Config) (bool, error) {
	ctx := context.TODO()

	orgClient := organizations.NewFromConfig(cfg)

	// Attempt to describe the organization
	_, err := orgClient.DescribeOrganization(ctx, &organizations.DescribeOrganizationInput{})
	if err != nil {
		var orgNotInUseErr *types.AWSOrganizationsNotInUseException
		if errors.As(err, &orgNotInUseErr) {
			// Account is not part of any organization
			return true, nil
		}
		// For other errors, return the error
		return false, fmt.Errorf("error describing organization: %v", err)
	}

	// If no error, the account is part of an organization
	return false, nil
}
