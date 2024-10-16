package transactions

import (
	api2 "github.com/opengovern/og-util/pkg/api"
	"github.com/opengovern/og-util/pkg/httpclient"
	"github.com/opengovern/opengovernance/pkg/describe/api"
	client2 "github.com/opengovern/opengovernance/pkg/describe/client"
	api3 "github.com/opengovern/opengovernance/pkg/workspace/api"
	"github.com/opengovern/opengovernance/pkg/workspace/config"
	"github.com/opengovern/opengovernance/pkg/workspace/db"
	"golang.org/x/net/context"
	"strings"
)

type EnsureDiscoveryFinished struct {
	cfg config.Config
}

func NewEnsureDiscoveryFinished(
	cfg config.Config,
) *EnsureDiscoveryFinished {
	return &EnsureDiscoveryFinished{
		cfg: cfg,
	}
}

func (t *EnsureDiscoveryFinished) Requirements() []api3.TransactionID {
	return []api3.TransactionID{api3.Transaction_EnsureWorkspacePodsRunning}
}

func (t *EnsureDiscoveryFinished) ApplyIdempotent(ctx context.Context, workspace db.Workspace) error {
	hctx := &httpclient.Context{UserRole: api2.InternalRole}
	schedulerURL := strings.ReplaceAll(t.cfg.Scheduler.BaseURL, "%NAMESPACE%", t.cfg.KaytuOctopusNamespace)
	schedulerClient := client2.NewSchedulerServiceClient(schedulerURL)

	status, err := schedulerClient.GetDescribeAllJobsStatus(hctx)
	if err != nil {
		return err
	}

	// waiting for all connections to finish
	if status == nil || *status != api.DescribeAllJobsStatusResourcesPublished {
		return ErrTransactionNeedsTime
	}

	return nil
}

func (t *EnsureDiscoveryFinished) RollbackIdempotent(ctx context.Context, workspace db.Workspace) error {
	return nil
}
