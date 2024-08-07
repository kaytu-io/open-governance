package state

import (
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/api"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/db"
)

type Reserved struct {
}

func (s Reserved) Requirements(workspace db.Workspace) []api.TransactionID {
	return []api.TransactionID{
		api.Transaction_CreateWorkspaceKeyId,
		api.Transaction_CreateMasterCredential,
		api.Transaction_CreateServiceAccountRoles,
		api.Transaction_CreateHelmRelease,
	}
}

func (s Reserved) ProcessingStateID() api.StateID {
	return api.StateID_Reserving
}

func (s Reserved) FinishedStateID() api.StateID {
	return api.StateID_Reserved
}
