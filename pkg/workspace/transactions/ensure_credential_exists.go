package transactions

import (
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/api"
	"github.com/kaytu-io/kaytu-engine/pkg/workspace/db"
)

type EnsureCredentialExists struct {
	db *db.Database
}

func NewEnsureCredentialExists(
	db *db.Database,
) *EnsureCredentialExists {
	return &EnsureCredentialExists{
		db: db,
	}
}

func (t *EnsureCredentialExists) Requirements() []api.TransactionID {
	return nil
}

func (t *EnsureCredentialExists) Apply(workspace db.Workspace) error {
	creds, err := t.db.ListCredentialsByWorkspaceID(workspace.ID)
	if err != nil {
		return err
	}

	if len(creds) == 0 {
		return ErrTransactionNeedsTime
	}

	return nil
}

func (t *EnsureCredentialExists) Rollback(workspace db.Workspace) error {
	return nil
}
