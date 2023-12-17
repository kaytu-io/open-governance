package api

import (
	describe "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	inventory "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
	"github.com/kaytu-io/kaytu-engine/services/integration/api/healthz"
	"github.com/kaytu-io/kaytu-engine/services/integration/api/source"
	"github.com/kaytu-io/kaytu-engine/services/integration/db"
	"github.com/kaytu-io/kaytu-engine/services/integration/meta"
	"github.com/kaytu-io/kaytu-engine/services/integration/repository"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"github.com/kaytu-io/kaytu-util/pkg/vault"
	"github.com/labstack/echo/v4"
	"go.uber.org/zap"
)

type API struct {
	logger          *zap.Logger
	describe        describe.SchedulerServiceClient
	inventory       inventory.InventoryServiceClient
	meta            *meta.Meta
	steampipe       *steampipe.Database
	database        db.Database
	kms             *vault.KMSVaultSourceConfig
	masterAccessKey string
	masterSecretKey string
	arn             string
}

func New(
	logger *zap.Logger,
	d describe.SchedulerServiceClient,
	i inventory.InventoryServiceClient,
	m *meta.Meta,
	s *steampipe.Database,
	db db.Database,
	kms *vault.KMSVaultSourceConfig,
	arn string,
	masterAccessKey string,
	masterSecretKey string,

) *API {
	return &API{
		logger:          logger.Named("api"),
		describe:        d,
		inventory:       i,
		meta:            m,
		steampipe:       s,
		database:        db,
		kms:             kms,
		arn:             arn,
		masterAccessKey: masterAccessKey,
		masterSecretKey: masterSecretKey,
	}
}

func (api *API) Register(e *echo.Echo) {
	var healthz healthz.Healthz
	source := source.New("", api.kms, repository.NewSource(api.database), api.masterAccessKey, api.masterSecretKey)

	healthz.Register(e.Group("/healthz"))
	source.Register(e.Group("/sources"))
}
