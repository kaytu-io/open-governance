package onboard

import (
	"context"
	"fmt"
	describeClient "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	metadataClient "github.com/kaytu-io/kaytu-engine/pkg/metadata/client"
	"github.com/kaytu-io/kaytu-engine/pkg/onboard/db"
	"github.com/kaytu-io/kaytu-util/pkg/postgres"
	"github.com/kaytu-io/kaytu-util/pkg/queue"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"
	"github.com/kaytu-io/kaytu-util/pkg/vault"

	"go.uber.org/zap"
	"gopkg.in/go-playground/validator.v9"

	inventory "github.com/kaytu-io/kaytu-engine/pkg/inventory/client"
)

type HttpHandler struct {
	db                               db.Database
	steampipeConn                    *steampipe.Database
	sourceEventsQueue                queue.Interface
	kms                              *vault.KMSVaultSourceConfig
	awsPermissionCheckURL            string
	inventoryClient                  inventory.InventoryServiceClient
	describeClient                   describeClient.SchedulerServiceClient
	metadataClient                   metadataClient.MetadataServiceClient
	validator                        *validator.Validate
	keyARN                           string
	logger                           *zap.Logger
	masterAccessKey, masterSecretKey string
}

func InitializeHttpHandler(
	rabbitMQUsername string, rabbitMQPassword string, rabbitMQHost string, rabbitMQPort int,
	sourceEventsQueueName string,
	postgresUsername string, postgresPassword string, postgresHost string, postgresPort string, postgresDb string, postgresSSLMode string,
	steampipeHost string, steampipePort string, steampipeDb string, steampipeUsername string, steampipePassword string,
	logger *zap.Logger,
	awsPermissionCheckURL string,
	keyARN string,
	inventoryBaseURL string,
	describeBaseURL string,
	masterAccessKey, masterSecretKey string,
) (*HttpHandler, error) {

	logger.Info("Initializing http handler")

	// setup source events queue
	qCfg := queue.Config{}
	qCfg.Server.Username = rabbitMQUsername
	qCfg.Server.Password = rabbitMQPassword
	qCfg.Server.Host = rabbitMQHost
	qCfg.Server.Port = rabbitMQPort
	qCfg.Queue.Name = sourceEventsQueueName
	qCfg.Queue.Durable = true
	qCfg.Producer.ID = "onboard-service"
	sourceEventsQueue, err := queue.New(qCfg)
	if err != nil {
		return nil, err
	}

	logger.Info("Connected to the source queue", zap.String("name", sourceEventsQueueName))

	cfg := postgres.Config{
		Host:    postgresHost,
		Port:    postgresPort,
		User:    postgresUsername,
		Passwd:  postgresPassword,
		DB:      postgresDb,
		SSLMode: postgresSSLMode,
	}
	orm, err := postgres.NewClient(&cfg, logger)
	if err != nil {
		return nil, fmt.Errorf("new postgres client: %w", err)
	}
	logger.Info("Connected to the postgres database", zap.String("database", postgresDb))

	steampipeConn, err := steampipe.NewSteampipeDatabase(steampipe.Option{
		Host: steampipeHost,
		Port: steampipePort,
		User: steampipeUsername,
		Pass: steampipePassword,
		Db:   steampipeDb,
	})
	if err != nil {
		return nil, fmt.Errorf("new steampipe client: %w", err)
	}
	logger.Info("Connected to the steampipe database", zap.String("database", steampipeDb))

	kms, err := vault.NewKMSVaultSourceConfig(context.Background(), "", "", KMSAccountRegion)
	if err != nil {
		return nil, err
	}

	onboardDB := db.NewDatabase(orm)
	err = onboardDB.Initialize()
	if err != nil {
		return nil, err
	}
	logger.Info("Initialized postgres database: ", zap.String("database", postgresDb))

	inventoryClient := inventory.NewInventoryServiceClient(inventoryBaseURL)
	describeCli := describeClient.NewSchedulerServiceClient(describeBaseURL)

	meta := metadataClient.NewMetadataServiceClient(MetadataBaseUrl)

	return &HttpHandler{
		db:                    onboardDB,
		steampipeConn:         steampipeConn,
		sourceEventsQueue:     sourceEventsQueue,
		kms:                   kms,
		awsPermissionCheckURL: awsPermissionCheckURL,
		inventoryClient:       inventoryClient,
		describeClient:        describeCli,
		validator:             validator.New(),
		keyARN:                keyARN,
		logger:                logger,
		masterAccessKey:       masterAccessKey,
		masterSecretKey:       masterSecretKey,
		metadataClient:        meta,
	}, nil
}
