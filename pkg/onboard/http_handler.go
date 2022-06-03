package onboard

import (
	"fmt"

	"github.com/hashicorp/vault/api/auth/kubernetes"

	"gitlab.com/keibiengine/keibi-engine/pkg/internal/queue"
	"gitlab.com/keibiengine/keibi-engine/pkg/internal/vault"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type HttpHandler struct {
	db                Database
	sourceEventsQueue queue.Interface
	vault             vault.SourceConfig
}

func InitializeHttpHandler(
	rabbitMQUsername string,
	rabbitMQPassword string,
	rabbitMQHost string,
	rabbitMQPort int,
	sourceEventsQueueName string,
	postgresUsername string,
	postgresPassword string,
	postgresHost string,
	postgresPort string,
	postgresDb string,
	vaultAddress string,
	vaultToken string,
	vaultRoleName string,
	vaultCaPath string,
	vaultUseTLS bool,
) (*HttpHandler, error) {

	fmt.Println("Initializing http handler")

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

	fmt.Println("Connected to the source queue: ", sourceEventsQueueName)

	// setup postgres connection
	dsn := fmt.Sprintf(`host=%s port=%s user=%s password=%s dbname=%s sslmode=disable TimeZone=GMT`,
		postgresHost,
		postgresPort,
		postgresUsername,
		postgresPassword,
		postgresDb,
	)

	orm, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to the postgres database: ", postgresDb)

	k8sAuth, err := kubernetes.NewKubernetesAuth(
		vaultRoleName,
		kubernetes.WithServiceAccountToken(vaultToken),
	)
	if err != nil {
		return nil, err
	}

	// setup vault
	v, err := vault.NewSourceConfig(vaultAddress, vaultCaPath, k8sAuth, vaultUseTLS)
	if err != nil {
		return nil, err
	}

	fmt.Println("Connected to vault:", vaultAddress)

	db := Database{orm: orm}
	err = db.Initialize()
	if err != nil {
		return nil, err
	}
	fmt.Println("Initialized postgres database: ", postgresDb)

	return &HttpHandler{
		vault:             v,
		db:                db,
		sourceEventsQueue: sourceEventsQueue,
	}, nil
}
