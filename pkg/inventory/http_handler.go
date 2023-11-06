package inventory

import (
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/config"
	"strings"
	"time"

	confluent_kafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"

	awsSteampipe "github.com/kaytu-io/kaytu-aws-describer/pkg/steampipe"
	azureSteampipe "github.com/kaytu-io/kaytu-azure-describer/pkg/steampipe"
	"github.com/kaytu-io/kaytu-util/pkg/postgres"
	"github.com/turbot/steampipe-plugin-sdk/v5/plugin"

	"github.com/go-redis/cache/v8"

	complianceClient "github.com/kaytu-io/kaytu-engine/pkg/compliance/client"
	onboardClient "github.com/kaytu-io/kaytu-engine/pkg/onboard/client"

	"github.com/go-redis/redis/v8"

	describeClient "github.com/kaytu-io/kaytu-engine/pkg/describe/client"
	"github.com/kaytu-io/kaytu-util/pkg/steampipe"

	kaytuAws "github.com/kaytu-io/kaytu-aws-describer/pkg/kaytu-es-sdk"
	kaytuAzure "github.com/kaytu-io/kaytu-azure-describer/pkg/kaytu-es-sdk"
	"github.com/kaytu-io/kaytu-util/pkg/kaytu-es-sdk"
	"go.uber.org/zap"
)

type HttpHandler struct {
	client           kaytu.Client
	awsClient        kaytuAws.Client
	azureClient      kaytuAzure.Client
	db               Database
	steampipeConn    *steampipe.Database
	schedulerClient  describeClient.SchedulerServiceClient
	onboardClient    onboardClient.OnboardServiceClient
	complianceClient complianceClient.ComplianceServiceClient
	rdb              *redis.Client
	cache            *cache.Cache
	kafkaProducer    *confluent_kafka.Producer

	logger *zap.Logger

	awsPlg, azurePlg, azureADPlg *plugin.Plugin
}

func InitializeHttpHandler(
	esConf config.ElasticSearch,
	postgresHost string, postgresPort string, postgresDb string, postgresUsername string, postgresPassword string, postgresSSLMode string,
	steampipeHost string, steampipePort string, steampipeDb string, steampipeUsername string, steampipePassword string,
	KafkaService string,
	schedulerBaseUrl string, onboardBaseUrl string, complianceBaseUrl string,
	logger *zap.Logger,
	redisAddress string,
) (h *HttpHandler, err error) {

	h = &HttpHandler{}

	fmt.Println("Initializing http handler")

	// setup postgres connection
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

	h.db = Database{orm: orm}
	fmt.Println("Connected to the postgres database: ", postgresDb)

	err = h.db.Initialize()
	if err != nil {
		return nil, err
	}
	fmt.Println("Initialized postgres database: ", postgresDb)

	// setup steampipe connection
	steampipeConn, err := steampipe.NewSteampipeDatabase(steampipe.Option{
		Host: steampipeHost,
		Port: steampipePort,
		User: steampipeUsername,
		Pass: steampipePassword,
		Db:   steampipeDb,
	})
	h.steampipeConn = steampipeConn
	if err != nil {
		return nil, err
	}
	fmt.Println("Initialized steampipe database: ", steampipeConn)

	h.client, err = kaytu.NewClient(kaytu.ClientConfig{
		Addresses:    []string{esConf.Address},
		Username:     &esConf.Username,
		Password:     &esConf.Password,
		IsOpenSearch: &esConf.IsOpenSearch,
		AwsRegion:    &esConf.AwsRegion,
	})
	if err != nil {
		return nil, err
	}
	h.awsClient = kaytuAws.Client{
		Client: h.client,
	}
	h.azureClient = kaytuAzure.Client{
		Client: h.client,
	}
	h.schedulerClient = describeClient.NewSchedulerServiceClient(schedulerBaseUrl)

	h.rdb = redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	h.cache = cache.New(&cache.Options{
		Redis:      h.rdb,
		LocalCache: cache.NewTinyLFU(100000, 5*time.Minute),
	})
	h.onboardClient = onboardClient.NewOnboardServiceClient(onboardBaseUrl, h.cache)
	h.complianceClient = complianceClient.NewComplianceClient(complianceBaseUrl)

	h.logger = logger

	h.awsPlg = awsSteampipe.Plugin()
	h.azurePlg = azureSteampipe.Plugin()
	h.azureADPlg = azureSteampipe.ADPlugin()

	kafkaProducer, err := confluent_kafka.NewProducer(&confluent_kafka.ConfigMap{
		"bootstrap.servers":            strings.Join(strings.Split(KafkaService, ","), ","),
		"linger.ms":                    100,
		"compression.type":             "lz4",
		"message.timeout.ms":           10000,
		"queue.buffering.max.messages": 100000,
	})
	if err != nil {
		return nil, err
	}

	h.kafkaProducer = kafkaProducer
	return h, nil
}
