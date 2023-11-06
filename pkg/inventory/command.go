package inventory

import (
	"context"
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/config"
	"os"
	"strconv"

	"github.com/kaytu-io/kaytu-engine/pkg/internal/httpserver"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
	RedisAddress = os.Getenv("REDIS_ADDRESS")

	ElasticSearchAddress         = os.Getenv("ES_ADDRESS")
	ElasticSearchUsername        = os.Getenv("ES_USERNAME")
	ElasticSearchPassword        = os.Getenv("ES_PASSWORD")
	ElasticSearchIsOpenSearchStr = os.Getenv("ES_ISOPENSEARCH")
	ElasticSearchAwsRegion       = os.Getenv("ES_AWS_REGION")

	PostgreSQLHost     = os.Getenv("POSTGRESQL_HOST")
	PostgreSQLPort     = os.Getenv("POSTGRESQL_PORT")
	PostgreSQLDb       = os.Getenv("POSTGRESQL_DB")
	PostgreSQLUser     = os.Getenv("POSTGRESQL_USERNAME")
	PostgreSQLPassword = os.Getenv("POSTGRESQL_PASSWORD")
	PostgreSQLSSLMode  = os.Getenv("POSTGRESQL_SSLMODE")

	SteampipeHost     = os.Getenv("STEAMPIPE_HOST")
	SteampipePort     = os.Getenv("STEAMPIPE_PORT")
	SteampipeDb       = os.Getenv("STEAMPIPE_DB")
	SteampipeUser     = os.Getenv("STEAMPIPE_USERNAME")
	SteampipePassword = os.Getenv("STEAMPIPE_PASSWORD")

	KafkaService = os.Getenv("KAFKA_SERVICE")

	SchedulerBaseUrl  = os.Getenv("SCHEDULER_BASE_URL")
	OnboardBaseUrl    = os.Getenv("ONBOARD_BASE_URL")
	ComplianceBaseUrl = os.Getenv("COMPLIANCE_BASE_URL")

	HttpAddress = os.Getenv("HTTP_ADDRESS")
)

func Command() *cobra.Command {
	return &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			return start(cmd.Context())
		},
	}
}

func start(ctx context.Context) error {
	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("new logger: %w", err)
	}

	elasticSearchIsOpenSearch, _ := strconv.ParseBool(ElasticSearchIsOpenSearchStr)
	esConf := config.ElasticSearch{
		Address:      ElasticSearchAddress,
		Username:     ElasticSearchUsername,
		Password:     ElasticSearchPassword,
		IsOpenSearch: elasticSearchIsOpenSearch,
		AwsRegion:    ElasticSearchAwsRegion,
	}

	handler, err := InitializeHttpHandler(
		esConf,
		PostgreSQLHost, PostgreSQLPort, PostgreSQLDb, PostgreSQLUser, PostgreSQLPassword, PostgreSQLSSLMode,
		SteampipeHost, SteampipePort, SteampipeDb, SteampipeUser, SteampipePassword,
		KafkaService,
		SchedulerBaseUrl, OnboardBaseUrl, ComplianceBaseUrl,
		logger,
		RedisAddress,
	)
	if err != nil {
		return fmt.Errorf("init http handler: %w", err)
	}

	return httpserver.RegisterAndStart(logger, HttpAddress, handler)
}
