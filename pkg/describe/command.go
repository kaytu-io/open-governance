package describe

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

const (
	DescribeJobsQueueName                = "describe-jobs-queue"
	DescribeResultsQueueName             = "describe-results-queue"
	DescribeCleanupJobsQueueName         = "describe-cleanup-jobs-queue"
	ComplianceReportJobsQueueName        = "compliance-report-jobs-queue"
	ComplianceReportResultsQueueName     = "compliance-report-results-queue"
	ComplianceReportCleanupJobsQueueName = "compliance-report-cleanup-jobs-queue"
	InsightJobsQueueName                 = "insight-jobs-queue"
	InsightResultsQueueName              = "insight-results-queue"
	CheckupJobsQueueName                 = "checkup-jobs-queue"
	CheckupResultsQueueName              = "checkup-results-queue"
	SummarizerJobsQueueName              = "summarizer-jobs-queue"
	SummarizerResultsQueueName           = "summarizer-results-queue"
	SourceEventsQueueName                = "source-events-queue"
	DescribeConnectionJobsQueueName      = "describe-connection-jobs-queue"
	DescribeConnectionResultsQueueName   = "describe-connection-results-queue"
)

var (
	RabbitMQService  = os.Getenv("RABBITMQ_SERVICE")
	RabbitMQPort     = 5672
	RabbitMQUsername = os.Getenv("RABBITMQ_USERNAME")
	RabbitMQPassword = os.Getenv("RABBITMQ_PASSWORD")

	KafkaService        = os.Getenv("KAFKA_SERVICE")
	KafkaResourcesTopic = os.Getenv("KAFKA_RESOURCE_TOPIC")

	PostgreSQLHost     = os.Getenv("POSTGRESQL_HOST")
	PostgreSQLPort     = os.Getenv("POSTGRESQL_PORT")
	PostgreSQLDb       = os.Getenv("POSTGRESQL_DB")
	PostgreSQLUser     = os.Getenv("POSTGRESQL_USERNAME")
	PostgreSQLPassword = os.Getenv("POSTGRESQL_PASSWORD")
	PostgreSQLSSLMode  = os.Getenv("POSTGRESQL_SSLMODE")

	VaultAddress  = os.Getenv("VAULT_ADDRESS")
	VaultToken    = os.Getenv("VAULT_TOKEN")
	VaultRoleName = os.Getenv("VAULT_ROLE")
	VaultCaPath   = os.Getenv("VAULT_TLS_CA_PATH")
	VaultUseTLS   = strings.ToLower(strings.TrimSpace(os.Getenv("VAULT_USE_TLS"))) == "true"

	ElasticSearchAddress  = os.Getenv("ES_ADDRESS")
	ElasticSearchUsername = os.Getenv("ES_USERNAME")
	ElasticSearchPassword = os.Getenv("ES_PASSWORD")

	HttpServerAddress       = os.Getenv("HTTP_ADDRESS")
	GRPCServerAddress       = os.Getenv("GRPC_ADDRESS")
	DescribeDeliverEndpoint = os.Getenv("DESCRIBE_DELIVER_ENDPOINT")

	PrometheusPushAddress = os.Getenv("PROMETHEUS_PUSH_ADDRESS")

	RedisAddress  = os.Getenv("REDIS_ADDRESS")
	CacheAddress  = os.Getenv("CACHE_ADDRESS")
	JaegerAddress = os.Getenv("JAEGER_ADDRESS")

	DescribeIntervalHours      = os.Getenv("DESCRIBE_INTERVAL_HOURS")
	DescribeTimeoutHours       = os.Getenv("DESCRIBE_TIMEOUT_HOURS")
	ComplianceIntervalHours    = os.Getenv("COMPLIANCE_INTERVAL_HOURS")
	ComplianceTimeoutHours     = os.Getenv("COMPLIANCE_TIMEOUT_HOURS")
	InsightIntervalHours       = os.Getenv("INSIGHT_INTERVAL_HOURS")
	CheckupIntervalHours       = os.Getenv("CHECKUP_INTERVAL_HOURS")
	MustSummarizeIntervalHours = os.Getenv("MUST_SUMMARIZE_INTERVAL_HOURS")
	CurrentWorkspaceID         = os.Getenv("CURRENT_NAMESPACE")
	WorkspaceBaseURL           = os.Getenv("WORKSPACE_BASE_URL")
	MetadataBaseURL            = os.Getenv("METADATA_BASE_URL")
	ComplianceBaseURL          = os.Getenv("COMPLIANCE_BASE_URL")
	OnboardBaseURL             = os.Getenv("ONBOARD_BASE_URL")
	IngressBaseURL             = os.Getenv("BASE_URL")

	CloudNativeAPIBaseURL = os.Getenv("CLOUD_NATIVE_API_BASE_URL")
	CloudNativeAPIAuthKey = os.Getenv("CLOUD_NATIVE_API_AUTH_KEY")

	// For cloud native connection job command
	AccountConcurrentDescribe              = os.Getenv("ACCOUNT_CONCURRENT_DESCRIBE")
	CloudNativeCredentialsJson             = os.Getenv("CLOUDNATIVE_CREDENTIALS")
	CloudNativeOutputQueueName             = os.Getenv("CLOUDNATIVE_WORKER_OUTPUT_QUEUE_NAME")
	CloudNativeOutputConnectionString      = os.Getenv("CLOUDNATIVE_WORKER_OUTPUT_QUEUE_CONNECTION_STRING")
	CloudNativeBlobStorageConnectionString = os.Getenv("CLOUDNATIVE_WORKER_BLOB_STORAGE_CONNECTION_STRING")
	CloudNativeBlobStorageEncryptionKey    = os.Getenv("CLOUDNATIVE_WORKER_BLOB_STORAGE_ENC_KEY")
)

func SchedulerCommand() *cobra.Command {
	var (
		id string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case id == "":
				return errors.New("missing required flag 'id'")
			default:
				return nil
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			s, err := InitializeScheduler(
				id,
				DescribeJobsQueueName,
				DescribeResultsQueueName,
				DescribeConnectionResultsQueueName,
				CloudNativeAPIBaseURL,
				CloudNativeAPIAuthKey,
				DescribeCleanupJobsQueueName,
				ComplianceReportJobsQueueName,
				ComplianceReportResultsQueueName,
				ComplianceReportCleanupJobsQueueName,
				InsightJobsQueueName,
				InsightResultsQueueName,
				CheckupJobsQueueName,
				CheckupResultsQueueName,
				SummarizerJobsQueueName,
				SummarizerResultsQueueName,
				SourceEventsQueueName,
				PostgreSQLUser,
				PostgreSQLPassword,
				PostgreSQLHost,
				PostgreSQLPort,
				PostgreSQLDb,
				PostgreSQLSSLMode,
				VaultAddress,
				VaultRoleName,
				VaultToken,
				VaultCaPath,
				VaultUseTLS,
				HttpServerAddress,
				DescribeIntervalHours,
				DescribeTimeoutHours,
				ComplianceIntervalHours,
				ComplianceTimeoutHours,
				InsightIntervalHours,
				CheckupIntervalHours,
				MustSummarizeIntervalHours,
			)
			if err != nil {
				return err
			}

			defer s.Stop()

			return s.Run()
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "The scheduler id")

	return cmd
}

func WorkerCommand() *cobra.Command {
	var (
		id             string
		resourcesTopic string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case id == "":
				return errors.New("missing required flag 'id'")
			case resourcesTopic == "":
				return errors.New("missing required flag 'resources-topic'")
			default:
				return nil
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true

			w, err := InitializeWorker(
				id,
				RabbitMQUsername,
				RabbitMQPassword,
				RabbitMQService,
				RabbitMQPort,
				DescribeJobsQueueName,
				DescribeResultsQueueName,
				strings.Split(KafkaService, ","),
				resourcesTopic,
				VaultAddress,
				VaultRoleName,
				VaultToken,
				VaultCaPath,
				VaultUseTLS,
				logger,
				PrometheusPushAddress,
				RedisAddress,
				JaegerAddress,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run(context.Background())
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "The worker id")
	cmd.Flags().StringVarP(&resourcesTopic, "resources-topic", "t", "", "The kafka topic where the resources are published.")

	return cmd
}

func CleanupWorkerCommand() *cobra.Command {
	var (
		id string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case id == "":
				return errors.New("missing required flag 'id'")
			default:
				return nil
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true

			w, err := InitializeCleanupWorker(
				id,
				RabbitMQUsername,
				RabbitMQPassword,
				RabbitMQService,
				RabbitMQPort,
				DescribeCleanupJobsQueueName,
				ElasticSearchAddress,
				ElasticSearchUsername,
				ElasticSearchPassword,
				logger,
				PrometheusPushAddress,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run()
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "The worker id")

	return cmd
}

func ConnectionWorkerCommand() *cobra.Command {
	var (
		id             string
		resourcesTopic string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case id == "":
				return errors.New("missing required flag 'id'")
			case resourcesTopic == "":
				return errors.New("missing required flag 'resources-topic'")
			default:
				return nil
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true

			w, err := InitializeConnectionWorker(
				id,
				RabbitMQUsername,
				RabbitMQPassword,
				RabbitMQService,
				RabbitMQPort,
				DescribeConnectionJobsQueueName,
				DescribeConnectionResultsQueueName,
				strings.Split(KafkaService, ","),
				resourcesTopic,
				VaultAddress,
				VaultRoleName,
				VaultToken,
				VaultCaPath,
				VaultUseTLS,
				logger,
				ElasticSearchAddress,
				ElasticSearchUsername,
				ElasticSearchPassword,
				PrometheusPushAddress,
				RedisAddress,
				CacheAddress,
				JaegerAddress,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run(context.Background())
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "The worker id")
	cmd.Flags().StringVarP(&resourcesTopic, "resources-topic", "t", "", "The kafka topic where the resources are published.")

	return cmd
}

func CloudNativeConnectionWorkerCommand() *cobra.Command {
	var (
		instanceId     string
		id             string
		resourcesTopic string
		jobJson        string
		job            DescribeConnectionJob
		secrets        map[string]any
		sendTimeout    bool
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case instanceId == "":
				return errors.New("missing required flag 'instance-id'")
			case id == "":
				return errors.New("missing required flag 'id'")
			case resourcesTopic == "":
				return errors.New("missing required flag 'resources-topic'")
			case jobJson == "":
				return errors.New("missing required flag 'job-json'")
			}
			err := json.Unmarshal([]byte(jobJson), &job)
			if err != nil {
				return errors.New("invalid json for job")
			}
			err = json.Unmarshal([]byte(CloudNativeCredentialsJson), &secrets)
			if err != nil {
				return errors.New("invalid json for secrets")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}
			cmd.SilenceUsage = true
			w, err := InitializeCloudNativeConnectionWorker(
				instanceId,
				id,
				job,
				resourcesTopic,
				CloudNativeOutputQueueName,
				CloudNativeOutputConnectionString,
				CloudNativeBlobStorageConnectionString,
				CloudNativeBlobStorageEncryptionKey,
				secrets,
				logger,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run(context.Background(), sendTimeout)
		},
	}

	cmd.Flags().StringVar(&instanceId, "instance-id", "", "The instance id")
	cmd.Flags().StringVar(&id, "id", "", "The worker id")
	cmd.Flags().StringVarP(&resourcesTopic, "resources-topic", "t", "", "The kafka topic where the resources are published.")
	cmd.Flags().StringVarP(&jobJson, "job-json", "j", "", "The job json.")
	cmd.Flags().BoolVar(&sendTimeout, "send-timeout", false, "If true the worker will only send a timeout message to the output queue and exit.")

	return cmd
}

func LambdaDescribeWorkerCommand() *cobra.Command {
	var (
		workspaceId      string
		connectionId     string
		resourceType     string
		describeEndpoint string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case workspaceId == "":
				return errors.New("missing required flag 'workspace-id'")
			case connectionId == "":
				return errors.New("missing required flag 'connection-id'")
			case resourceType == "":
				return errors.New("missing required flag 'resource-type'")
			case describeEndpoint == "":
				return errors.New("missing required flag 'describe-endpoint'")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}
			cmd.SilenceUsage = true
			w, err := InitializeLambdaDescribeWorker(
				workspaceId,
				connectionId,
				resourceType,
				describeEndpoint,
				logger,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run(context.Background())
		},
	}

	cmd.Flags().StringVar(&workspaceId, "workspace-id", "w", "The workspace id")
	cmd.Flags().StringVar(&connectionId, "connection-id", "c", "The connection id")
	cmd.Flags().StringVarP(&resourceType, "resource-type", "t", "", "resource type")
	cmd.Flags().StringVarP(&describeEndpoint, "describe-endpoint", "d", "", "describe grpc endpoint")

	return cmd
}

func OldCleanerWorkerCommand() *cobra.Command {
	var (
		lowerThan string
	)
	cmd := &cobra.Command{
		PreRunE: func(cmd *cobra.Command, args []string) error {
			switch {
			case lowerThan == "":
				return errors.New("missing required flag 'id'")
			default:
				return nil
			}
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}

			cmd.SilenceUsage = true

			lowerThanInt, err := strconv.Atoi(lowerThan)
			if err != nil {
				return err
			}

			w, err := InitializeOldCleanerWorker(
				uint(lowerThanInt),
				ElasticSearchAddress,
				ElasticSearchUsername,
				ElasticSearchPassword,
				logger,
			)
			if err != nil {
				return err
			}

			return w.Run()
		},
	}

	cmd.Flags().StringVar(&lowerThan, "lower-than", "", "The clean resource job ids lower than this")

	return cmd
}
