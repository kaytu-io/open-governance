package reporter

import (
	"fmt"
	config2 "github.com/kaytu-io/kaytu-util/pkg/config"
	"github.com/kaytu-io/kaytu-util/pkg/httpserver"
	kaytuTrace "github.com/kaytu-io/kaytu-util/pkg/trace"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"golang.org/x/net/context"
	"os"
)

var HttpAddress = os.Getenv("HTTP_ADDRESS")

var (
	ReporterQueueName = "reporter-jobs-queue"

	SteampipeHost     = os.Getenv("STEAMPIPE_HOST")
	SteampipePort     = os.Getenv("STEAMPIPE_PORT")
	SteampipeDb       = os.Getenv("STEAMPIPE_DB")
	SteampipeUser     = os.Getenv("STEAMPIPE_USERNAME")
	SteampipePassword = os.Getenv("STEAMPIPE_PASSWORD")

	PostgreSQLHost     = os.Getenv("POSTGRESQL_HOST")
	PostgreSQLPort     = os.Getenv("POSTGRESQL_PORT")
	PostgreSQLDb       = os.Getenv("POSTGRESQL_DB")
	PostgreSQLUser     = os.Getenv("POSTGRESQL_USERNAME")
	PostgreSQLPassword = os.Getenv("POSTGRESQL_PASSWORD")
	PostgreSQLSSLMode  = os.Getenv("POSTGRESQL_SSLMODE")

	PrometheusPushAddress = os.Getenv("PROMETHEUS_PUSH_ADDRESS")

	OnboardBaseURL = os.Getenv("ONBOARD_BASEURL")

	JaegerAgentHost = os.Getenv("JAEGER_AGENT_HOST")
)

func ReporterCommand() *cobra.Command {
	var (
		id   string
		mode string
	)

	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			logger, _ := zap.NewProduction()
			switch mode {
			case "worker":
				tp, err := kaytuTrace.InitTracer(JaegerAgentHost)
				if err != nil {
					return err
				}
				defer func() {
					if err := tp.Shutdown(ctx); err != nil {
						logger.Error(err.Error())
					}
				}()
				worker, err := InitializeWorker(id,
					ReporterQueueName,
					logger,
					PrometheusPushAddress,
					PostgreSQLHost, PostgreSQLPort, PostgreSQLDb, PostgreSQLUser, PostgreSQLPassword, PostgreSQLSSLMode,
					SteampipeHost, SteampipePort, SteampipeDb, SteampipeUser, SteampipePassword,
					OnboardBaseURL)
				if err != nil {
					logger.Error("initialize worker", zap.Error(err))
					return err
				}
				defer worker.Stop()
				return worker.Run(ctx)
			default:
				config := ServiceConfig{}
				config2.ReadFromEnv(&config, nil)
				j, err := New(config, logger)
				if err != nil {
					panic(err)
				}

				EnsureRunGoroutin(func() {
					j.Run(cmd.Context())
				})
				return startHttpServer(cmd.Context(), j)
			}
		},
	}

	cmd.Flags().StringVar(&id, "id", "", "The worker id")
	cmd.Flags().StringVar(&mode, "mode", "", "The binary mode")

	return cmd
}

func startHttpServer(ctx context.Context, j *Service) error {

	logger, err := zap.NewProduction()
	if err != nil {
		return fmt.Errorf("new logger: %w", err)
	}

	httpServer := NewHTTPServer(HttpAddress, logger, j)
	if err != nil {
		return fmt.Errorf("init http handler: %w", err)
	}

	return httpserver.RegisterAndStart(ctx, logger, HttpAddress, httpServer)
}
