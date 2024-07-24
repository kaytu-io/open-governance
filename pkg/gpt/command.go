package gpt

import (
	"context"
	"fmt"
	"github.com/kaytu-io/kaytu-util/pkg/httpserver"
	"os"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
)

var (
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

	handler, err := InitializeHttpHandler(
		logger,
	)
	if err != nil {
		return fmt.Errorf("init http handler: %w", err)
	}

	return httpserver.RegisterAndStart(ctx, logger, HttpAddress, handler)
}
