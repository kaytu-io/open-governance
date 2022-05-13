package workspace

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/spf13/cobra"
)

var (
	PostgresHost     = os.Getenv("POSTGRES_HOST")
	PostgresPort     = os.Getenv("POSTGRES_PORT")
	PostgresDBName   = os.Getenv("POSTGRES_DB")
	PostgresUser     = os.Getenv("POSTGRES_USERNAME")
	PostgresPassword = os.Getenv("POSTGRES_PASSWORD")
	ServerAddr       = os.Getenv("SERVER_ADDR")
	DomainSuffix     = os.Getenv("DOMAIN_SUFFIX")
)

type Config struct {
	Host         string
	Port         string
	User         string
	Password     string
	DBName       string
	ServerAddr   string
	DomainSuffix string
}

func NewConfig() *Config {
	return &Config{
		Host:         PostgresHost,
		Port:         PostgresPort,
		User:         PostgresUser,
		Password:     PostgresPassword,
		DBName:       PostgresDBName,
		ServerAddr:   ServerAddr,
		DomainSuffix: DomainSuffix,
	}
}

func Command() *cobra.Command {
	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx, cancel := signal.NotifyContext(cmd.Context(), syscall.SIGINT, syscall.SIGTERM)
			defer cancel()

			// init the server configuration
			settings := NewConfig()

			s := NewServer(settings)
			// start the http server
			s.Start(ctx)

			// block until received the terminate signal
			<-ctx.Done()

			// stop the http server
			return s.Stop()
		},
	}
	return cmd
}
