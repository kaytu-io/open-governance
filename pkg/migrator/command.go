package migrator

import (
	"github.com/spf13/cobra"
	"gitlab.com/keibiengine/keibi-engine/pkg/config"
	"go.uber.org/zap"
)

type JobConfig struct {
	PostgreSQL            config.Postgres
	QueryGitURL           string `yaml:"query_git_url"`
	AWSComplianceGitURL   string `yaml:"aws_compliance_git_url"`
	AzureComplianceGitURL string `yaml:"azure_compliance_git_url"`
	PrometheusPushAddress string `yaml:"prometheus_push_address"`
}

func JobCommand() *cobra.Command {
	var (
		cnf JobConfig
	)
	config.ReadFromEnv(&cnf, nil)

	cmd := &cobra.Command{
		RunE: func(cmd *cobra.Command, args []string) error {
			cmd.SilenceUsage = true
			logger, err := zap.NewProduction()
			if err != nil {
				return err
			}

			w, err := InitializeJob(
				cnf,
				logger,
				cnf.PrometheusPushAddress,
			)
			if err != nil {
				return err
			}

			defer w.Stop()

			return w.Run()
		},
	}

	return cmd
}
