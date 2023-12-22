package elasticsearch

import (
	"context"
	"github.com/kaytu-io/kaytu-engine/services/migrator/config"
	"github.com/kaytu-io/kaytu-util/pkg/kaytu-es-sdk"
	"go.uber.org/zap"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Migration struct {
}

func (m Migration) IsGitBased() bool {
	return false
}

func (m Migration) AttachmentFolderPath() string {
	return "/elasticsearch-index-config"
}

func (m Migration) Run(conf config.MigratorConfig, logger *zap.Logger) error {
	logger.Info("running", zap.String("es_address", conf.ElasticSearch.Address))

	var externalID *string
	if len(conf.ElasticSearch.ExternalID) > 0 {
		externalID = &conf.ElasticSearch.ExternalID
	}
	elastic, err := kaytu.NewClient(kaytu.ClientConfig{
		Addresses:     []string{conf.ElasticSearch.Address},
		Username:      &conf.ElasticSearch.Username,
		Password:      &conf.ElasticSearch.Password,
		IsOpenSearch:  &conf.ElasticSearch.IsOpenSearch,
		AwsRegion:     &conf.ElasticSearch.AwsRegion,
		AssumeRoleArn: &conf.ElasticSearch.AssumeRoleArn,
		ExternalID:    externalID,
	})
	if err != nil {
		logger.Error("failed to create es client due to", zap.Error(err))
		return err
	}

	for {
		err := elastic.Healthcheck(context.TODO())
		if err != nil {
			if err.Error() == "unhealthy" {
				logger.Warn("Waiting for status to be GREEN or YELLOW. Sleeping for 10 seconds...")
				time.Sleep(10 * time.Second)
				continue
			}
			logger.Error("failed to check es healthcheck due to", zap.Error(err))
			return err
		}
		break
	}
	logger.Warn("Starting es migration")

	var files []string
	err = filepath.Walk(m.AttachmentFolderPath(), func(path string, info fs.FileInfo, err error) error {
		if strings.HasSuffix(info.Name(), ".json") {
			files = append(files, path)
		}
		return nil
	})
	if err != nil {
		logger.Error("failed to get files", zap.Error(err))
		return err
	}

	var finalErr error
	for _, fp := range files {
		if strings.Contains(fp, "_component_template") {
			err = CreateTemplate(elastic, logger, fp)
			if err != nil {
				finalErr = err
				logger.Error("failed to create component template", zap.Error(err), zap.String("filepath", fp))
			}
		}
	}

	for _, fp := range files {
		if !strings.Contains(fp, "_component_template") {
			err = CreateTemplate(elastic, logger, fp)
			if err != nil {
				finalErr = err
				logger.Error("failed to create template", zap.Error(err), zap.String("filepath", fp))
			}
		}
	}

	return finalErr
}

func CreateTemplate(es kaytu.Client, logger *zap.Logger, fp string) error {
	fn := filepath.Base(fp)
	idx := strings.LastIndex(fn, ".")
	fne := fn[:idx]

	f, err := os.ReadFile(fp)
	if err != nil {
		return err
	}

	if strings.HasSuffix(fne, "_component_template") {
		err = es.CreateComponentTemplate(context.TODO(), fne, string(f))
		if err != nil {
			logger.Error("failed to create component template", zap.Error(err), zap.String("filepath", fp))
			return err
		}
	} else {
		err = es.CreateIndexTemplate(context.TODO(), fne, string(f))
		if err != nil {
			logger.Error("failed to create index template", zap.Error(err), zap.String("filepath", fp))
			return err
		}
	}

	return nil
}
