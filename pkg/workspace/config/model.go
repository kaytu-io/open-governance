package config

import (
	"github.com/kaytu-io/kaytu-util/pkg/config"
)

type Config struct {
	Postgres   config.Postgres
	Redis      config.Redis
	Http       config.HttpServer
	Auth       config.KaytuService
	Onboard    config.KaytuService
	Scheduler  config.KaytuService
	Compliance config.KaytuService
	Inventory  config.KaytuService

	DomainSuffix               string
	KaytuHelmChartLocation     string
	KaytuOctopusNamespace      string
	FluxSystemNamespace        string
	AutoSuspendDurationMinutes int64
	S3AccessKey                string
	S3SecretKey                string
	KMSAccountRegion           string `yaml:"kms_account_region"`
	KMSKeyARN                  string `yaml:"kms_key_arn"`
	AWSMasterAccessKey         string `yaml:"aws_master_access_key"`
	AWSMasterSecretKey         string `yaml:"aws_master_secret_key"`
	AWSMasterPolicyARN         string `yaml:"aws_master_policy_arn"`

	OpenSearchRegion string `yaml:"open_search_region"`
}
