package config

import (
	"github.com/opengovern/og-util/pkg/koanf"
	"github.com/opengovern/og-util/pkg/vault"
)

type IntegrationConfig struct {
	Postgres koanf.Postgres   `json:"postgres,omitempty" koanf:"postgres"`
	Http     koanf.HttpServer `json:"http,omitempty" koanf:"http"`
	Vault    vault.Config     `json:"vault,omitempty" koanf:"vault"`
}
