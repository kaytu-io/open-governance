package config

import "github.com/kaytu-io/kaytu-util/pkg/koanf"

type WastageConfig struct {
	Http      koanf.HttpServer   `json:"http,omitempty" koanf:"http"`
	Pennywise koanf.KaytuService `json:"pennywise" koanf:"pennywise"`
}
