// Package config reads hm's environment. All knobs are env vars with fleet
// defaults; hm carries no config file — an observer with a config file is an
// observer whose deployment can drift from its documentation.
package config

import (
	"os"
	"strings"
	"time"
)

// Config is everything hm needs to come up.
type Config struct {
	// KafkaBrokers is the seed broker list (HM_KAFKA_BROKERS, comma-split).
	// Empty means hm comes up degraded: a control port with no eyes, saying so.
	KafkaBrokers []string
	// SchemaRegistryURL locates the shared registry (HM_SCHEMA_REGISTRY_URL).
	SchemaRegistryURL string
	// HTTPAddr is the control port (HM_HTTP_ADDR).
	HTTPAddr string
	// HeartbeatInterval is the frood heartbeat cadence (HM_HEARTBEAT_INTERVAL,
	// Go duration syntax).
	HeartbeatInterval time.Duration
}

// FromEnv loads Config with defaults matching the fleet's k3d layout.
func FromEnv() Config {
	cfg := Config{
		SchemaRegistryURL: envOr("HM_SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		HTTPAddr:          envOr("HM_HTTP_ADDR", ":8090"),
		HeartbeatInterval: 15 * time.Second,
	}
	if v := os.Getenv("HM_KAFKA_BROKERS"); v != "" {
		for _, b := range strings.Split(v, ",") {
			if b = strings.TrimSpace(b); b != "" {
				cfg.KafkaBrokers = append(cfg.KafkaBrokers, b)
			}
		}
	}
	if v := os.Getenv("HM_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.HeartbeatInterval = d
		}
	}
	return cfg
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
