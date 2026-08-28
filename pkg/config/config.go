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
	// FliprURL locates the flag store, BY NAME (HM_FLIPR_URL). Empty disables
	// flag polling entirely -- hm runs fine with no flipr, holding LogLevel.
	FliprURL string
	// LogLevel is the boot default (HM_LOG_LEVEL); the log.level flag in
	// flipr (service scope over _global) overrides it at runtime. The fleet
	// default is warn -- info and debug exist to be flipped on, not lived in.
	LogLevel string
	// IntrospectTick is the broker-introspection cadence (HM_INTROSPECT_TICK,
	// Go duration syntax). Jittered +-20% at runtime.
	IntrospectTick time.Duration
}

// FromEnv loads Config with defaults matching the fleet's k3d layout.
func FromEnv() Config {
	cfg := Config{
		SchemaRegistryURL: envOr("HM_SCHEMA_REGISTRY_URL", "http://localhost:8081"),
		HTTPAddr:          envOr("HM_HTTP_ADDR", ":8090"),
		FliprURL:          envOr("HM_FLIPR_URL", "http://flipr.flipr.svc.cluster.local"),
		LogLevel:          envOr("HM_LOG_LEVEL", "warn"),
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
	cfg.IntrospectTick = time.Minute
	if v := os.Getenv("HM_INTROSPECT_TICK"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 {
			cfg.IntrospectTick = d
		}
	}
	return cfg
}

// envOr returns the env var's value, or fallback when unset/empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
