package config

import (
	"testing"
	"time"
)

func TestFromEnv(t *testing.T) {
	t.Setenv("HM_KAFKA_BROKERS", "kafka:9092, kafka2:9092 ,")
	t.Setenv("HM_HTTP_ADDR", ":9999")
	t.Setenv("HM_INTROSPECT_TICK", "30s")
	t.Setenv("HM_HEARTBEAT_INTERVAL", "bogus") // bad duration keeps the default

	cfg := FromEnv()
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[1] != "kafka2:9092" {
		t.Fatalf("brokers parsed wrong: %v", cfg.KafkaBrokers)
	}
	if cfg.HTTPAddr != ":9999" {
		t.Fatalf("addr = %q", cfg.HTTPAddr)
	}
	if cfg.IntrospectTick != 30*time.Second {
		t.Fatalf("tick = %v", cfg.IntrospectTick)
	}
	if cfg.HeartbeatInterval != 15*time.Second {
		t.Fatalf("bad duration did not keep default: %v", cfg.HeartbeatInterval)
	}
}
