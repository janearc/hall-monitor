// hm is the mesh's hall monitor: the resident that checks services actually
// do on the wire what they claim to do. This is v0, the passive skeleton: a
// fleet citizen (heartbeat, /health, /metrics, JSON logs) whose eyes — the
// consume-everything loop and broker introspection — arrive in the next
// change. See doc/rfc-hall-monitor.md for the ratified design.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/janearc/big-little-mesh/emit"
	"github.com/janearc/big-little-mesh/frood"
	observabilityproto "github.com/janearc/big-little-mesh/proto/observability/v1"
	"github.com/spf13/cobra"

	"github.com/janearc/hall-monitor/pkg/config"
	"github.com/janearc/hall-monitor/pkg/server"
)

// main parses the command line and runs the daemon.
func main() {
	cmd := &cobra.Command{
		Use:          "hm",
		Short:        "hm -- the mesh's hall monitor (sentinel role)",
		SilenceUsage: true,
		RunE: func(_ *cobra.Command, _ []string) error {
			return run()
		},
	}
	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}

// run is the daemon: logging, config, the control port, the heartbeat, and
// (as the stack above this lands) the watch loops; blocks until signalled.
func run() error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.FromEnv()
	srv := server.New(cfg.HTTPAddr, logger)

	// The publisher is best-effort per fleet convention: a nil publisher is a
	// no-op and hm keeps running — but unlike other froods, hm's whole job is
	// the wire, so no broker is a DEGRADED state and /health says so loudly
	// (fail loud, doctrine: never present uncertain state as truth).
	var pub *emit.Publisher
	if len(cfg.KafkaBrokers) == 0 {
		srv.SetDegraded("no kafka brokers configured (HM_KAFKA_BROKERS empty): hm has no eyes")
		logger.Error("hm is up with no broker configured; health reports degraded")
	} else {
		p, err := emit.New(ctx, cfg.KafkaBrokers, cfg.SchemaRegistryURL)
		if err != nil {
			// the health reason is a stable operator-facing sentence; the raw
			// error carries addresses and library noise and belongs in the log
			srv.SetDegraded("kafka unreachable at startup (see logs)")
			logger.Error("kafka unreachable at startup; health reports degraded", "err", err)
		} else {
			pub = p
			defer pub.Close()
		}
	}

	go frood.Heartbeat(ctx, pub, "hm", observabilityproto.Schema, cfg.HeartbeatInterval, logger)

	logger.Info("hm starting", "brokers", len(cfg.KafkaBrokers), "addr", cfg.HTTPAddr)
	return srv.Serve(ctx)
}
