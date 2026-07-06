// hm is the mesh's hall monitor: the resident that checks services actually
// do on the wire what they claim to do. v0 is the passive half: a fleet
// citizen (heartbeat, /health, /metrics, JSON logs) running the
// consume-everything loop, broker introspection, the absence ledger, and
// the truth report at /truth. See doc/rfc-hall-monitor.md for the design.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/janearc/big-little-mesh/emit"
	"github.com/janearc/big-little-mesh/frood"
	observabilityproto "github.com/janearc/big-little-mesh/proto/observability/v1"
	"github.com/spf13/cobra"

	"github.com/janearc/hall-monitor/pkg/config"
	"github.com/janearc/hall-monitor/pkg/ledger"
	"github.com/janearc/hall-monitor/pkg/report"
	"github.com/janearc/hall-monitor/pkg/server"
	"github.com/janearc/hall-monitor/pkg/watch"
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

		// The watch loops: consume-everything + the introspection tick. A
		// watcher that cannot connect is the same degraded state as no broker.
		w, err := watch.New(ctx, cfg.KafkaBrokers, logger)
		if err != nil {
			srv.SetDegraded("watcher could not connect to kafka (see logs)")
			logger.Error("watcher could not connect; health reports degraded", "err", err)
		} else {
			// shutdown leaves the consumer group cleanly so the broker
			// rebalances immediately; fresh context because the daemon ctx
			// is already cancelled by the time this defer runs
			defer func() {
				leaveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				w.Close(leaveCtx)
			}()
			// the absence ledger rides the record seam; the truth report
			// reads both and serves on the control port, current at ask time
			led := ledger.New()
			w.OnRecord(led.Observe)
			srv.Handle("/truth", report.Handler(w, led))
			go w.Run(ctx, cfg.IntrospectTick)
		}
	}

	go frood.Heartbeat(ctx, pub, "hm", observabilityproto.Schema, cfg.HeartbeatInterval, logger)

	logger.Info("hm starting", "brokers", len(cfg.KafkaBrokers), "addr", cfg.HTTPAddr)
	return srv.Serve(ctx)
}
