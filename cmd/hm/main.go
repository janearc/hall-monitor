// hm is the mesh's hall monitor: the resident that checks services actually
// do on the wire what they claim to do. v0 is the passive half: a fleet
// citizen (heartbeat, /health, /live, /metrics, JSON logs) running the
// consume-everything loop, broker introspection, the absence ledger, and
// the truth report at /truth. See doc/rfc-hall-monitor.md for the design.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	blmflag "github.com/janearc/big-little-mesh/flag"
	"github.com/janearc/big-little-mesh/emit"
	"github.com/janearc/big-little-mesh/frood"
	"github.com/spf13/cobra"

	"github.com/janearc/hall-monitor/pkg/config"
	"github.com/janearc/hall-monitor/pkg/connect"
	"github.com/janearc/hall-monitor/pkg/lease"
	"github.com/janearc/hall-monitor/pkg/ledger"
	"github.com/janearc/hall-monitor/pkg/metrics"
	"github.com/janearc/hall-monitor/pkg/report"
	"github.com/janearc/hall-monitor/pkg/server"
	"github.com/janearc/hall-monitor/pkg/watch"
)

// the connect-attempt counter, by OUTCOME per fleet policy, and the
// connected gauge. Registered at zero before the first attempt so the first
// failure lands on an existing series -- a counter that appears only once
// something has gone wrong is invisible exactly when it is new.
const (
	attemptsOK    = `hm_kafka_connect_attempts_total{outcome="ok"}`
	attemptsError = `hm_kafka_connect_attempts_total{outcome="error"}`
	connected     = `hm_kafka_connected`
	wireLost      = `hm_kafka_wire_lost_total`
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

// run is the daemon: logging, config, the control port, and the wire loop;
// blocks until signalled.
func run() error {

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	cfg := config.FromEnv()

	// The level is a dial, not a constant: boot at cfg.LogLevel (fleet
	// default warn), then follow the log.level flag -- hm's own scope over
	// _global -- for as long as flipr answers. Flipr absent holds the level;
	// see blm/flag for why tuning inverts the gate rule.
	var level slog.LevelVar
	if lvl, lerr := blmflag.ParseLevel(cfg.LogLevel); lerr == nil {
		level.Set(lvl)
	} else {
		level.Set(slog.LevelWarn)
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: &level}))
	slog.SetDefault(logger)
	if cfg.FliprURL != "" {
		go blmflag.PollLogLevel(ctx, cfg.FliprURL, "hm", &level, logger, 15*time.Second)
	}
	srv := server.New(cfg.HTTPAddr, logger)

	metrics.Add(attemptsOK, 0)
	metrics.Add(attemptsError, 0)
	metrics.Add(wireLost, 0)
	metrics.Set(connected, 0)

	// /truth is registered now, before the control port serves, and answers
	// 503 until the wire loop hands it a watcher. The absence ledger lives
	// for the whole process: a reconnection must not forget what it saw.
	truth := &report.Gate{}
	srv.Handle("/truth", truth)
	led := ledger.New()

	if len(cfg.KafkaBrokers) == 0 {
		// no broker named is a configuration fault, not an outage: nothing
		// to retry against, and /health says so for as long as it runs
		srv.SetDegraded("no kafka brokers configured (HM_KAFKA_BROKERS empty): hm has no eyes")
		logger.Error("hm is up with no broker configured; health reports degraded")
	} else {
		srv.SetDegraded("kafka not connected yet")
		go wireLoop(ctx, cfg, srv, truth, led, logger)
	}

	logger.Info("hm starting", "brokers", len(cfg.KafkaBrokers), "addr", cfg.HTTPAddr)
	return srv.Serve(ctx)
}

// session is one established wire: the publisher the heartbeat rides and
// the watcher the truth report reads. Both die together.
type session struct {
	pub *emit.Publisher
	w   *watch.Watcher
}

// wireLoop is the whole lifecycle of hm's connection to kafka, forever:
// connect (bounded backoff, every attempt counted and logged), serve until
// the watcher reports the wire lost, tear down, mark degraded, connect
// again. It returns only when ctx ends. Until 2026-08-22 this was one dial
// at startup; the VM restart that day scheduled hm before kafka's DNS
// existed and hm ran blind for forty minutes under a green liveness probe.
func wireLoop(ctx context.Context, cfg config.Config, srv *server.Server, truth *report.Gate,
	led *ledger.Ledger, logger *slog.Logger) {
	for ctx.Err() == nil {
		var s session
		err := connect.Loop(ctx, connect.Defaults, func(ctx context.Context) error {
			got, err := dial(ctx, cfg, logger)
			if err != nil {
				return err
			}
			s = got
			return nil
		}, func(a connect.Attempt) {
			if a.Err == nil {
				metrics.Inc(attemptsOK)
				logger.Info("kafka connected", "attempt", a.N)
				return
			}
			metrics.Inc(attemptsError)
			logger.Error("kafka connect attempt failed; will retry",
				"attempt", a.N, "err", a.Err, "next_try_in", a.Next.String())
		})
		if err != nil {
			return // ctx ended during connect
		}

		// the wire is up: say so on every surface that reports it
		metrics.Set(connected, 1)
		srv.SetOK()
		s.w.OnRecord(led.Observe)

		// this session's context: cancelling it stops the heartbeat, the
		// introspection tick and the lease judge, which is the contract Run's
		// comment states
		sctx, cancel := context.WithCancel(ctx)

		// the lease authority is session-scoped like the publisher it emits
		// through: a new session gets a fresh table, judged from the wire it
		// is actually watching rather than remembered from one it lost.
		auth := lease.New(sctx, s.pub, logger)
		s.w.OnRecordValue(auth.Observe)
		truth.Set(report.Handler(s.w, led, auth))
		go func() {
			// judge at a second's grain: cheap, idempotent, and finer than
			// any plausible heartbeat cadence.
			t := time.NewTicker(time.Second)
			defer t.Stop()
			for {
				select {
				case <-sctx.Done():
					return
				case now := <-t.C:
					auth.Tick(now)
				}
			}
		}()
		go frood.Heartbeat(sctx, s.pub, "hm", cfg.HeartbeatInterval, logger)
		runErr := s.w.Run(sctx, cfg.IntrospectTick)
		cancel()

		// the wire is down, or we are: say that first, then clean up
		truth.Clear()
		metrics.Set(connected, 0)
		if errors.Is(runErr, watch.ErrWireLost) {
			metrics.Inc(wireLost)
			srv.SetDegraded("kafka connection lost; reconnecting (see logs)")
			logger.Error("kafka wire lost; reconnecting", "err", runErr)
		}
		teardown(s, logger)
	}
}

// dial builds one session: the publisher and the watcher, or neither. A
// half-session (publisher up, watcher down) is torn down rather than kept,
// so every session is whole and the heartbeat never reports GREEN from a
// process that cannot see.
func dial(ctx context.Context, cfg config.Config, logger *slog.Logger) (session, error) {
	pub, err := emit.New(ctx, cfg.KafkaBrokers, cfg.SchemaRegistryURL)
	if err != nil {
		return session{}, fmt.Errorf("publisher: %w", err)
	}
	w, err := watch.New(ctx, cfg.KafkaBrokers, logger)
	if err != nil {
		pub.Close()
		return session{}, fmt.Errorf("watcher: %w", err)
	}
	return session{pub: pub, w: w}, nil
}

// teardown leaves the consumer group cleanly and releases both clients. A
// fresh context because the one that ran the session is already cancelled;
// the explicit leave matters (see Watcher.Close).
func teardown(s session, logger *slog.Logger) {
	leaveCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if s.w != nil {
		s.w.Close(leaveCtx)
	}
	if s.pub != nil {
		s.pub.Close()
	}
	logger.Info("kafka session closed")
}
