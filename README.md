# hall-monitor

Every gate in this fleet judges artifacts: the judge reads diffs against
specs, CI runs tests, coverage counts lines. All of them can be green while
the mesh itself is dead. It happened: producers emitted into topics nothing
consumed for a week, and every artifact gate smiled through it. The wire is
the only place integration is true, and hm is the service that owns the wire.

hm sits on the bus, reads the broker's own metadata, and says out loud what
it sees -- including about itself. The first finding it ever filed in the
whoops environment was that its own heartbeat had no consumers. That is the
job, working.

## What it does today (v0, verified against the running service)

- **Consumes every topic.** Discovery is dynamic: an introspection tick asks
  the broker what exists and subscribes by name, so the consumed set is
  auditable and never depends on pattern semantics.
- **Reads broker truth.** Consumer groups, subscriptions, offsets, lag --
  from the broker's metadata, not from what services claim.
- **Keeps the absence ledger.** Presence with history: a producer that goes
  quiet past three of its own observed cadences is a finding, not a mystery.
  Never-observed gaps are never reported -- no history, no verdict.
- **Files refusal-class findings.** `void`: a live producer with zero live
  consumer groups. `silent`: a known producer gone quiet. Findings are
  self-contained rows, machine-readable.
- **Emits a contracted heartbeat.** Every interval, onto
  `observability.events`, framed by big-little-mesh's publisher in
  schema-registry wire format. hm is a fleet citizen on the bus it audits.
- **Serves the truth report.** `/truth`: who is talking, to whom, and what
  is yelling into the void.

## Leases -- the active build

Ruled by the operator, 2026-08-28: hm manages leases, and it must be visible
at a glance who is authorized to be on the network.

The shape: a lease is a contracted bus message. A service requests at boot
and renews on an interval; hm is the authority; trust that is not renewed
decays to refusal on its own clock. `/truth` grows an `authorized` table --
service, lease state, last renewal -- and a producer observed on the wire
without a live lease is a refusal-class finding. flipr and kingfisher are the
first lessees. Enforcement (per-service credentials, signed-tag admission)
comes after visibility, per the RFC; findings precede blocked connections.

## Surface

| endpoint | what it answers |
|---|---|
| `/health` | readiness: can hm see the wire (503 while blind) |
| `/live` | liveness: is the process up |
| `/metrics` | prometheus: `hm_kafka_connected`, `hm_records_consumed_total{topic}`, connect attempts, wire losses, topics subscribed |
| `/truth` | the truth report: topics, groups, findings |

| env | default | meaning |
|---|---|---|
| `HM_KAFKA_BROKERS` | `localhost:9092` | seed broker list, comma-split |
| `HM_SCHEMA_REGISTRY_URL` | `http://localhost:8081` | the shared registry |
| `HM_HTTP_ADDR` | `:8090` | control port |
| `HM_HEARTBEAT_INTERVAL` | 25s | heartbeat cadence |
| `HM_INTROSPECT_TICK` | 30s | broker introspection cadence |

## Deployment

hm runs in both environments from the same source: prod's `fleet` namespace,
and whoops (`hm.test`) where its development lives. The whoops build is
pinned by commit -- `create.sh` in janearc/whoops refuses to build if the
checkout has moved off the pin, so the manifest names exactly what runs.
Contract bindings come from big-little-mesh as a pinned Go module; there is
no codegen at deploy time.

The bus must hold `observability.events` before the heartbeat can land;
topic creation is a bus furnishing (a file, not a hand step), and producing
to a missing topic fails -- franz-go does not auto-create.

## Honest state of the tests

config 93.8 / ledger 95.0 / report 93.2 / connect 87.5 / metrics 82.4 /
server 75.0 / **watch 29.2** -- and watch is the core: the consumer loop,
reconnect, the introspection tick. There are no integration tests yet;
whoops exists so they can run against a real broker. Both gaps are owned
work, not accepted debt.

Known defect, on the list: `/health` reports the consume side honestly and
says nothing about the emit side. hm's heartbeat failed for an absent topic
for minutes while `/health` said `ok`.

## Doctrine, one line each

Refusal is the default. Satisfied, never bypassed. Observed over claimed.
Broker state is the truth; events report it. Trust is time-bounded. The
operator's key is the root of trust.

## Reading further

The full design -- doctrine, the tier ladder, attestation mechanics, and the
whiteboard -- is [doc/rfc-hall-monitor.md](doc/rfc-hall-monitor.md).

hm is a Go service and a fleet citizen like any other: `/health`,
`/metrics`, structured JSON logs, contracts first. It is owned by the
infrastructure family and its home is whoops.
