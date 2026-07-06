# hall-monitor 🤔

hm is the mesh's hall monitor. It checks that services actually do on the
wire what they claim to do — before their code is allowed onto the mesh, and
continuously while it runs.

Every other gate in this fleet evaluates artifacts: the judge reads diffs
against specs, CI runs tests, coverage counts lines. hm exists because the
mesh once ran for a week with producers emitting to topics nothing consumed,
and every artifact gate stayed green the whole time. The wire is the only
source of truth for integration, and hm owns the wire.

## What it does today (v0)

- Consumes every topic and reads the broker's own metadata: consumer groups,
  subscriptions, offsets, lag.
- Keeps an absence ledger. A missing heartbeat, a reply that stopped coming,
  a producer with zero live consumers — absences are first-class, alarmed
  findings, not mysteries for later.
- Produces the truth report: who is talking, to whom, and who is emitting
  into the void — surfaced where the operator actually looks, with
  refusal-class findings paging a human.

## What the design ratifies next

- **Surrogate sessions** — at deploy time, hm presents the entire mesh's
  wire surface to one candidate build, runs the same session against the
  current build, and mechanically diffs the two transcripts. A behavior
  that silently disappears is refused before it lands.
- **Attestation** — per-service broker credentials, minted by hm only for
  deploy tags signed by the operator's key. No signature, no mesh.
- **Leases** — admission is time-bounded. Trust that is not renewed decays
  to refusal on its own clock, and the lease stream broadcasts every
  countdown.

## Doctrine, one line each

Refusal is the default. Satisfied, never bypassed. Observed over claimed.
Broker state is the truth; events report it. Trust is time-bounded. The
operator's key is the root of trust.

## Reading further

The full design — doctrine, the tier ladder, attestation mechanics, the
mapesis assessment tier, and the whiteboard — is
[doc/rfc-hall-monitor.md](doc/rfc-hall-monitor.md).

hm is a Go service and a fleet citizen like any other: `/health`,
`/metrics`, structured JSON logs, contracts first.
