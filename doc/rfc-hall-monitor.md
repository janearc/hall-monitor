# rfc: hall-monitor (hm)

Status: RATIFIED 2026-07-06 (sprint 13 T0; janearc/sprints PR 46, merge
9a8469e). This is the living design document for hm. The origin copy —
including its full review history across six rounds — is archived in the
sprints repo at `2026-07-06/SENTINEL-DESIGN.md` and does not change; this
copy is the one that evolves with the service, by PR, like everything else
here. The service is `hm`; "sentinel" names the ROLE it implements. Mascot:
thinking-face emoji, display surfaces only, never in shell output.

The design has three components: **hm** (section 3), **additions to the
judge** (section 4), and **mapesis** (section 5). They are separable —
mapesis failing entirely is a stopping condition for section 5, not for hm.

## 1. The problem

The mesh ran for roughly a week in a state we would have called an outage if
anything had been obligated to look: producers publishing to topics nothing
consumed, a sidecar down while its service kept emitting into the void. Every
gate stayed green, because every gate we have evaluates artifacts, and the
failure was on the wire.

Good code can pass every artifact gate and still misbehave on the network.
The only legitimate source of truth for integration is the wire, and nothing
owns the wire. hm is the check that comes from the mesh:

| Gate | Rules on | Fires |
|------|----------|-------|
| Judge | what the software SAYS — diff vs spec, artifact hygiene | at landing, commit-side |
| hm | what the software DOES — wire behavior, integration liveness | at deploy time and continuously |
| Enablement | what RUNS — runtime gating via delightd /state | at every consumer read |

Three independent verdicts; a change MUST satisfy all three; they MUST NOT
be collapsed. Complementary layers, stated plainly: test coverage and the
judge assess INTERNAL code behavior; hm asserts EXTERNAL, observable
behavior — what actually appears on the wire and in the store. Neither
substitutes for the other.

Uber's machinery (BITS/SLATE shift-left E2E, enclave gates) is design
reference, not parentage — and the Uber knowledge here is the operator's
dated firsthand experience plus public material plus extrapolation, not
current fact. hm is its own thing, built for an n=1 mesh on one machine.

## 2. Declarations, with consequences

Each load-bearing assumption, and what it costs.

| Declaration | Consequence |
|-------------|-------------|
| hm is the ONLY writer of broker policy (ACLs, quotas, credentials). | A foreign mutation voids the affected principals' leases and raises a mesh-wide alarm; unattributable mutations void ALL leases. hm reconciles broker state against its ledger on a jittered loop to make this enforceable. |
| hm places secrets it cannot read back (create/update without get/list). | If hm is compromised it can replace a credential but not read the estate; the operator has accepted hm's total risk envelope explicitly. |
| The operator's signing key is the root of trust. | Key unavailable = deployment freezes. hm can refuse but cannot invent; nothing enters the mesh that does not trace to the operator's signature. See untrusted mode (3.5) for the crypto-broken case. |
| Kafka grants have no native TTL. | Lease decay must be engineered — self-fencing froods, reaping at startup and on a loop, alarms on hm silence. Claimed fail-closed would be a lie; engineered decay is the honest version. |
| The broker is the enforcement point. | We manage the bus, not the packets; the bus already evaluates every request. hm never sits on the data path, so hm's own load cannot slow the mesh. |
| One hm governs the whole mesh today; enclaves are the logical model. | Generalization is a primary guiding philosophy of the mesh and delightd: the code speaks enclaves from day one, the deployment is one instance until surface area demands more. |
| hm can remove any service's network access. | That is its purpose, and it is why doctrine binds it this tightly and why its every verdict must cite evidence. |
| v0 is not live until the operator can see it. | The operator has no way to know the mesh is down if nothing tells him. A truth report he can read and an alarm path that reaches him are v0 definition-of-done, not follow-up. |
| surrealdb is tier-1/essential (operator ruling). | Session-scoped namespaces (3.3) ride an essential service; hm's own ledger still keeps its independent embedded store so hm can rule while anything else is down. |

## 3. hm

### 3.1 Doctrine

1. **Refusal is the default.** Absence of a verdict is a refusal. An
   evaluation that cannot complete MUST refuse and cite what it could not
   reach. An unciteable verdict is a refusal.
2. **Satisfied, never bypassed.** No override flag, no force path.
   Disagreement with a refusal changes hm's rules through review, never
   routes around them.
3. **Observed over claimed.** "Emits to kafka" is a claim; a live consumer
   group with moving offsets is evidence. Where the broker can be asked
   directly, claims are not accepted — including a service's own declared
   traffic, which is advisory input, never truth.
4. **Broker state is the truth; events report it.** Enforce in the broker
   FIRST, then announce. Never announce-and-hope.
5. **Trust is time-bounded.** Every admission is a lease; decay is
   engineered per declaration 4.
6. **The human key is the root of trust** (declaration 3).
7. **Every verdict, refusal, and finding is recorded as a regular,
   machine-readable record.** The ledger is deliberately a training corpus:
   we describe what we did and what happened so uniformly that a model can
   be fine-tuned on it later (the sprints-29 long game).

### 3.2 The passive resident (v0, this sprint)

Read-only against production. Resident, cheap, mechanical, no LLM.

- Consumes every topic — its decoders are generated from the contract set,
  fresh-fetched every build. A contract hm does not know is one it cannot
  rule on, and cannot-rule is refusal: a stale hm fails loud.
- Reads broker metadata: consumer groups, subscriptions, offsets, lag.
- Keeps the **absence ledger** — who emits what at what cadence, what
  follows what — so a missing heartbeat, a vanished reply, and an emitter
  with zero live consumers are first-class alarmed findings. A missing
  result is harder to diagnose than broken traffic; only a resident with
  history sees one.
- Produces the **truth report**: machine-readable, who is talking, to whom,
  and what is emitting into the void. Pairs with the delightd 85 inventory
  (what is RUNNING vs what is TALKING; the diff is where silent outages
  live).
- **Operator observability, in v0**: the truth report surfaces in the
  floater — a roster pane, two badges per service, RYG health and
  grey/yellow/green lease state (grey = no lease machinery yet, which is
  everything in v0) — and refusal-class findings page the operator through
  the existing alarm path.

The passive resident injects nothing; chaos engineering is a distance goal
(section 7) and does not enter through this door.

### 3.3 Surrogate sessions

At deploy time there is exactly one candidate, so hm needs no second mesh:
it presents the mesh's wire surface to one service, playing every
counterparty at once. The surrogate's producers and decoders are gen output
from the same contracts — the virtual mesh cannot drift from the real one
without gen-freshness catching it.

- **Isolation is topic-space**: an ephemeral set of session-prefixed topics
  on the production broker plus a session consumer group, env-plumbed into
  the candidate. Nothing in a session touches production topics; the schema
  registry is shared because the schemas ARE the contracts. Cost: a few
  partitions, TTL cleanup.
- **Baseline comparison**: every session runs twice — candidate and current
  production build — and the verdict includes the mechanical diff of the two
  transcripts. A silent removal becomes a visible behavioral diff before
  landing instead of a three-days-later mystery.
- **Stateful surfaces**: a candidate with database state gets a
  session-scoped surrealdb namespace per run (one each for candidate and
  baseline). Stateful dependencies are DEMONSTRATED, not declared: the
  session requires a round-trip through the candidate's own code path —
  store, read back — and hm checks the session namespace for the actual
  writes. This catches the degraded-but-alive frood: up, heartbeating,
  well-behaved, reconnect-looping on a failed database connection, useless.
  Alive is not healthy; leases renew on healthy.
- **Partial coverage, named**: some state cannot be session-scoped (an
  external endpoint, a store with no namespace mechanism, host-path state).
  Then the evaluation is PARTIAL, the verdict names the uncovered surface,
  and whether partial is admissible for that service is recorded in its
  scope declaration at the operator's ratification. Per service, visible,
  never silent.
- **Non-goal**: the surrogate validates declared traffic; it does not
  explore state space. No fuzzing, no fault injection, no
  retries-as-probes.

One broker, two worlds — the session is topic-scope, not a second anything:

```
                     production broker (the one appliance)
   +--------------------------------------------------------------------+
   |   production topics                  session topics (ephemeral)    |
   |   delight.events, ...                hm.session.<id>.*  [TTL]      |
   +--------------------------------------------------------------------+
        ^          |                          ^            |
        |          v                          |            v
   leased froods (real traffic)          candidate <---> hm surrogate
                                             |           (plays ALL
                                             v            counterparties;
                                    session surrealdb     generated from
                                    namespace (per run:   the contracts)
                                    candidate | baseline)

   shared schema registry (the schemas ARE the contracts)
   production state: unreachable from any session
```

### 3.4 Enforcement

The broker is a bus that already evaluates every request; hm manages the
bus. Levers, in deployment order:

| Lever | Requires | Effect |
|-------|----------|--------|
| Quotas keyed on client-id | nothing; works on PLAINTEXT today | throttle a named client to zero. Self-reported identity: adequate against bugs (our threat model), useless against malice. Fleet convention: client-id MUST be the frood identity. |
| Consumer-group member removal | nothing | force a rebalance without the member; a shove, not a wall (clients rejoin) |
| ACL deny per principal | SASL (v1) | per-request authorization — revocation reaches even open connections on their next request. This is "tell kafka not to accept that." |

**Leases.** Admission is a lease on (principal, commit), granted after a
surrogate pass. Renewal is automatic while (a) no open REFUSAL-CLASS finding
stands against your traffic and (b) no new build of you has registered;
either failing forces a fresh surrogate session first. **Renewal is
mechanical** — ledger and registration checks only, no session, no mapesis,
no metal. hm at steady state is a consumer and a bookkeeper.

Refusal-class, enumerated (grows only by ratified addition):

| Class | Finding | Blocks renewal |
|-------|---------|:--:|
| refusal | off-contract traffic (undecodable against any known schema) | yes |
| refusal | emission outside declared scope | yes |
| refusal | declared emission silent past N cadences | yes |
| refusal | producing with zero live consumer groups past threshold | yes |
| refusal | traffic on a contract hm does not know | yes |
| refusal | foreign broker-policy mutation attributed to this principal | yes |
| finding | cadence jitter within declared bounds | no |
| finding | consumer lag growth, recovering | no |
| finding | novel-but-valid pattern (classification is v3; v0-v2 record only) | no |

**The lease stream** is a contracted event stream hm owns, broadcasting each
lease clock ("example-svc lease expires in 14:59"). It is the legible shadow
of broker state, never the mechanism (doctrine 4). Subscribers: the
good-citizen library self-fences on it (a frood that cannot see its own
countdown treats the lease as expired and stops producing); delightd
reflects it into enablement; the floater renders it as the lease badge.

**An hm outage fails the right way**: citizens self-fence on their own
clocks; enforcement for non-cooperative clients freezes until hm returns and
reaps (reconciliation runs FIRST at startup); delightd alarms on hm silence.
Closed for citizens, frozen-and-alarming for strangers.

**Promotion is an ACL rewrite** (v2): an unleased build's ACLs reach only
its surrogate-session topics — the "dev network" is not a place, it is the
scope of an unjudged principal's ACLs. Passing the session rewrites ACLs to
production topics and starts the lease clock. Nothing to bypass, because
there is no path.

### 3.5 Attestation and identity (v1, specified now so v0 does not paint over it)

Three questions, three mechanisms we already run:

1. **Who is speaking** — SASL/SCRAM per frood; principal = frood name. The
   broker (cp-kafka 7.5.0 / kafka 3.5) handles SCRAM, ACLs, and quotas over
   the plain admin API. Its PVC (PersistentVolumeClaim — kubernetes' claim
   on disk that survives pod restarts) is bound at 20Gi: verified by
   INSPECTION only. v1 includes a restart test — write a SCRAM credential,
   restart the broker, verify it persists — before any of this is trusted.
2. **Is it really them** — Kubernetes is already an attestation authority:
   each frood's credential lives in a Secret RBAC-scoped to its own
   ServiceAccount. Possessing the credential implies being the workload k8s
   says you are. No SPIFFE deployment.
3. **Is it the code we judged** — the lease binds (principal, commit);
   delightd registration (the existing seam) grows one obligation: present
   your build identity. delightd is the identity REGISTRAR; hm is the
   policy ENGINE.

**Identity carries a descriptor hm populates.** Beyond the name: a
programmatic statement of what the thing IS in mesh terms — test
deployment, surrogate-session candidate, leased production build, untrusted
(break-glass) — written by hm from its own admission state, not
self-declared by the service. The floater and the truth report render it.

**The deploy ceremony.** Landing does not change (branch, PR, judge,
merge). Deployment requires a tag signed by the operator's key — merge
commits are signed by GitHub's web-flow key, not the operator's, so the tag
is the signature target. The two gates authorize different things: the
LANDING gate (PR review, judge) authorizes what enters the REPO; the signed
tag authorizes what RUNS ON THE MESH. The ceremony is ONE deliberate
gesture — touchid, sign, deploy — a single command that signs and pushes
the tag, the same weight as clicking merge. We land diffs most days; a
ceremony that fights that cadence gets routed around, and routing around is
what doctrine forbids. v1 ships the runbook AND the tooling, and they MUST
match — divergence is a defect in whichever is wrong. RULED: no deployment
without a signature; the --no-gpg-sign fallback is retired for fleet deploy
tags.

**No machine key — RULED.** Everything routes through the operator's right
index finger: touchid → sign → deploy on this machine; anywhere else would
require a hardware key (yubikey-class) as the equivalent gate. The scoped
machine key was refused for the reasons that decide themselves: key custody
on the mesh's own machine is theater, a bump-classifier becomes a gameable
security boundary, and a dependency bump is precisely where the operator's
signature carries the MOST value — supply-chain attacks are dependency
bumps. The ruling is coded to leave room for BATCH signing: a tag points at
whatever landed HEAD the operator chooses, so one signature can deploy one
change or twenty accumulated bumps. Batching is signing less often, not a
new mechanism, and it is never per-commit — the gate is per-DEPLOY.

```
  LANDING (authorizes the repo)      DEPLOY (authorizes the mesh)     hm
  -----------------------------      ----------------------------    -----------------------
  branch -> PR -> judge -> merge --> operator: touchid, sign tag --> verify tag signature
                                     (one or many landed changes)      | fail -> REFUSE
                                                                       v
                                                                     judge verdict green?
                                                                       | fail -> REFUSE
                                                                       v
                                                                     surrogate session
                                                                     (candidate + baseline)
                                                                       | fail -> REFUSE, cited
                                                                       v
                                                                     mint (rotate SCRAM)
                                                                       v
                                                                     promote (broker ACLs FIRST)
                                                                       v
                                                                     announce (lease stream)
```

**Untrusted mode — RULED, it exists.** Shit breaks: this mesh runs on an
unreleased Apple beta that may break cryptography overnight and not
apologize. If the operator cannot sign or hm cannot verify, the mesh must
not simply go down. Untrusted mode: invocable only by the operator,
locally; FREEZES existing leases (decay pauses, self-fencing stands down);
mints, promotes, and admits NOTHING new; the lease stream broadcasts
UNTRUSTED continuously and every descriptor says so; time-boxed, re-invoked
by hand. Doctrine 2 survives because nothing new is satisfied into the
mesh — the already-admitted stops expiring while the operator repairs
trust. Degraded beats down; blind never beats either.

**The mint flow.** A new service (call it `example-svc`) declares its scope
— name, ServiceAccount, topics — as a signed change to the fleet roster. hm
sees a signed scope for an unknown name: identity born. Per revision:
signed tag + judge green + surrogate pass → hm mints. The principal stays
the service name; the PASSWORD rotates per deployed commit (a commit-bearing
principal would churn ACLs every deploy). hm writes the credential to the
broker and places the Secret into the service's scope.

**Migration, no flag day.** Two listeners: existing PLAINTEXT plus a new
SASL port — the frood-lib migration pattern, proven once already. First
enumerate which roster services actually use kafka in a way the surrogate
and judge can assess; that list is the migration surface. One issue per
frood in its own repo, worked opportunistically. hm's truth report names
who remains on the legacy listener; PLAINTEXT retires when the list is
empty (tracking issue filed in hall-monitor — closing it will be
satisfying).

**hm's store.** Its own PVC on local-path (the k3s default class), a
deliberately boring embedded store. The BROKER holds WHAT (credentials,
ACLs, quotas); hm holds WHY (identities, leases, verdicts with cited
evidence). hm depends on no other service's pod to know who anyone is.

### 3.6 Enclaves

An enclave is a traffic domain. Ours today: **backend** (delightd,
registration/state, observability) and **data** (the work froods produce
and consume). hm enforces at the bus all domains share, so it "sits
between" enclaves logically while sitting on top of everything physically.
One instance today; the code speaks enclaves from day one (declaration 6).

### 3.7 Language

**Go.** hm is arguably the second-most-critical service after delightd, and
the call is confidence and reviewability: franz-go is native with deep
in-fleet precedent; the Rust path wraps librdkafka in C; the operator's
review budget weights Rust 5x Go, disqualifying by itself at this
criticality. Rust was considered seriously and declined. The wire keeps
language freedom regardless — nothing else knows or cares what hm is
written in.

## 4. Additions to the judge

Commit-side, on artifacts, touching nothing on the wire. Everything
decidable from artifacts belongs to the judge; it gains three checks:

- **Provenance — no copies, ever.** No checked-in copy of a remote .proto,
  remote gen output, or another repo's library code. Compliant shapes:
  import the owner's generated library pinned, or regenerate every build
  from a FRESH fetch of the remote source. "Copied byte-for-byte so it's
  the same" is the named laundering move; the judge refuses it.
- **Declared traffic.** Each frood's contract surface declares emissions
  and expectations, cadence included; the judge verifies the declarations
  exist and cohere with the diff. hm consumes them as generated data and
  holds them to doctrine 3: a declaration that disagrees with observed
  traffic is itself a finding — code or declaration is wrong, both are
  defects, the broker's word wins meanwhile.
- **Contract-diff hygiene.** buf-breaking and gen-freshness results are
  cited in the verdict, not assumed.

## 5. Mapesis

**Mapped synthesis → "mapesis"** (operator's coinage, canonical):
mechanically decompose material into chunks small enough for a local model
to process without confabulating (the MAP); process each chunk; synthesize
the chunk results with the same local process (the SYNTHESIS). wonderlib
already works this way — this formalizes an existing process. RULED:
**mapesis lives in wonderlib.** Consumers (judge and hm, both Go) reach it
across the language boundary: a thin contracted surface over the Python/MLX
carve-out, strictly typed, treated as untrusted input, never an in-process
import.

**The economics.** Fable runs on its own token allotment under the
operator's Claude Max plan; Opus decomposes documents well but Fable signs
off eventually. The goal is not a free tier — it is getting Fable and Opus
OUT OF THE MIDDLE:

1. Fable decides mapesis applies and writes the mapper instructions —
   highly regular, curated for the engines doing the work. Mapesis MAY
   first assess a document's viability for mapping, and an advanced model
   MAY recompose a document into mapesis-available form.
2. Mapesis maps.
3. Mapesis synthesizes.
4. Fable verifies the synthesis AGAINST THE INSTRUCTIONS — by definition
   without re-reading the corpus, or the point is defeated.

Requirements that fall out of step 4: mapesis products are not prose — they
are itemized digests of exactly what was asked, trivially verifiable
without re-reviewing the corpus, cache-optimized (avoid vocabulary with
high encoding tax). Inputs may drift from natural-sounding language toward
the harshest reduction that still carries content — approaching a regular
grammar — and that is intended. When it works it saves up to twenty minutes
of Fable wallclock per document.

**Engines — enumerated on this host before commit** (macOS 27.0 "golden
gate" beta, July 2026). The throwaway, included with its output per review:

```swift
// throwaway: enumerate Apple Foundation Models availability on this host
import FoundationModels

let base = SystemLanguageModel.default
print("default model availability: \(base.availability)")
print("default model isAvailable:  \(base.isAvailable)")
for (name, uc) in [("general", SystemLanguageModel.UseCase.general),
                   ("contentTagging", .contentTagging)] {
    print("useCase \(name): available=\(SystemLanguageModel(useCase: uc).isAvailable)")
}
print("supported languages: \(base.supportedLanguages.count)")
```

```
default model availability: available
default model isAvailable:  true
useCase general: available=true
useCase contentTagging: available=true
supported languages: 23
```

**What it actually is** (so we can reason about capability, not just
presence). This is Apple's own foundation model family, WWDC26 generation:
the on-device model was rebuilt this cycle and sits in the ~3B-parameter
class; the announced "AFM 3 Core Advanced" (20B sparse, 1-4B parameters
active per request) is the Private Cloud Compute tier, which reports
UNAVAILABLE in this context — fine, because a cloud tier would defeat the
point. It is not Siri (Siri's models are separate asset families, visible
side-by-side on disk: UAF_Siri_* vs UAF_FM_*), and it is not a repackaged
third-party model. Not phi-2. Asset families present on this host include
FM_GenerativeModels, FM_CodeLM — a distinct code model, worth its own
sprints-38 look — and FM_Visual (the WWDC26 vision capability); asset
directory sizes are root-locked, so on-disk weight was not measured.

macOS 27 also ships `fm`, a first-party CLI over the framework
(/usr/bin/fm): `available`, `chat`, `respond`, `schema` (JSON generation
schemas), `token-count`, `quota-usage`, and — the integration seam handed
to us — `serve`, a local Chat Completions API server over the on-device
model. wonderlib can speak a standard chat-completions dialect to the metal
without any Swift bridge of ours.

```
% fm available
System model available
```

First capability datum, recorded: asked (via `fm respond`) for an exact
JSON object "and nothing else," the model returned the correct JSON wrapped
in a markdown fence — instructable, with small-model texture the harness
strips mechanically. That is a mapesis-shaped answer to a mapesis-shaped
ask. What sprints 38 still owes is the real capability evaluation: can it
hold the mapper role over our material without confabulating.

**Engine posture, ruled: mapesis is PROVEN on mistral-24b, and the engine
seat is a config value.** A 3B-class model is expected to underperform this
role today; we do not gate mapesis on it. Also on this host (ollama):
mistral-24b (19GB, genuinely good at regular structured work, not on the
metal), llama3.1, llama3.2. The alignment that makes the seat swappable is
already in place: ollama and `fm serve` both speak the chat-completions
dialect, so mapesis targets ONE endpoint shape and the engine changes by
configuration — kick the chair out from under mistral whenever a stronger
engine arrives, on-metal or otherwise. The platform's trajectory makes a
stronger on-device model a matter of when, not if; when AFM (or FM_CodeLM)
clears the capability bar sprints 38 defines, it takes the seat and the
work moves to the ANE for free. Until then mistral proves the process, and
every engine evaluation lands as a dated finding (doctrine 7).

**Bounds.** hm's transcripts and the judge's evidence bundles are
mapesis-shaped from day one (self-contained units, machine-readable,
independently judgeable). Steady-state metal load is ~zero: assessment
fires at deploy time and on flagged novelties, never on the renewal clock.
The Go caller wraps mapesis in an aggressive timeout; unavailable, hung, or
over-time assessment is cannot-rule, and cannot-rule is refusal citing
"assessment unavailable" — recorded as a finding like every other verdict
(doctrine 7), so the pattern of assessment failures is itself examinable
later. Model flakiness makes deploys wait; it never makes them blind, and
it never silently downgrades a judged admission to a mechanical one.

**This effort may fail entirely.** We go to great lengths to prove it
possible or conclusively not-worth-it; a first or second failure is a
finding, never the verdict — and so is the conditional answer: "possibly a
more advanced metal model would make this possible, but it is not possible
presently" is a RECORDED finding (doctrine 7), dated and revisitable when
the metal improves, not a shrug. If mapesis fails, that is a stopping
condition for this section only — hm's mechanical layers stand without it,
and the hole gets a deliberate decision.

## 6. Estate and sequencing

Swept into this design at operator direction:

- **kafka-svc: sunset.** RULED 2026-07-06: contracts → big-little-mesh
  (per the kafka-svc 14 division), broker manifests → the meubilair
  estate; archive when both land. hm absorbs its reason to exist. taco's
  no-copies compliance (HELD this morning) rides the sunset — one
  migration, pointed at the contracts' final home.
- **obs-svc: sunset.** RULED 2026-07-06: truth report + absence ledger
  replace its proof duties; tiny-monitor stays the face (the roster pane
  is the concrete replacement surface); archive when the truth report
  exists.
- **Pre-delightd furnishings** (5Gi elasticsearch, grafana): bucketed in
  the delightd 85 inventory as furnishing candidates — likely
  adopt-and-bring-current, the inventory's call to stage.

| Rung | Scope | When |
|------|-------|------|
| v0 | this doc ratified; passive resident on the network; truth report; quota lever; floater roster pane + alarm path | today (13) |
| v1 | attestation: SASL listener, SA-scoped SCRAM, scope declarations, signed-tag ceremony with one-click tooling + matching runbook, broker durability restart test, mint flow, registration carries build identity, kafka-user enumeration + per-repo migration issues | next sprint, named |
| v2 | leases, surrogate sessions, promotion-by-ACL, lease stream, untrusted mode (if ratified) | after v1 |
| v3 | mapesis assessment on transcripts (rides sprints 38); emergent-state classification | after the metal proves out |

**v0 definition of done**: the truth report exists, is machine-readable,
names every producer with no live consumer and every declared emitter that
is silent — AND the operator can see it and be paged by it. The failure
class we lived through last week cannot recur unobserved, where "observed"
means by a human.

## 7. Not decided here

- Enclave boundaries beyond the two named (operational need, not
  speculation).
- Chaos engineering / active fault discovery — a distance goal, not this
  service.
- Emergent-state classification ("new legitimate pattern" vs "slow mutual
  deadlock") — v3, honestly hard; v0-v2 claim only the mechanical wins.
- Credential-minting automation beyond the operator-signed flow — future
  scope. (The machine-key question is CLOSED: refused, batch-signing room
  coded in — see 3.5.)

## 8. Whiteboard

Go-flavored pseudocode, not compilable. The lifecycle is a state machine.

```go
// Identity: born from a signed scope declaration in the fleet roster.
// Desc is POPULATED BY HM from its own admission state (test deployment,
// session candidate, leased production, untrusted) — programmatic, never
// self-declared by the service. Rendered by the floater and truth report.
type Identity struct {
    Name  Principal // "example-svc" — stable broker principal
    SA    string    // k8s ServiceAccount the credential is scoped to
    Desc  string    // hm-populated: what this thing IS in mesh terms
    Scope Scope     // topics it may produce/consume
}

// Not raw strings: validated newtypes, constructed only by parsing.
type Principal string   // roster-legal name; parse-don't-validate
type CommitSHA [20]byte // from the signed deploy tag, hex-parsed

type Lease struct {
    Principal Principal
    Commit    CommitSHA
    ExpiresAt time.Time // the lease stream broadcasts this countdown
}

// A build's admission state machine. Default is refusal (doctrine 1).
type admission int
const (
    unregistered admission = iota // no signed scope: no principal, no path
    registered                    // identity known; ACLs reach surrogate topics ONLY
    underSession                  // surrogate session running (candidate + baseline)
    leased                        // promoted: production ACLs + lease clock
    decayed                       // lease expired unrenewed: quota-zero / ACL deny
)

func (h *HM) evaluate(dep Deploy) {
    if !h.verifySignedTag(dep.Tag) { h.refuse(dep, "unsigned tag") ; return }
    if !h.judgeGreen(dep.Commit)   { h.refuse(dep, "judge not green"); return }
    cand := h.session(dep)          // candidate transcript, session topics
    base := h.session(h.prod(dep))  // baseline against current build
    diff := transcriptDiff(base, cand) // mechanical; mapesis-shaped chunks
    if !h.assess(diff) { h.refuse(dep, diff.Citations()); return }
    h.mint(dep)      // SCRAM password rotated, Secret placed
    h.promote(dep)   // broker FIRST: production ACLs, lease clock
    h.announce(dep)  // THEN the lease stream says what is already true
}

// The passive resident: read-only, no LLM. This loop and the
// reconciliation tick are hm's hot paths and instrumented as such: loop
// duration, message rate, and hm's own CPU/memory ride /metrics; ticks
// are jittered so reconciliation cannot self-synchronize into a periodic
// spike; hm reports its own resource pressure as a first-class signal —
// if hm is the problem, hm says so.
func (h *HM) watch(ctx context.Context) {
    for ev := range h.consumeEverything(ctx) {
        h.ledger.Observe(ev) // cadence, exchanges, history
    }
    // separately, on a jittered tick: broker metadata — groups, offsets,
    // lag — diffed against declared traffic; absences alarm, orphans alarm.
}
```

Whiteboard:code ratio tracked at close per the standing convention.
