# EXTRACTED: Mapesis -- belongs to wonderlib, parked here in transit

This was section 5 of rfc-hall-monitor.md. It is real, ruled design -- "mapesis
lives in wonderlib" is its own first ruling -- and it landed in hm's RFC because
both were being worked at once, not because they are one system. The operator
confirmed on 2026-08-28 that mapesis has nothing to do with hm.

Extracted verbatim below so the ruling record survives the move. Relocate to
wonderlib's tree on the operator's word; nothing in hm references this file.

---

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

