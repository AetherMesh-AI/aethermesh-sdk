# Bounded naming and promotion fixture

This is a pure SDK contract prototype for the [accepted roster and plan](capability-naming-plan.md).
It neither trains a model nor certifies live capabilities. Run it from a checkout
with the SDK installed (or `PYTHONPATH=src`):

```sh
python examples/capability_fixture_demo.py
```

The example reads checked-in synthetic data from `tests/fixtures/` and prints an
eligible **simulated** proposal for Ash: internal capability version 3 → 4. Public
names contain no version suffix. The separate contributor fixture projects Ash
and Ember from synthetic execution events and labels the output simulated. These
are not eight trained available experts, actual routed participation or a model picker.
The example has no network, trainer, router, credential or deployment action.

## Evidence contract

`aethermesh_core.capability_fixture.evaluate_naming_fixture` takes bounded JSON
bytes, an external tuple of trusted **fixture** report digests and an external
expected policy digest. The sample stores pins separately in
`capability_naming_pins.json`. Selecting pins is a local test assumption, not
cryptographic evaluator provenance or publisher authorization. Do not derive trust
from a report's self-declared evaluator or silently accept its supplied digest.
`fixture_digest` is a helper for already bounded structured fixture data, not a
general untrusted-object serializer.

The exact version-1 input contains:

- Baseline and candidate profiles: immutable artifact ID, parent ID, stable lineage,
  input/output schema commitment, trainer label and intended capability.
- Baseline and candidate reports: exact artifact/baseline/lineage/schema, evaluation,
  dataset and taxonomy commitments, evaluator label, metric values and uncertainty.
- Externally pinned policy: exact baseline, current internal capability version,
  evaluation/dataset/taxonomy commitments, allowed fixture evaluator labels and
  complete metric rules (direction, required gain, tolerated regression, uncertainty
  and candidate value bounds).
- Versioned taxonomy: capability labels, classification intervals and ordered name
  candidates. Namespace: stable lineage-to-name assignments. Hints: run count and
  self-description, intentionally ignored for decisions.

A candidate must descend from the pinned baseline, differ in artifact identity and
preserve lineage, schema and intended role. Both reports must match all commitments;
changing evaluation or taxonomy requires a fresh matching pair. An allowed evaluator
label cannot equal either trainer label. A real identity/provenance system remains
necessary to establish that two differently named actors are independent.

Metrics must match policy exactly. Conservative uncertainty intervals must satisfy
resource/value bounds, every regression budget and each required positive gain.
Exact rational comparisons of decoded numeric values avoid binary floating-point
subtraction and do not depend on ambient Decimal precision, rounding or traps.
Missing/conflicting evidence or failed gates returns experimental with no proposed
name or version increment. Passing evidence that does not classify the intended
capability returns unclassified. Internal versions stop at 4096. No training-run
count, flattering description or name change earns a version.

Names remain stable for an existing lineage. New lineages use the first available
word in the classified capability's ordered pool. Names collide case-insensitively;
accepted roster words are reserved for their corresponding roles. Empty pools,
invalid words and namespace conflicts fail validation; exhausted usable words block
a proposal. This prototype chooses 1–32 ASCII letters as a bounded implementation
constraint, not a product language requirement. A proposal does not reserve a name
or allocate a version; durable concurrent allocation is future work.

Results include exact input/policy/report commitments, baseline/candidate IDs,
measured labels, reasons and proposed internal version. `simulated=True` and
`promotion_committed`, `publication_authorized`, `live_capability_verified`,
`namespace_reserved` all false are explicit trust boundaries. No file or state is
modified. Eligible means only that this fixture satisfies its pinned fixture policy.

## Contributor fixture contract

`aethermesh_core.contributor_fixture.project_contributor_fixture` accepts version-1
JSON containing a request ID, fixture clock and freshness bound, namespace, explicit
routes and events. A route binds request, attempt, artifact and lineage. Events must
match that exact route, increase sequence contiguously, preserve timestamp order and
obey selected → queued/executing → terminal transitions (failure/cancellation may
occur earlier). Executing heartbeats are allowed. Terminal attempts cannot resume.

Only a fresh executing event contributes a deduplicated family name. Selected,
queued, missing, stale, completed, failed and cancelled attempts contribute none.
Unknown attempts, mismatched artifacts/requests, future events and duplicate or
out-of-order events are rejected. Event age applies only to simulated display
activity; it grants no ownership, reclamation or retention authority. Caller-created
events are not live evidence: `simulated=True`, `live_activity_verified=False`.
A future real adapter must consume authenticated actual routed participation and
cannot relabel these fixture outputs as live activity.

## Limits and unfinished integration

Both entrypoints accept at most 64 KiB of strict JSON (naming includes external
pin bytes in that bound), 16 nesting levels and reject duplicate keys/nonfinite
numbers/unknown fields. Naming allows 2 reports, 64 metrics, 32 taxonomy rules,
16 words per rule, 32 namespace entries, 16 evaluator labels and 64 external report
pins. Numeric metric magnitudes are at most 10^12; versions/revisions are bounded.
Contributor projection allows 32 routes and 64 events with a 1–3600 second fixture
freshness window. Cooperative cancellation is checked before and between bounded
work and before returning; there are no background workers or I/O.

Tests cover golden data, changed commitments, insufficient/regressed/uncertain or
resource-exceeding evidence, name stability/collisions, decimal boundaries, malformed
and oversized input, cancellation and contributor event bindings/transitions. The
installed-package acceptance also exercises both APIs with these same static files.
This is not measured model quality, training, small-device performance, distributed
promotion, independent identity, real contributor activity or autonomous operation.
Trainers/app orchestration remain external; publication and persistent trust still
require their own authority. Generic peer/storage use remains independent of AI.
