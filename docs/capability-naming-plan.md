# Capability naming and evidence-based promotion plan

**Status: initial names and intended roles accepted; implementations and learned
capabilities remain planned.** This adds a staged AI-participation task alongside
current peer/storage work. The roster is not eight trained models already available.
It does not change repository identity, transport trust, project publisher rules,
generic SDK onboarding or resource ceilings. Trainers and application orchestration
remain outside the SDK. Storage work continues independently.

## Accepted initial family roster

Public display uses only the single-word name. These responsibilities belong in
internal profiles or details and are intended roles, not demonstrated capabilities.

| Name | Internal intended responsibility |
| --- | --- |
| Compass | Route requests to eligible experts under deterministic policy. |
| Loom | Plan and coordinate work in the application/service layer. |
| Tempo | Forecast resource use and propose scheduling within fixed budgets. |
| Ash | Initial Python specialist. |
| Ember | Initial TypeScript specialist. |
| Lens | Capability evaluation and naming proposals, subject to independent verification. |
| Grove | Data preparation and training-experiment proposals within provenance/privacy policy. |
| Vigil | Anomaly/regression monitoring and recovery proposals within existing authority. |

Start with deterministic components where appropriate. Learned replacements must
outperform the declared baseline under independent evaluation and regression/resource
gates before authorized promotion. A role name does not confer credentials, execution
permission, evaluator authority or a right to change its own limits. In particular,
Lens cannot certify its own gains and Vigil cannot manufacture lost ownership evidence.
The accepted full hands-off goal and staged acceptance criteria are below.

## Separate identity, description and compatibility

An immutable artifact/checkpoint ID identifies exact bytes. Preserve parent/base
artifact IDs, training job/checkpoint lineage, intended task, input/output schema,
data/provenance commitments and declared training configuration. An intended task
is a claim to evaluate, not proof that the artifact can perform it. Content hashes
provide identity/integrity, never publisher authorization by themselves.

Measured capability reports bind the exact artifact, baseline artifact,
versioned evaluation suite/taxonomy, metric definitions and direction, test-data
commitments, evaluator provenance, resource conditions, uncertainty and regression
results. Keep private datasets protected; publishing provenance need not publish
raw data. A candidate's self-description and its trainer's assertions cannot be
the independent evaluation evidence used for promotion.

Keep these namespaces separate:

- Immutable artifact/checkpoint identity and lineage.
- Internal versioned capability taxonomy and a single-word public family name.
- Internal capability version awarded by deterministic evidence policy.
- Model input/output schema, API/protocol compatibility and project release channel.

A new training run, checkpoint or display-name edit need not earn a new capability
version. A compatibility break cannot be hidden by a measured capability gain.
The public display is a single-word contributor name **only**, with no version
number beside it and no required model picker. The router selects eligible experts
for each request. Capability descriptions, schemas, lineage, versions and exact
checkpoints stay internal or in technical details. A message such as “Ash and Ember
are working on this request” illustrates name-only participation. It is only
truthful when those accepted families actually execute that request; the roster
alone is not evidence of execution or trained availability. Internal version syntax
and future-family naming policy remain undecided. Names are not taxonomy IDs or
permissions.

## Autonomous proposals within deterministic policy

Start with an explicit evaluation taxonomy and deterministic report-to-label
rules. Labels describe demonstrated tasks at stated evidence levels. Permit
`experimental` and `unclassified` outcomes when evidence is insufficient,
inconsistent or outside the taxonomy; never invent a stronger label to avoid an
unclassified result. A model may have several independently supported capabilities.
Do not equate one classifier label with comprehensive competence or safety.

The autonomous namer may propose a suitable single word from the verified profile
and internal capability labels. Once a family name is accepted, preserve it across
that lineage; new reports or training runs do not silently rename the family.
Treat forks, ambiguous family continuity and name collisions as explicit policy
outcomes. Normalize candidate names consistently and check a caller-selected
namespace; do not claim global uniqueness from a local lookup. Preserve the accepted
initial roster above; earlier alternatives do not override those assignments.

A later optional small classifier/namer may learn from versioned report-to-label
pairs bootstrapped from those explicit rules and independently checked examples.
Keep held-out report families/lineages separate to measure label accuracy,
calibration and abstention. The classifier/namer proposes internal labels and
single-word names only; it cannot change the taxonomy, metric thresholds, permissions, accepted evaluator set or promotion
policy. Its own description of a candidate is not promotion evidence. Namer
training is a separately budgeted external task, not part of the first prototype.

A deterministic promotion policy consumes independently verified, comparable
baseline/candidate reports. Require predeclared gain thresholds, regression
ceilings and resource constraints for the selected capability. Handle uncertainty
and missing measurements conservatively. Emit an explicit decision and reasons
bound to artifact IDs, report IDs, taxonomy, evaluation version and policy version.
Never use training-run count, wall-clock age or marketing/self-description as a
version-increment signal. Failed/regressed/insufficient evidence leaves the internal
capability version unchanged while the immutable experimental artifact remains
addressable. A revoked or rolled-back capability mapping must not rewrite history.

If the evaluation suite, metric meaning, task schema or taxonomy changes, rerun
the baseline and candidate under the new conditions before comparing. Do not
compare unlike scores or silently carry an old label into a new taxonomy. Define
compatibility/migration separately. Independent evaluator authority and public
publisher authorization remain explicit future design boundaries; this plan does
not appoint a mandatory company evaluator or issue credentials.

## Truthful routed-contributor status

The SDK may eventually supply structured contribution events for an application to
render as names, but this plan adds no UI, model picker or execution scheduler.
Map each name internally to the actual routed request, attempt and exact artifact.
Keep selection, queued, executing, completed, failed and cancelled states distinct.
Do not call an expert “working” merely because it was proposed, is available, or
appears in a capability catalog. Require evidence of actual execution for that
request before showing active contribution, and stop claiming activity after a
terminal event or when the evidence is unknown/stale under the defined policy.
Several replicas of one family must not be presented as independent expert
identities or independent quality consensus. The router chooses the experts;
name display does not give users an exact-checkpoint choice or grant model trust.

The smallest prototype may validate bounded synthetic routing-event fixtures and
name mappings to test these semantics. Fixture outputs must be explicitly local
and simulated, never shown as real live network activity. Authentic remote event
provenance, ordering, freshness and transport behavior need a separate contract
before a service or UI claims real participation.

## Smallest fixture-driven prototype

Build a pure local SDK decision utility with bounded structured inputs and golden
fixtures, not a training job or autonomous release publisher. Use at most 32
capabilities, 64 metrics per report and 64 KiB total encoded inputs in the initial fixture
contract; these are proposed prototype ceilings, not changes to storage/network
limits. Reject oversized/nonfinite/malformed inputs before classification. Freeze
canonical identifiers and explicit comparison directions; do not parse authority
or thresholds from free-form model prose.

The caller supplies two synthetic artifact/lineage records, independently selected
baseline/candidate fixture reports, the explicit versioned taxonomy/rules and a
promotion policy. Local fixture selection stands in for externally verified
provenance only inside the test harness. The result must mark itself local,
experimental and not publication-authorized; it is not a remote-verification or
independent-evaluator implementation.

First implement rule-based internal label proposals, a bounded single-word family
name proposal and deterministic promotion eligibility with blocked reasons.
Use a small reserved test vocabulary and a caller-supplied fixture namespace map.
For this initial prototype only, propose 1–32 ASCII-letter words with case-insensitive
collision comparison; broader language/confusable-name policy remains undecided.
A collision with another lineage must select another available test word or return
blocked, never overwrite the mapping, append a task description or claim a global
reservation. A known lineage retains its family name. The pure result proposes a
name but does not register it or allocate public namespace authority. Use an
abstract unchanged/eligible internal version transition in fixtures while internal version syntax remains undecided; never append it to a
public contributor name. Record immutable input and policy/report commitments so the same inputs reproduce the same decision.
Do not mutate artifacts, allocate an internal accepted version, contact peers,
call providers, train a classifier, create credentials, publish a release or execute model code.

Acceptance fixtures must cover:

- Public contributor output contains single-word names only: no version suffix,
  task-description name or required model picker. Versions remain in details.
- Contributor names/status derive from actual bound routing/execution events.
  Missing, merely selected, queued, stale, duplicate, out-of-order, failed and
  cancelled events cannot fabricate an active worker; fixture activity stays
  explicitly simulated. Internal request/attempt/artifact binding is preserved.
- Exact same inputs yield the same internal label, single-word name proposal and
  decision despite run-count or prose changes. Public names contain no task phrase.
- Existing lineage keeps its family name; another lineage cannot take it. Case
  collisions, duplicate namespace entries, exhausted test words, reserved/invalid
  words and ambiguous forks yield deterministic alternatives or blocked reasons.
- Verified gain within every regression/resource limit is eligible; a regression,
  missing metric, excessive uncertainty or exceeded budget blocks promotion.
- Wrong artifact/base lineage, changed schema, mismatched suite/taxonomy/policy,
  stale baseline and unverified evaluator provenance cannot promote.
- Out-of-taxonomy or inadequate reports remain experimental/unclassified.
- Multiple capabilities are assessed separately; duplicates, ambiguous labels,
  malformed/nonfinite values and declared size bounds fail deterministically.
- An evaluation change requires a newly comparable baseline/candidate pair.
- Internal capability version, artifact identity and API compatibility remain
  distinct; rejected candidates do not erase accepted history or consume accepted
  internal versions.
- Rollback/revocation decisions preserve immutable evidence and require separate
  publication authority. No fixture result authorizes release or deployment.

The next stage, only after this contract is reviewed, is optional tiny namer
training on synthetic/approved report-label pairs with measured held-out accuracy,
abstention, resource use and comparison against the deterministic baseline. Useful
CPU/ARM expert training, actual independent evaluation, durable internal promotion
state and network distribution remain separate milestones with their own evidence.

## Full hands-off goal and staged acceptance

The accepted goal is sustained unattended useful work and improvement within
standing operator authority: route eligible requests, plan/coordinate them, apply
resource budgets, perform useful work, propose and run permitted training,
independently evaluate candidates, promote only when authorized policy passes,
monitor results, recover supported faults and roll back regressions. This is a
goal, not current deployed functionality or permission to run an unbounded job.

Before an unattended pilot, predeclare the duration, useful task suite and quality
thresholds, permitted data/tools/training, device/resource budgets, failure matrix,
allowable deferrals and evidence collection. Acceptance requires zero manual
interventions for eligible routine work over that window, independently checked
useful outputs, reproducible promotion decisions and traceable contributor events.
Do not count simulated work, decorative activity, silent abandonment or endlessly
retried tasks as useful completion. Report completion/deferral/failure rates,
latency, resource use and recovery/rollback times against the declared baseline.

Progress in explicit stages:

1. **Deterministic fixture loop.** Use a bounded synthetic task queue, stubbed
   experts/trainers and declared failure events to prove routing, budget enforcement,
   checkpointing, evidence gates, deferral and continued eligible work. This tests
   control semantics only; it is neither useful trained capability nor live activity.
2. **Useful local supervised pilot, then unattended replay.** Integrate one narrow
   useful external expert/trainer under existing permissions. Measure real outputs
   and resources, independent held-out/regression evaluation, authorized promotion,
   restart and rollback. Then rerun the declared workload unattended within those
   same limits. Introduce additional roster components only with evidence.
3. **Independent-operator fault pilot.** Under separate environment authorization,
   exercise supported worker/storage/network faults and remove original project
   domains, gateways, evaluators and company recovery services. Eligible work must
   continue through independently operated alternatives; required authority cannot
   be silently replaced or centralized to make the test pass. Publish unresolved
   dependency failures and measured resource/recovery behavior.

Unsafe, unverified or ambiguous tasks preserve checkpoints, evidence and charges,
defer with explicit reasons, and allow other eligible work to proceed within the
remaining shared budgets. Exhausted or uncertain capacity is not reusable capacity.
Lost ownership bindings still hit their separate deferred restoration gate; the
hands-off goal does not promise universal recovery or authorize marker regeneration.
Missing permissions or credentials, new spending, real exposure, publisher authority
and irreversible actions remain governed by explicit standing authorization. No
component may expand those boundaries to avoid a deferral or improve its score.

Acceptance must inject interrupted training/checkpoints, corrupt or incompatible
updates, evaluator outage, rejected promotion, post-promotion regression, resource
exhaustion and uncertain recovery. Verify supported progress restoration, no duplicate
side effects, independently checked rollback, honest contributor status, preserved
failed evidence and continued unrelated eligible work. A bounded fixture or pilot
establishes only its tested envelope; it does not prove all faults recoverable,
Internet operation, useful small-ARM training or production autonomy.
