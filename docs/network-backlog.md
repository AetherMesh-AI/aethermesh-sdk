# Network and participation backlog

This is the staged task backlog for AetherMesh SDK (repository
`AetherMesh-AI/aethermesh-sdk`, historically `aethermesh-core`). It extends the
[persistent goal](persistent-goal.md) and [SDK scope](sdk-scope.md); it does not
rename the repository or authorize deployment. AER means Adaptive Expert Routing.

## Status and ownership

Snapshot: 2026-10-12, main `96c6a3ab67f9db11e8ff96e3d4022d62fa300162`.
Implemented foundations include installed-package authenticated direct peers,
project-scoped bounded opaque content exchange, explicit publisher verification
boundaries, local learning experiments, a bounded 4+2 artifact codec and placement
simulation, selected-file verification, durable local admission, directory
ownership, payload ingestion, explicit verified recovery and failed-staging
quarantine. The artifact codec currently caps originals at 8 MiB. These are not
public discovery, durable network storage, useful general model training or
Internet deployment evidence. Admission and quarantined files remain charged;
local audits and successful uploads do not prove future retention or authorize
reclamation. Do not mark dependent tasks complete merely because a prototype,
draft or interface exists.

Every unchecked task below is **proposed**, including items with prototype
precursors. Completion requires a linked reviewed change and acceptance evidence.
SDK work covers contracts, verification, bounded utilities and conformance tests.
Training engines, fleet UI, production relay/storage operations, full scheduling,
wallet applications and chain operation have separate owning services/projects.
No Eidolon checkout or app integration changes are included in this backlog.

## Stage 1 — Peer usability, storage and independent operation

- [ ] **NET-1: Discovery and relay fallback.** Specify replaceable discovery,
  direct-connect and relay interfaces with authenticated identity continuity,
  protocol compatibility, bounded retry/cancellation and explicit operator trust.
  Accept only after installed packages connect on two independent machines,
  demonstrate direct failure plus relay fallback, and reject malicious discovery
  records, wrong identities and incompatible versions. Loopback remains the safe
  default fixture; real exposure requires separate authorization.
- [ ] **NET-2: Repository and artifact exchange.** Build bounded project-scoped
  source/release exchange over the content foundation. Keep SDK, schema, project
  release versions and channels distinct; publisher authorization must be checked
  separately from transport identity and content hashes. Accept tamper, replay,
  rollback, wrong-project, cancellation and quota tests plus an installed-package
  exchange walkthrough. Never automatically run received code or builds; retain
  GitHub as an operational validation/recovery path during transition.
- [ ] **STORE-1: Dataset/model packaging and storage.** Specify versioned manifests,
  integrity and privacy policy, approximately 5 MB erasure fragments, bounded
  aggregate admission, repair, retention and safe reclamation. Advance beyond the
  current 8 MiB prototype only after measured memory/disk/repair budgets and
  crash/restart/concurrency/corruption tests. Prove reconstruction within the
  declared failure tolerance and fail closed when it is exceeded. Warm runnable
  expert replicas are separate from storage shards; a 5 MB shard is not a training
  RAM requirement. Local metadata or a successful upload is not retention proof.
- [ ] **NET-3: Operator-disappearance resilience.** Define a repeatable test that
  removes original project domains, bootstrap servers, gateways, evaluators,
  storage providers and company recovery keys. Independent operators must still
  discover/connect, verify artifacts and applicable state, recover, update and
  eventually train through separately operated alternatives. Record each missing
  capability as a failure rather than hiding it behind a mandatory gateway.
  Distinguish client verification, light-client proofs and full validation; rerun
  the scenario as later stages land. Depends on NET-1/2 and STORE-1.

## Stage 2 — Optional AI participation and owner-linked fleets

- [ ] **AI-1: Account creation, linking and recovery.** Design create/link/recover
  flows only after explicit AetherMesh AI participation enablement. Generic SDK
  connectivity, opaque exchange and peer updates must work without an AI account
  or wallet. Accept independent installed-package tests for both paths, including
  declined onboarding and recovery without the original project operator.
- [ ] **AI-2: Multi-node permissions.** Model an owner-linked fleet with distinct
  device keys, least-privilege permissions, explicit enrollment, revocation and
  recovery. Separate node transport identity, AI owner authority, project
  publisher authority and release channels. Accept lost/stolen/offline device,
  stale authorization, revoked key and recovery abuse tests. Never replicate an
  owner master key to every node. SDK supplies contracts; apps own onboarding UI.

## Stage 3 — Measured learning and safe execution controls

- [ ] **LEARN-1: Useful small CPU/ARM experts.** Select a narrow useful expert task
  and external trainer, configurable model size and contribution limits. Measure
  real small-device peak RAM, thermal behavior, throughput, request latency,
  cancellation and checkpoint latency against a deterministic baseline. Publish
  device/workload/version details and regression evidence. A desktop arm64 run or
  synthetic polynomial demo does not satisfy this task.
- [ ] **LEARN-2: Compatible distributed learning.** Prefer eligible nodes already
  holding/running the exact base model. Bind updates to base artifact/version,
  job, schema and data/privacy eligibility. Distinguish data-parallel/federated
  learning from model partitioning and inference replicas. Accept incompatible,
  stale, poisoned and interrupted update tests, independent held-out/regression
  evaluation, verified promotion and rollback. Training engines remain external.
- [ ] **LEARN-3: Capability naming and evidence-based promotion.** Preserve immutable
  artifact/checkpoint IDs and lineage; bind intended tasks, schemas and provenance
  to versioned measured capability reports. Bootstrap report-to-label proposals
  from an explicit evaluation taxonomy/rules, with an optional later small namer
  trained on approved report-label pairs. Public display is a single-word
  contributor name only, without a version suffix. There is no model picker; the
  router selects experts; capability labels, schemas, lineage, versions and exact
  checkpoints stay internal or in details. Name proposals use verified profiles,
  collision handling and stable lineage. Contributor status must reflect actual
  routed participation, never decorative or fabricated activity. Deterministic
  policy awards internal capability versions from independently verified gains and regression/resource
  limits, never training-run count or candidate self-description. Keep API/model
  compatibility versions separate. Permit experimental/unclassified outcomes and
  rerun comparable baselines when evaluation or taxonomy changes. Start with the
  bounded [fixture-driven decision prototype](capability-naming-plan.md), not a
  training job or release publisher. Record the accepted initial roster: Compass,
  Loom, Tempo, Ash, Ember, Lens, Grove and Vigil, with intended internal roles in
  that plan. These are not eight trained available models. Internal version syntax
  remains undecided. Depends on LEARN-2 evidence and CTRL-1 limits for later deployment.
- [ ] **CTRL-1: Deterministic enforcement first.** Enforce resource, permission,
  privacy and spending ceilings outside candidate model control. Define safe
  degraded operation, bounded queues, cancellation and deferral of ineligible jobs
  while other work continues. Accept adversarial requests and restart/failure
  tests proving no permission or budget escalation. Depends on relevant AI-2 and
  STORE-1 contracts; generic networking retains its independent controls.
- [ ] **CTRL-2: Learned scheduling controller.** Evaluate a controller that proposes
  versioned scheduling/resource actions within CTRL-1 limits. Compare with the
  deterministic baseline using independent evaluation, rollback and circuit
  breakers. Accept bad-policy, missing-evaluator and degraded-network trials.
  The controller cannot grant itself trust, credentials, spending or larger
  resource ceilings; app orchestration/full scheduling remain external.

## Stage 4 — Demand-aware expert availability

- [ ] **REPL-1: Warm expert placement.** Specify demand/capacity-aware warm replicas
  across independently assessed operator and failure domains. Percentage targets
  are configurable design decisions requiring measurement, not a fixed network
  promise. Accept demand spikes, churn, correlated outages and cold-start tests
  with explicit memory, bandwidth and energy budgets. Reuse STORE-1 for artifacts;
  shard redundancy does not satisfy warm availability.
- [ ] **REPL-2: Bounded request hedging.** Evaluate configurable one-to-three request
  replicas, delayed hedging, privacy eligibility, hard total budgets and prompt
  cancellation of unnecessary work. Measure tail latency, reliability and waste
  against one replica. Accept only responses meeting request validation policy;
  first response alone is insufficient and same-model replicas are not independent
  quality consensus. Prove tool calls, payments and other side effects execute
  once under separate authorization/idempotency despite retries and hedges.
  Depends on CTRL-1 and LEARN-2 validation contracts.

## Stage 5 — Chain, economics and wallet evaluation

These are research and design tasks. No chain choice, token issuance, wallet/key
creation, infrastructure deployment, spending or legal conclusion is authorized
by this document. Threat modeling and independent economic, legal and security
review must precede issuance or spend; record approvals and unresolved risks.

- [ ] **ECON-1: Dedicated chain/fork evaluation.** Compare a dedicated chain/fork
  against alternatives for verification, operator independence, security,
  maintenance and costs. EVM compatibility is a candidate, not a selected design.
  Accept a documented decision with adversarial simulations and independent
  review, including recovery when the originating team disappears.
- [ ] **ECON-2: Incentives and continuity.** Specify Sybil-resistant stake rules and
  verified-work rewards funded by paying non-node users. Model fake work, collusion,
  identity splitting, evaluator capture, low demand and treasury depletion.
  Define a bounded continuity treasury with transparent authority and spending
  limits. Accept independently reviewed economics and security evidence before
  issuance, collection or expenditure; no speculative reward guarantee.
- [ ] **ECON-3: Governance and mint lifecycle.** Evaluate AI-assisted proposals,
  governance and mint lifecycle under deterministic authority/supply constraints,
  independent verification, auditability, rollback where feasible and emergency
  bounds. Prove the controller cannot manufacture voting power through new
  identities, circular rewards, minted stake or self-approved work. Define who
  can authorize irreversible changes before any implementation or launch.
- [ ] **WALLET-1: Interoperability and authority separation.** Evaluate MetaMask and
  Trust Wallet compatibility for candidate protocols and an optional native wallet
  interface. Document supported signing/network flows with test fixtures before
  selecting a chain. Separate validator/node authority from withdrawal authority,
  enforce account-wide budgets across concurrent devices, and test revocation,
  replay and recovery. No master-key replication or real wallet provisioning.
- [ ] **WALLET-2: Independent access and verification.** Specify independently hosted
  RPC and explorer roles with replaceable providers and client verification of
  relied-on state; distinguish light clients from validators. Accept dishonest,
  stale, unavailable and conflicting-provider tests and a project-gateway removal
  drill. Neither the project RPC nor explorer may become mandatory authority.
  Depends on ECON-1 and NET-3; wallet support never gates generic SDK connectivity.

## Cross-stage goal — Full hands-off operation

- [ ] **AUTO-1: Sustained unattended useful work and improvement.** Aim for the
  complete bounded loop: routing, planning, resource forecasting/enforcement,
  useful execution, permitted training, independent evaluation, authorized
  promotion, monitoring, supported fault recovery and rollback. Start deterministic
  components where appropriate; learned replacements must earn promotion against
  measured baselines. Follow the [staged acceptance plan](capability-naming-plan.md#full-hands-off-goal-and-staged-acceptance)
  with predeclared workload/duration/quality/resource thresholds, zero manual
  interventions for eligible routine work and honest completion/deferral evidence.
  Test operator disappearance through independent alternatives without replacing
  missing authority. Unsafe/ambiguous work preserves progress/evidence/charges and
  defers while other eligible work continues inside remaining budgets. No universal
  recovery promise, marker regeneration, trust/credential/spending override or
  unapproved deployment follows from this goal. Application orchestration, trainers
  and production operations stay outside the SDK. Depends on the applicable
  NET/STORE/LEARN/CTRL milestones; this item is not marked complete by naming models.

## Evidence and sequencing rule

Start with NET-1/2 and the reviewed storage slices; keep optional AI, replica and
chain work separately scoped. Each feature PR records exact head/tree, installed
package evidence, applicable checks, trust boundaries and limitations. Independent
review precedes merge. Deployment and real device/network experiments need their
own explicit authorization and evidence; passing local CI does not prove them.
