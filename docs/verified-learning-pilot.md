# Verified tiny-router learning pilot

This is an **opt-in development experiment** connecting a small CPU-trained
router to real, explicitly trusted TLS peers. It does not deploy a public
network, train a large model, implement Byzantine consensus, or establish that
arbitrary models are safe. AER means Adaptive Expert Routing; this pilot learns
a two-way routing decision, not expert model weights. It exercises part of the
Router → Expert → Validator → Aggregator (REVA) direction without claiming a
complete expert network.

## What runs

- A separate example trainer receives a small training split and learns three
  integer perceptron parameters. The SDK does not contain a training engine.
- A coordinator quarantines those parameters under a fixed operator policy.
- Two separately running peer processes compute tiny prediction microtasks over
  mutually authenticated TLS. Their independently configured certificate pins
  map to distinct operator authorities.
- The coordinator verifies every prediction, scores a held-out split and an
  independent regression split, and promotes only an improving candidate with
  complete independent evidence and no newly broken regression case.
- Active routing changes only on promotion. An explicit local rollback restores
  the previous router. Rejected, cancelled and rollback decisions produce local
  content-addressed audit receipts.

See `examples/network_learning.py` and `tests/test_network_learning_integration.py`
for runnable evidence. This uses actual sockets and OS processes, not an
in-memory transport simulation. All example listeners bind to loopback. The
example's explicit ephemeral-PKI option creates short-lived **test-only** keys
inside a temporary directory and removes them on exit. Never use those identities
for deployment. The example writes a JSON outcome/receipt artifact to a path you
choose; its documentation and `--help` give the exact invocation.

For a source checkout with dependencies installed:

```sh
PYTHONPATH=src python examples/network_learning.py \
  --ephemeral-test-pki --output learning-result.json
```

For installed-package use, copy all three `examples/network_learning*.py` files
together outside the checkout and run the same command without `PYTHONPATH=src`.
The evaluator extension requires updated version-2 clients. Version-1 sessions
remain diagnostic-only, even on an evaluator-enabled service. Older version-2
clients that reject unknown capabilities need upgrading.

## Tiny-computer participation

The implemented contribution is a deterministic two-feature linear-router
prediction, with no GPU, model download, optional inference package or new
runtime dependency. A task contains only a SHA-256 context, three bounded integer
weights, a bounded feature array and a content-addressed task ID. There are no
URLs, Python objects, serialized executable models or dynamic imports.

Hosting requires explicitly constructing `RouterEvaluator(EvaluationBudget(...))`
and passing it as `PeerService(..., router_evaluator=evaluator)`. Merely importing
or connecting the SDK enables no contribution. Default peers remain diagnostic
status/optional echo services.

`EvaluationBudget` defaults to eight rows per request and 256 rows over that
evaluator object's lifetime. Hard ceilings are 32 rows per request and 4,096 total.
Each row has exactly two integer features and each model exactly three integer
parameters; every integer is in `[-1000, 1000]` and booleans are rejected. A
prediction needs two multiplications, two additions and a comparison. The outer
protocol still enforces its 65,536-byte frame limit, connection/request ceilings
and deadlines. This bounds work units and object counts, not a measured RSS, CPU
percentage, battery, thermal or wall-clock allocation.

Valid repeated requests consume their full row count. Reconnection and service
restart using the same evaluator cannot refill the budget. Creating a new
evaluator is a new explicit allowance; process restart does not preserve it.
Invalid requests do not reserve compute, but parsing/authentication still costs
resources. Budget exhaustion fails with `resource_limit`; it cannot trigger
training, billing or a larger allocation. The shared evaluator budget applies
across all admitted peers, so one trusted-but-abusive peer can exhaust it.

Cancel an outstanding `collect` asyncio task to use the SDK's best-effort request
cancellation. Call `GovernedRouter.cancel()` to abandon the candidate. Close the
service to stop contributing. These very small synchronous computations may
finish before cancellation arrives; already performed work cannot be undone.
There is no background schedule, automatic retry, hidden participation or claim
that every embedded device can run Python/TLS. Real low-end hardware latency,
RAM, energy and reliability measurements remain future work.

## Authority and decisions

Import policy types from `aethermesh_core.network.governance`. Configuration is
immutable and bounded. `GovernancePolicy.authorities` explicitly maps certificate
fingerprints to operator IDs. The quorum is at least two independently configured
non-producer operators **for every microtask**. Repeated sessions, certificates
mapped to the same operator, node names, hardware claims and contribution counts
grant no additional authority. The producer operator cannot approve its own
candidate. Operators must establish independence out of band; this package
cannot discover common ownership or prevent colluding trusted operators.

`GovernedRouter.collect` obtains the fingerprint from a live `PeerClient` session,
checks its project and configured authority, and correlates the response with the
exact content-addressed task. No public API imports a vote or score from an
untrusted receipt document. Protocol results allow only `task_id` and exact binary
predictions. The governor recomputes those predictions, rejecting lies even from
an authenticated peer. This intentional duplication is affordable for three
weights; it is not a scalable verifier for expensive training or large-model
inference.

Candidate IDs bind a fresh controller-run nonce, project, policy, suite, parent revision, epoch, bounded round
number, producer operator, numeric parameters and declared training-source/data
hashes. Task IDs additionally bind evaluator version, split and microbatch.
Old results cannot become evidence for a different task or round. Duplicate
operator evidence is rejected, including concurrent submissions and reconnects.
Malformed, missing, disconnected, cancelled or failing evaluations never count
as completed independent evidence.

`finish()` is a local decision, never a remotely callable promotion operation.
On a single event loop, it rechecks the parent revision and epoch without an intervening await, requires
complete quorum, requires the configured integer held-out gain, and rejects any
regression case that the old router got right and the candidate gets wrong.
Insufficient evidence rejects the candidate, rather than implicitly waiting or
lowering the quorum. The controller is not thread-safe; do not share it across
threads/event loops. A mean gain cannot excuse breaking a protected case.
`rollback(expected_active_id)` is an explicit local operator action, fails if a
candidate is pending or the revision is stale, and records the predecessor and
new epoch. Candidates have no path to rewrite authority, evaluation or resource
policy, fetch code, install software, or publish a release.

## Data and provenance boundaries

The trainer helper receives only training examples through stdin and does not
import the coordinator fixture. Peer evaluators receive evaluation **features**
but no expected labels. The coordinator retains trusted labels and controls all
quality decisions. Training/evaluation fixtures are synthetic; no private user
prompts or data are sent.

Separate processes demonstrate data-flow separation, not operating-system
sandboxing. The toy evaluation suite is inspectable in the source and every
process runs under the same account. A real use must keep evaluation answers
behind appropriate access controls, audit training/evaluation overlap, use
independent sources and fresh/rotating tests, and limit feedback. Repeated use of
a fixed holdout or revealing scores can overfit it. Suite hashes do not hide a
small guessable dataset. The SDK validates evaluation split disjointness but
cannot inspect an opaque training-data hash for overlap.

`TrainingProvenance` records locally asserted hashes of the trainer source and
training input. It does not sign or remotely attest them. The example controls
the trainer process and computes those hashes locally. A hash verifies identity
only against an independently trusted expectation; it does not grant publisher
or release authority.

Receipts are bounded, local audit records with canonical hashes. They include the
policy/suite commitments, baseline/candidate IDs, authority evidence and decision
metrics without raw answers. A hash is not a signature, and a saved receipt cannot
be replayed as a vote or prove its original TLS session to another machine.
The example persists the outcome, including rollback evidence. The SDK controller
is in-memory: process restart loses active state and history. Each new controller
gets a fresh random run identity, so prior-run task IDs/responses cannot be reused
as live evaluation evidence. Applications must initialize from a trusted baseline
and re-propose untrusted or saved candidates for fresh independent evaluation.
The constructor explicitly trusts its local `initial_weights` argument; it does
not verify that baseline's provenance or quality. Saved receipts cannot restore
authority or active state through the SDK. No receipt-import/restore path exists. The local hash
chain can detect accidental changes when checked against a trusted saved digest,
but an attacker can rewrite an unsigned chain, so it is not a recovery trust root.
There is no crash-safe durable state store, restore API, signed portable evidence,
append-only audit service or automatic recovery. Do not use this pilot for
unattended production governance.

## Threats this does not solve

- Backdoors outside the protected suite, unrepresentative labels, poisoned
  training data that passes those tests, or test-set overfitting
- Compromised coordinators, a dishonest human trust configuration, colluding
  operators or malicious endpoints that see legitimately shared features
- Public discovery, Sybil-resistant enrollment, NAT/relays, Internet-facing DoS,
  transport deployment hardening, certificate lifecycle or revocation
- Arbitrary artifact transfer, signed releases, updater rollback/freshness
  protection, self-modifying source or a general distributed training scheduler

Successful evaluation is evidence for **this router and these bounded tests**.
It is not universal bad-actor prevention or evidence of continual general AI
improvement. Relevant background includes
[model-poisoning backdoors](https://proceedings.mlr.press/v108/bagdasaryan20a.html),
[the Sybil problem](https://www.microsoft.com/en-us/research/publication/the-sybil-attack/),
[verification of provenance against expectations](https://slsa.dev/spec/v1.2/verifying-artifacts),
and [distinct-key thresholds and update rollback/freshness rules](https://theupdateframework.github.io/specification/v1.0.36/).
