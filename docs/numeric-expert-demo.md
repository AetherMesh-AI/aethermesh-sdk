# Recoverable synthetic numeric-expert demo

This example trains a small, genuine regression model: six rational coefficients
for `a + b*x + c*y + d*x² + e*x*y + f*y²`. It learns from 16 synthetic training rows
using exact least squares. It is separate from the earlier three-weight router
pilot and is not a general model trainer, language model, production AER expert,
or network training service. No SDK or peer-transfer API changes are required.

The trainer, evaluator, and coordinator are trusted local example programs.
Separate processes do **not** establish independent operators or authenticated
evidence. The journal is one trusted local database; replaceable coordinators
sharing it are **not** replicated coordination, consensus, or decentralization.
No fixed peer identity is required, but losing this sole database loses recovery
state. The report explicitly marks both guarantees as false.

## Reproduce in a development checkout

Use the repository's development Python environment and an SDK build containing
`aethermesh_core.learning_state`. Keep `numeric_expert_demo.py` and
`numeric_expert_worker.py` adjacent. Installed-package validation executes the
example with `python -I` against the built wheel, outside the source checkout.

```sh
# Fresh disposable state; the job ID is a run identifier, not a credential.
state_dir="$(mktemp -d)"
job_id="$(python -c 'import secrets; print(secrets.token_hex(32))')"

# First process checkpoints after four training rows, then exits.
PYTHONPATH=src python examples/numeric_expert_demo.py \
  --directory "$state_dir/job" --job-id "$job_id" --create --steps 1

# A replacement coordinator claims a new term and resumes the same job.
PYTHONPATH=src python examples/numeric_expert_demo.py \
  --directory "$state_dir/job" --job-id "$job_id" --steps 4

# Explicit rollback is a new durable transition to the pinned baseline.
PYTHONPATH=src python examples/numeric_expert_demo.py \
  --directory "$state_dir/job" --job-id "$job_id" --rollback
```

The first output reports `training`, `processed: 4`. The second reports
`promoted`, the learned coefficients `3, 2, -1, 1, 1, 2`, held-out squared error
**250 → 0**, and **zero regressed cases**. The final output reports `rolled_back`
and the original baseline `3, 2, -1, 0, 0, 0`. Rational coefficients are encoded
as canonical `[numerator, denominator]` pairs; no pickle or executable model
format is accepted. “Active model” means this experiment's saved numeric model,
not activation in Eidolon, a router, or any production service.

`--steps 0` inspects after explicitly claiming coordination. `--cancel` writes a
terminal cancellation during training or evaluation. Interrupting an in-flight
helper instead leaves the last committed checkpoint resumable; it does not
implicitly write a terminal cancellation. A completed, rejected, cancelled, or
rolled-back job never starts another candidate automatically. Creating another
job is an explicit operator action with a fresh directory and run ID.

## Data flow and evaluation policy

1. The coordinator pins the project/run, training data, baseline, policy, suite,
   helper source, and coordinator/evaluator source to `LearningBinding` digests.
   On reopen it reconstructs these commitments from its locally installed code
   and rejects mismatches. A code/policy change requires an explicitly new job;
   this example provides no state migration. Hashes do not establish authority.
2. Each trainer invocation receives only the synthetic training rows and a
   data-only checkpoint. Four rows advance a six-by-six normal matrix and its
   right-hand side. After 16 rows the trainer solves that system to produce a
   candidate. The coordinator independently recomputes the expected training
   prefix statistics before committing each checkpoint. A test shifts the
   training targets and verifies that the learned intercept changes, ruling out
   a hard-coded answer.
3. The evaluator receives the candidate and eight feature pairs, with no labels.
   Its separately implemented polynomial calculation returns exact predictions.
   The task digest binds the run/policy/suite/evaluator, candidate, fixed round,
   and all feature rows. The coordinator checks the exact task/result shape and
   independently recomputes every prediction. Claimed scores, rewritten policy,
   wrong predictions, cross-job results, and extra fields are rejected.
4. Four held-out cases and four regression cases are disjoint from each other and
   all training feature pairs. Promotion requires held-out squared-error gain of
   at least 1 **and** no regression case whose squared error increases relative
   to the pinned baseline. Scores use exact rational arithmetic. A candidate
   that improves held-out error to 4 but regresses the baseline's perfect `(0,0)`
   case is rejected. A no-gain candidate is also rejected.
5. Candidate, checkpoint, verified result, decision, and replay tombstone commit
   atomically. Recovery rechecks the checkpoint and any saved decision/evidence
   against the frozen evaluator. A stale coordinator or late result cannot
   commit after takeover. Only a promoted candidate can roll back; rollback
   preserves the evidence and monotonic journal revision.

The suite is small, fixed, synthetic, and visible in source. With filesystem
access a helper could read it; withholding labels from stdin is a data-flow
boundary, not secrecy or a sandbox. Successful results demonstrate the lifecycle
and a fitted polynomial, not useful generalization on real AER workloads.
There are no cryptographic evaluator signatures, authority mapping, quorum,
Sybil defenses, independent training attestation, or publisher/release approval.

## Bounds and failure handling

- Fixed 16-row training set, four rows per checkpoint, six coefficients, one
  candidate, and eight evaluation rows. No automatic retry/search loop.
- Feature integers bounded to ±6; labels to ±1,000; input checkpoint integers and
  canonical coefficient numerators/denominators bounded to ±1,000,000 with
  positive denominators. The solver has a fixed six-by-six dimension.
- Each helper input/stdout is bounded to 16 KiB and stderr to 4 KiB. Pipe readers
  enforce limits while streaming. Each invocation has a five-second wall deadline
  and the fixed helper sets a two-second CPU limit. Cancellation, excess output,
  failure, and deadline paths kill/reap the helper. The helper spawns no children.
  This is not a general memory/process-tree/security sandbox.
- Each CLI invocation runs at most five steps, starts no listener or background
  service, and uses no credentials, sensitive data, network requests, or paid
  compute. It runs only the explicitly trusted adjacent helper with `python -I`;
  never pass received code as a helper or automatically execute downloaded models.
- The journal's 64-KiB state, 1-MiB checkpoint, lifetime revision/evidence limits,
  local POSIX filesystem assumptions, and uncertain-commit reconciliation rules
  still apply; see [durable learning state](durable-learning-state.md).
- Term/revision checks prevent stale commits, not wasted bounded work already
  running in another coordinator. There is no automatic leader election,
  replicated failover, retry, network evidence authentication, or external model
  activation transaction. Same-user filesystem/code tampering is outside scope.

Tests cover real helper execution, changed training labels, process restart and
crash recovery, malicious/misbound results, duplicate/cross-job replay, stale
coordinators, invalid checkpoints, regression/no-gain rejection, promotion,
rollback, durable cancellation, bounded pipes, deadlines, and child cleanup.
Additional journal tests use simultaneous **separate processes** for competing
claims/commits and a real SQLite lock timeout followed by successful recovery.
The lock test verifies the five-second production setting and uses a shortened
50-ms test-only wait. These tests do not establish Internet or multi-machine
operation, malicious-code containment, or independent evaluator identity.

Future service work should define a real task/data set and stronger evaluation,
then authenticated portable evidence and replication with explicit authority and
resource policy. Training participation, peer identity, project publisher rights,
and release channels must remain separate. This example adds none of those
permissions merely because a process can connect to a peer.
