# Durable local learning state

`aethermesh_core.learning_state` provides a first recovery foundation for a
separate learning service. It stores one job's complete opaque state and
checkpoint atomically, binds the job to operator-selected policy/suite/evaluator
commitments, and fences a replaced local coordinator. It performs no training,
model loading, remote execution, network listening, peer election, or promotion.
Existing peer transfer and the in-memory tiny-router pilot are unchanged.

This is a local durability primitive, not replicated state, signed evidence,
public consensus, or a generally trained AER expert. The three-weight pilot is
still a router experiment. A real expert trainer and its evaluation policy belong
in their own service or explicitly isolated example, not this SDK storage module.

## API and ownership

Import `LearningBinding`, `LearningJournal`, `LearningSnapshot`, and
`LearningConflict` from `aethermesh_core.learning_state`.

- `LearningBinding(project, job_id, policy_sha256, suite_sha256,
  evaluator_sha256)` is immutable. All four digests are lowercase SHA-256. Use a
  fresh job identifier per independent run; preserve it on recovery. Digests
  commit to bytes, not publisher authority. The service must separately supply
  the actual frozen policy, suite, and evaluator and verify those commitments.
- `LearningJournal.create(absolute_directory, binding, state=bytes,
  checkpoint=bytes)` exclusively creates a private directory and initial state.
  Its initial revision/term are zero. This initial state is supplied by the
  trusted service; the store does not semantically validate it.
- `LearningJournal(absolute_directory, expected_binding)` reopens only a matching
  version-1 job. `snapshot()` returns immutable revision, term, state, checkpoint,
  and computed `state_sha256`/`checkpoint_sha256` properties. Recovery checks the
  stored content digests. Treat the returned bytes as data, never executable
  code, pickle, a module, or instructions to change policy.
- `claim(expected_revision=...)` explicitly takes over coordination, increments
  revision and term, and returns a snapshot. It preserves the job/checkpoint.
  A stale revision fails. It has no permanent peer ID, lease, clock dependency,
  automatic takeover, or distributed election.
- `commit(term=..., expected_revision=..., state=bytes, checkpoint=bytes,
  evidence=tuple_of_digests, validate=callback)` atomically replaces the complete
  state/checkpoint and records evidence identifiers. The callback receives
  `(previous_snapshot, proposed_state, proposed_checkpoint)` and must raise to
  reject a transition. No default validator is provided. It runs while the write
  transaction is held: keep it bounded, synchronous, side-effect-free, and never
  reenter the store. Perform network collection before the transaction.
- `close()` releases the connection. Connections are thread-confined. Use
  `try/finally`; do not share one connection between concurrent worker threads.

The service owns job/candidate IDs, training progress, source/data provenance,
active/previous model references, evidence payloads, cancellation state, and
promotion/rollback history inside the opaque state/checkpoint. Persist enough to
recover those semantics; this module does not infer them. A checkpoint can be
empty and must use a data-only format the trusted service explicitly supports.
External references alone do not make model artifacts durable. Small checkpoints
can be stored directly; larger artifacts need a separate durable store and a
service-level commit protocol.

A coordinator with the current term can commit only against the current revision.
A replacement must reopen, verify its locally supplied policy/suite/evaluator,
claim against the observed revision, and recover the saved job. Old terms fail
even if an old process learns the latest revision. These tokens prevent accidental
stale writers; filesystem-authorized code can deliberately claim again. They are
not credentials or protection against a hostile process running as the same user.

## Evidence and promotion boundary

The callback must independently recompute/check the result against the candidate,
task, split, job, and frozen evaluator; authenticate the evidence origin if that
is part of policy; enforce current candidate/round bindings; and enforce held-out
improvement plus regression limits before changing the active model. Candidate
content must never select or rewrite the evaluator, suite, trust roots, release
channel, or promotion policy. Network authentication is separate from project
publisher authorization and evaluator independence.

Evidence identifiers are retained for the lifetime of this job, including after
restart, cancellation, or model rollback. A duplicate digest anywhere in the job
is rejected. The service must derive identifiers from a canonical bound task and
result, and reject semantically replayed results with different encodings/IDs.
This is exact-ID deduplication, not authentication or a proof of correct results.
Empty evidence permits ordinary training checkpoints. Failed callbacks and
failed transactions leave no new evidence tombstones or partial checkpoint.

Rollback is a new validated state commit to a previously approved model. Revision
and term never move backward. No model is activated by the SDK. A rollout that
also mutates a serving process needs its own idempotent activation protocol;
SQLite cannot transact with an external model host.

## Bounds, durability, and failures

- State: at most 64 KiB; checkpoint: at most 1 MiB; exact immutable `bytes` only.
- At most 32 evidence IDs per commit and 1,024 across the job.
- At most 4,096 total revisions, including coordinator claims. Reopening cannot
  reset these limits. Reaching a limit requires an explicit new job and service
  policy decision; there is no implicit garbage collection or rollover.
- SQLite page size 4 KiB, database limit 4 MiB, DELETE rollback journal, synchronous
  EXTRA, bounded five-second lock wait. The rollback journal can require additional
  temporary disk space up to the database size. This is not a CPU/memory sandbox.
- Current implementation requires POSIX file ownership, directory fsync and local
  SQLite locking semantics. Use a private directory in trusted, stable local
  parents; do not use a network filesystem or open a received SQLite database.
  Symlinked paths, shared permissions, hard-linked database files, unexpected
  directory entries, and oversized database/journal files are rejected. These
  checks do not defeat malicious same-user path races.
- SQLite atomically commits state, checkpoint, and evidence tombstones. New
  directory entries are also fsynced. Crash recovery is subject to the filesystem
  and hardware honoring synchronization; no storage primitive guarantees against
  arbitrary disk corruption or a malicious operator rewriting the database.
- Stale terms/revisions and duplicate evidence raise `LearningConflict` with no
  commit. Invalid bindings, versions, limits, or payloads raise `ValueError`.
  OS/SQLite failures and callback exceptions propagate. Creation can leave a
  private incomplete directory; never automatically overwrite it.
- A process crash or I/O failure at the commit boundary can leave the caller unsure
  whether a commit completed. Reopen and reconcile the revision and content
  digests before retrying. Do not treat an exception as proof that nothing landed.
  This synchronous API has no interruptible disk-I/O deadline; cancellation of an
  async caller does not prove rollback of work dispatched to another thread.

## Acceptance evidence and next slices

`tests/test_learning_state.py` covers process death after SQL writes and before
commit, clean restart, policy/suite/evaluator/version mismatch, stale takeover,
compare-and-swap conflicts, durable duplicate rejection after logical rollback,
callback rejection/interruption, corruption, permissions, and lifetime limits.
The installed-wheel smoke also creates, closes, reopens, claims, and rejects an
old coordinator from outside the source checkout. These are local storage tests,
not two-machine, Internet, power-loss, independent-operator, or training evidence.

The next separately reviewable slice is a small real numeric-expert trainer outside
SDK code, with a data-only checkpoint, fixed disjoint training/held-out/regression
sets, independent result recomputation, malicious/replayed-result tests, restart
and coordinator replacement, and measured promotion plus rollback. Its policy
must remain frozen and its limits explicit. Later work can add authenticated
portable evidence and signed replicated state, scoped authorization/Sybil defenses,
and separate build/review/release services. None is implied by this journal.
