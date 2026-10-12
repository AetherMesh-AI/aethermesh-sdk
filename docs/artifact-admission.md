# Durable local admission prototype

This bounded local utility records aggregate admission decisions in one private
SQLite database shared by cooperating processes. It is not a retained-object
service, physical-space reservation, remote receipt, identity service or proof
that any file still exists. It has no account/wallet, project-server, gateway,
provider or company recovery-key dependency.

`ArtifactAdmission.create(directory, store_id=..., limits=AdmissionLimits(...))`
exclusively creates a caller-selected physical directory (0700) and database
(0600). `store_id` is a caller-selected public namespace commitment, not a key or
credential. Reopening pins that identity and the same frozen policy. All
participants in one resource scope must use the same ledger; separate ledgers
cannot enforce an aggregate node budget. Limits must represent capacity explicitly
assigned to this scope after other storage uses; the ledger does not discover or
subtract unrelated files. Use a local filesystem with reliable SQLite locking;
network-filesystem or distributed coordination is not validated. Never open a peer-supplied SQLite file.
Directories must be trusted and stable; this is not a hostile same-user sandbox.

Explicit ceilings are 64 MiB of charged logical fragment bytes, 192 records and
4,096 lifetime committed revisions. SQLite uses 4 KiB pages, a 4 MiB database
ceiling, DELETE rollback journals, synchronous EXTRA and a five-second busy
limit. These are local filesystem-dependent durability settings, not a
power-loss/hardware guarantee. Connections are synchronous, thread-confined and
must be closed. Creation failures can leave an incomplete private directory
requiring inspection, never automatic overwrite.

`reserve` binds a request ID to the project, caller-pinned artifact root,
fragment index, exact size, content digest and unchanged transfer-manifest
digest. A BEGIN IMMEDIATE transaction reads the committed inventory, applies
aggregate byte/object limits, and records the reservation atomically. Exact
request retries are idempotent; another binding with the same ID fails. Different
IDs charge separately even for identical content: deduplication is not assumed.
Checksums bind each record's fields, state and revision so accidental valid-type
corruption cannot silently reduce a charge. They are not authentication against
a local operator who can edit the database and recompute checksums.

`record_verification` accepts a matching local `FragmentFileVerification` and
records `verified_observation`. The trusted caller must obtain that observation
fresh from its selected file. This is unsigned local metadata, not authentication
of a remote operator or a future retention claim. `invalidate` advances the
record revision and returns it to `reserved`. Stale metadata updates fail; this
does not stop or fence unrelated filesystem writers.

Every record remains charged across restart, invalidation, cancellation and
abandonment. There is deliberately no release, automatic expiry, deletion or
reclamation API in this first slice. Safe reclamation needs proof that writers
have stopped and selected files have been reconciled/removed; a timeout alone is
insufficient. Ledger exhaustion requires explicit future migration/reconciliation,
not silently replacing the database to bypass a standing resource limit.

The existing learning journal and this ledger share a small private SQLite
file/configuration helper. Their schemas and semantics remain separate; the
artifact ledger does not depend on AI-account or learning-job onboarding.

Current prototype evidence includes independent-process competing reservations,
process exits immediately before COMMIT and after a committed operation returns,
idempotent recovery, revision fencing, policy binding, conservative charges and
private/exclusive file setup. Process-crash tests do not establish power-loss or
multi-machine durability. Full repository/installed-package validation and
independent review are required before publication or integration with hosting.

## Read-only restart reconciliation

`artifact_inventory.reconcile_inventory(ledger, selections, max_read_bytes=...)`
rechecks every admitted request against a caller-selected absolute file path and
manifest. `selections` is a tuple of `InventorySelection(request_id, manifest,
path)`, with exactly one entry per admitted request, at most 192. The trusted
ledger supplies each pinned manifest root, project, fragment index, length and
content/transfer binding. All selections and the aggregate logical read budget
(up to 64 MiB) are checked before any fragment is opened. The full charged amount
must fit that budget even when some files are absent. Actual reads use the
existing bounded 32 KiB verifier; the budget is not a total CPU/RSS or physical
I/O claim. Work is synchronous and sequential; admission SQLite waits retain the
existing five-second busy bound and are not asynchronously cancellable.

A report identifies each request as `verified`, `missing` or `unverified`.
Missing includes disappearance of a selected file or parent during inspection;
unverified includes unsafe file type/path, observable mutation, bad bytes and
filesystem errors. It is not an automatic repair or deletion decision. Cancellation
callbacks may raise any exception: they abort the pass rather than being mistaken
for a missing/corrupt file. No partial report is returned. A changed ledger
snapshot also rejects the complete report, including changes committed by another
process during the pass. Corrupt or unreadable ledger state fails closed.

Prior `verified_observation` metadata is never accepted as evidence of current
bytes. Reconciliation changes no records, revisions, charges or files, including
stale observations. Callers must use the fresh report for their recovery decision;
this API does not persist availability or grant a serving capability. Observations
are made at different times and are not an atomic filesystem snapshot or a
future retention promise. Reverify before serving or recovery use. Publisher
approval remains separate. Trusted stable physical directories remain required;
hostile same-user writers are outside the guarantee.

Unknown/orphan files are not enumerated or charged by this API. Establishing an
owned retained store, durable path ownership, writer fencing, crash-safe ingestion,
orphan reconciliation, safe reclaim and bounded repair remains future work. Never
release charges or execute downloaded bytes because this audit found a file missing.

The next [owned-directory prototype](artifact-directory.md) adds an explicit
single-directory binding and bounded non-destructive orphan accounting. It does
not yet provide payload ingestion or reclamation.
