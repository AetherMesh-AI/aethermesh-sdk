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
