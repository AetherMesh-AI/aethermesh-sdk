# Explicit verified-staging recovery

`artifact_recovery.recover_verified_fragment` is an operator-invoked local recovery
primitive for one existing admission. It is never called automatically by startup,
ingestion, inspection, expiration or an error handler. No charge, reservation or
revision is released or rewritten, even on success.

Supply the owned directory, manifest/index and pinned root, exact existing request
ID, ledger location/store ID/frozen policy, and a read budget covering the complete
fragment. The single-pass payload budget is capped at the existing maximum fragment
size. Metadata reads retain the existing marker/SQLite bounds. There is no payload
copy or size-limit increase. Reads use the existing 32 KiB verifier; logical read
counts are not a CPU/RSS, disk-block or physical-I/O guarantee.

Recovery takes the same exclusive nonblocking directory lock as ingestion and
validates the durable directory/ledger binding. Bounded accounting may tolerate
only the selected request's `unfinished_staging` condition. Orphans, unsafe/public
files, external/cross-request links, overages, incompatible final lengths and
other unfinished requests still block the operation. A matching filename or an
untrusted hash alone grants no recovery authority: the admission's full project,
root, index, size and content/transfer bindings must match.

Three outcomes are supported:

- **`published_staging`:** a complete private staging file with no final name is
  fully rehashed, synced and published without overwrite. Only after the final
  path is positively bound to that verified inode and synced is its staging alias
  removed. A new reservation is never created.
- **`removed_verified_alias`:** staging and final are two names for the same
  verified inode. Recovery rehashes that inode, checks both named bindings and
  syncs it/the directory before removing only the staging alias. The final bytes
  and full capacity charge remain.
- **`already_published`:** only a clean final remains. It is reverified/synced and
  reported without any file or ledger deletion.

Partial or corrupt staging, same-content files on different inodes, missing bytes,
unrecognized ownership, extra links and unrelated unfinished state are preserved.
This API cannot safely repair every interrupted operation. There is no force mode,
TTL shortcut, fallback overwrite, bulk cleanup, automatic quarantine or ledger
replacement. Explicit quarantine is a separate operation described below. Missing manifests, partial marker initialization, ambiguous files and
lost ownership bindings still require a separately designed operator procedure;
do not delete evidence or release charges to make the directory appear clean.

Cancellation exceptions abort until the final mutation boundary. A successful
hard link commits publication; removal of an already verified alias commits that
cleanup step. Later sync, close, ledger-change or directory-replacement errors can
raise after either outcome. Retrying this explicit operation rechecks the actual
state; an exception never authorizes rollback or capacity reclamation. No partial
report is returned. A ledger change during verification rejects before mutation;
changes noticed after mutation report uncertainty while preserving charges.

The result describes observed integrity and the specific action. Publisher
authorization, retention confirmation and charge release are always false.
Reverify before using/serving a returned path. Parents and directories must remain
trusted and stable, and all cooperating writers must honor the lock. Advisory
locks and observed pathname checks do not isolate hostile same-user mutation.
Filesystem sync calls are not power-loss durability proof; blocking OS/SQLite
calls retain their existing limits and cannot be preempted by the callback.

Tests terminate real processes before/after publication and before/after alias
removal, including recovery that starts with both aliases already present. Each
restart explicitly revalidates the surviving names, completes only positively
verified actions, and retains the original admission and charge. This is local
process-crash evidence, not multi-machine, Internet or small-device deployment
proof. Oversized/ambiguous-file disposition, actual charge reclamation, repair/replication
and network storage hosting remain unfinished.

## Failed staging and lost ownership bindings

[Explicit failed-staging quarantine](artifact-quarantine.md) is a separate local
operation for intact directory/ledger ownership. It preserves failed bytes and
all charges after a caller-selected observation is revalidated. It is never
invoked automatically by verified recovery or ingestion. Empty, short or corrupt
bytes may qualify; oversized files and ambiguous ownership remain blocked. An
old modification time does not establish abandonment, ownership, safe deletion
or permission to release capacity.

Lost ownership evidence requires a different procedure. The version-1 ledger
claim records the owned directory device/inode; `store.json` additionally pins the
database device/inode, store ID and frozen policy. If that marker is lost, a copied
database can retain the same directory claim. Recreating a marker from whichever
database happens to be present would silently adopt that copy. If the claim is
lost, filenames and content hashes cannot recreate its authority either. Neither
wall-clock age nor a successful content verification repairs either ambiguity.

Preserve the original database, journal if present, marker (including damaged
bytes), directory entries and manifests for operator investigation. Keep writers
stopped through application-owned coordination; the SDK must not stop unrelated
applications or alter trust. A future restoration/migration tool needs separately
preserved original ownership evidence, an explicit operator-selected mapping and
a design for copied databases, replaced directories, rollback and interrupted
initialization. A backup or hash alone is not blanket permission to rebind. There
is currently no supported marker regeneration, claim reconstruction, ownership
migration or lost-binding charge-reclamation API.

The unresolved ownership-evidence and rollback decisions are tracked as a distinct
[deferred restoration gate](artifact-restoration-gate.md). Intact-binding
[owned inventory audits](artifact-owned-inventory.md) do not bypass that gate.
