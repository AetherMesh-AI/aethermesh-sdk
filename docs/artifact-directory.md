# Owned directory and orphan-accounting prototype

This is the local ownership foundation for future durable ingestion. It does not
ingest, adopt, serve, repair or delete payloads. Use the installed Python module
`aethermesh_core.artifact_directory`; no service, wallet or account is started.

`initialize_artifact_directory(path, ledger_directory=..., store_id=...,
limits=...)` exclusively creates a caller-selected absolute physical directory
with mode 0700 for an existing private admission ledger. All parents must be
trusted and stable. The ledger atomically claims that directory's device/inode
under BEGIN IMMEDIATE, advances its revision once, and never replaces the claim.
An optional version-1 `owned_directory` table is added only by this explicit
operation; existing ledgers remain readable without initializing a directory.
An existing claim always rejects initialization, even if device/inode numbers
match, so inode reuse cannot silently authorize reinitialization. Charges and
fragment records do not change. Two processes cannot claim different
directories for the same ledger. Copying the database is not a supported way to
create another capacity scope.

A private, bounded `store.json` marker pins layout version 1, store ID, frozen
policy and database device/inode. It is a local binding, not signed publisher
identity or protection from an operator editing the database. A relocated copy
of the database or a replaced directory fails inspection. Directory renaming
that preserves its inode is not a new capacity scope. Filesystem migration,
restoration with different inodes, or losing the bound directory requires a
future explicit migration/recovery procedure; there is no rebind API.

Initialization syncs the new parent entry, marker and directory. These settings
are filesystem-dependent, not a power-loss guarantee. Failed or interrupted
initialization leaves the directory, any marker bytes and committed claim for
explicit inspection. It does not overwrite, roll back a claim, delete unknown
files or retry under a different directory. Unknown descriptor ownership after
interruption is not permission to guess/close an unrelated descriptor. Process
restart may be needed when an OS operation's result is uncertain.

`inspect_artifact_directory(...)` opens the same ledger and takes an exclusive,
nonblocking POSIX advisory lock on the directory inode. All future cooperating
writers must use this same lock. Lock contention fails immediately; process exit
releases its descriptors/lock. Trusted same-user processes can bypass advisory
locks, so this is not a hostile-local-user sandbox. No lock survives the call,
and the result is not a write permit or a filesystem snapshot.

The bounded layout recognizes `<request-id>.fragment` and
`<request-id>.staging`, where the request ID is an existing 64-hex admission ID.
These names are ownership slots, not evidence that the contents match a manifest.
At most 385 entries (one marker plus two slots for each of 192 records) are
inspected; excess entries fail closed without unbounded traversal. Payloads are
never opened. The marker read is bounded by the existing 32 KiB private-file
reader. Cancellation aborts without partial results. The ending ledger snapshot
must match the starting revision/records/charges.

Accounting reports observed regular-file logical bytes once per device/inode,
including orphan regular files. It does not measure disk blocks, sparse allocation,
directory overhead or nonregular objects. Every orphan, unsafe/public entry,
unresolved staging slot, external/cross-request hard link, wrong final length or
per-request byte overage sets `blocked`. Files with the correct length can still
contain corrupt bytes: `content_verified`, `retention_confirmed` and
`publisher_authorized` are always false. A report with `blocked=False` grants no
ingestion, serving, deletion or release authority. Hash bytes separately using
[read-only inventory reconciliation](artifact-admission.md#read-only-restart-reconciliation).

Unknown files are reported and preserved; they are not silently added to or
subtracted from the ledger. Original charges survive missing files, uncertain
outcomes and process restarts. No TTL, expired observation or missing pathname is
sufficient evidence for reclamation. Complete durable ingestion still requires a
reviewed crash-safe publication protocol, writer fencing, bounded staging,
explicit orphan resolution and safe reclaim. Network storage/repair and actual
multi-machine/resource measurements remain later work.

An explicit [bounded ingestion primitive](artifact-ingestion.md) now uses this
ownership/locking foundation. It preserves failed staging and charges; recovery,
reclamation and network hosting are still separate work.
