# Bounded explicit local fragment ingestion

`artifact_ingestion.ingest_fragment_file` copies one explicitly selected local
fragment into an initialized [owned directory](artifact-directory.md). It is a
headless SDK utility, not network hosting, a retention lease or publisher approval.
It never executes bytes or enables contribution, AI accounts, wallets or services.
Existing manifest/fragment limits remain unchanged, including the 8 MiB original
artifact cap. No resource ceiling is increased by this feature.

Pass the directory, absolute source path, manifest and fragment index, plus the
caller-pinned `expected_digest`, request ID, ledger location/store ID/frozen limits,
`max_read_bytes` and optional cancellation callback. The aggregate *payload* read
budget must cover twice the fragment length and be at most twice the existing
maximum fragment size. Metadata reads retain their separate bounded marker/SQLite
limits. This budget does not measure filesystem read amplification, CPU, RSS or
physical reserved blocks. The API is synchronous; OS sync/close and SQLite waits
cannot be interrupted through the callback while the OS call is blocked.

The operation takes the same nonblocking exclusive directory lock as accounting,
checks its ledger/marker binding, and rejects unknown, unsafe or unfinished entries.
It does not turn an orphan into an owned object by reserving its name. After a
stable initial inventory check, admission commits the immutable request binding
and full charge before source or staging I/O. Missing, invalid or unreadable input
can leave a charge even if no staging file was created. Exact retries reuse that
charge; another manifest/index binding cannot reuse the request ID.

A source must be a regular file of exactly the declared length through trusted
physical directories. Symlinks/FIFOs and observed replacement or mutation fail
closed. Copy reads and verification reads are at most 32 KiB. Partial writes are
handled without increasing the byte budget. The new `<request-id>.staging` file
is exclusively created with mode 0600; its payload is synced, fully rehashed and
checked against both content and transfer-manifest bindings. Staging descriptor
metadata and named-path binding are rechecked before publication. These are
observed-change checks, not an atomic sandbox against hostile same-user writers.

Publication uses a same-directory hard link to `<request-id>.fragment` and cannot
overwrite an existing final. A successful link is the commit point. The final
pathname is checked against the held descriptor and the directory is synced;
only then is the matching staging alias removed and the directory synced again.
This successful-publication alias cleanup does not release any capacity charge.
No failed staging, orphan or expired record is automatically deleted or reclaimed.

Cancellation callbacks may raise any exception and are checked before admission,
through copy/verification, and at the final pre-link boundary. They are not
rechecked after publication commits. Later sync, close, ledger-change or directory
replacement errors can still raise after the final name exists. A raised exception
therefore does not mean nothing was written, and does not authorize rollback,
deleting the final, retrying in another directory or releasing capacity.

Failed/interrupted operations preserve the admission charge and whatever staging
or final names exist. A staging entry blocks subsequent ingestion, including when
staging and final are two links to the same verified inode after a crash. Recovery
of those cases requires a future explicit, evidence-based operation; no TTL or
missing-path shortcut exists. If an OS descriptor creation result is uncertain,
never guess which descriptor to close; owning-process restart may be necessary.

A clean existing final supports an idempotent retry: it is rehashed, synced and
checked without opening the supplied source, which may no longer exist. The same
two-pass budget is conservatively required, although this path reads one payload
pass. Corrupt final bytes remain untouched and charged. Admission observations
are not rewritten as a side effect; publication and metadata are not a distributed
transaction. Changes to the ledger during the operation reject the result while
preserving any already committed bytes and charges.

The result records the final path, request/root/index, payload bytes read and
whether the final already existed. `integrity_verified` is true for the observed
bytes; `retention_confirmed` and `publisher_authorized` remain false. Reverify before
serving or using the path. File/directory fsync calls are filesystem-dependent and
do not establish power-loss durability or future availability. Parents must remain
trusted and stable, and all cooperating writers must obey the directory lock.

Evidence includes real process exits after ownership commit before marker creation
and after a partial marker, plus exits immediately before/after final hard-link
publication. These are process-crash tests, not power-loss or multi-machine proof.
A multi-block fixture checks actual payload read requests and Python allocation
bounds; it is not an ARM-device or OS-RSS benchmark. Safe orphan recovery, charge
reclamation, replication/repair and network storage service remain unfinished.

[Explicit verified-staging recovery](artifact-recovery.md) can now finish a
positively verified publication or remove its proven same-inode staging alias.
It preserves charges and refuses partial, corrupt or ambiguous evidence.
