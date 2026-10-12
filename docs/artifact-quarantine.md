# Explicit quarantine of failed staging

`artifact_quarantine` provides a local inspect-then-act operation for failed
staging whose ownership remains intact. It preserves the bytes and entire
admission charge. Importing the SDK, startup, inspection, ingestion, expiration
and ordinary error handlers never invoke quarantine automatically. No new
credentials, trust changes, listener or background service are involved.

## Minimum positive ownership evidence

All of the following must hold on inspection and again before mutation:

- The operator-selected private physical directory and database belong to the
  same store ID and frozen admission policy. The ledger's durable claim matches
  the directory device/inode; the intact `store.json` pins the actual database
  device/inode. The existing bounded marker and ledger validation applies.
- The exact existing admission matches request ID, project, trusted manifest
  root, fragment index, size, content hash and transfer digest. A filename or
  matching hash alone cannot authorize disposition.
- Bounded directory accounting has no unrelated unfinished, unsafe, orphan,
  external-link, cross-request-link or over-limit state. Every selected alias
  names the same private regular inode. A final fragment for the selected request
  blocks quarantine, even if it shares that inode.
- The caller's observation matches the freshly read observation: complete ledger
  snapshot, directory/database identities, inode/size/modification/change stamps,
  selected names, observed content and transfer hashes, and classification.

These checks depend on the same trusted, stable local parents and cooperative
advisory locking as ingestion. They do not isolate hostile code running as the
same local user. Evidence is an immutable local comparison object, not a signed
credential, remote authorization or publisher authority. Its hashes describe
observed failed bytes, not permission to publish or execute them.

A copied ledger is rejected by an intact original marker even if its directory
claim was copied. Missing/corrupt markers or missing/conflicting claims remain
blocked before payload reads. The SDK never regenerates a marker from whichever
database is present. See the [lost-binding decision](artifact-recovery.md#failed-staging-and-lost-ownership-bindings): safe restoration still needs separately
preserved original ownership evidence and an explicit restoration/migration
protocol covering copies, replacement, rollback and interrupted initialization.
Current file appearance or elapsed time cannot supply that missing authority.

## Explicit use

Use operator-selected paths and an already trusted manifest/root. This example
assumes the existing ledger and ownership marker have been initialized; it does
not create them or select trust on the caller's behalf.

```python
from aethermesh_core.artifact_quarantine import (
    FailedStagingRequest,
    inspect_failed_staging,
    quarantine_failed_staging,
)

selected = FailedStagingRequest(
    path=owned_directory,
    manifest=manifest,
    index=fragment_index,
    expected_digest=pinned_manifest_digest,
    request_id=existing_request_id,
    ledger_directory=ledger_directory,
    store_id=store_id,
    limits=frozen_admission_limits,
    max_read_bytes=manifest.fragment_size(fragment_index),
)
evidence = inspect_failed_staging(selected, check_cancel=check_cancel)
# The application explicitly chooses to preserve these failed bytes in quarantine.
result = quarantine_failed_staging(selected, evidence, check_cancel=check_cancel)
```

Each call requires a logical payload read budget covering the admitted size and
capped at the existing maximum fragment size. It reads only the actual bounded
file length with requests of at most 32 KiB; empty files are allowed. A shorter
file is `partial`; a full-size content/transfer mismatch is `corrupt`. Complete
valid staging must use verified recovery instead. Oversized bytes cannot be
classified through this API. Existing 385-entry, 192-admission, byte, database and
lifetime revision caps remain unchanged. Logical byte counts and measured Python
allocations do not establish disk-block, RSS, physical-I/O or small-ARM limits.

The action repeats the full observation while holding the same exclusive
nonblocking directory lock. After file sync, cancellation, ownership and ledger
checks, it creates `<request>.quarantine` with a no-overwrite hard link, syncs the
directory, verifies both names refer to the same inode, removes only the staging
alias, and syncs again. No failed payload bytes or admission metadata are deleted.
The result reports `quarantined`, `removed_quarantine_alias` or
`already_quarantined`; integrity, retention, publisher authority and charge release
are all false.

A successful link commits the first mutation. Subsequent sync/close or ledger
errors can report uncertainty after that point. Both names may survive; the
quarantine name may survive alone. Explicitly inspect again before retrying,
because even our own link/unlink changes can invalidate the prior observation.
The retry rehashes the surviving inode and removes an alias only with fresh
positive evidence. There is no automatic rollback or implicit retry on stale
observations. Cancellation is honored before the mutation boundary; blocking
filesystem and SQLite calls retain their existing limits.

## Capacity and remaining scope

Quarantined bytes remain unavailable and fully charged even when their actual
length is zero. Both ingestion and verified recovery reject that request ID.
Other explicitly admitted requests may proceed within remaining capacity; a
replacement request needs a separate full reservation. Accounted quarantined
bytes do not make any statement about content integrity or future availability.

Real-process tests exit before/after link and unlink, including recovery starting
with both aliases. Fault tests cover sync/close errors, stale observations,
changed bindings, copied databases, lost markers, destination races and charge
reuse. Installed-package acceptance exercises the inspect/action pair and unchanged
ledger snapshot. These are local process-crash and bounded-read observations,
not power-loss, multi-machine or Internet evidence.

Lost-binding restoration, oversized/ambiguous-file disposition, actual charge
reclamation, retention leases, repair scheduling/replication and network storage
hosting remain unfinished. The SDK does not stop applications, run downloaded
content, orchestrate builds or grant publisher authority.
