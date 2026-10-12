# Durable local retention intent

`artifact_retention_store.RetentionStore` explicitly persists prepared retention
steps in the **original owned admission database**. Acceptance and renewal repeat
the complete bounded owned-directory audit under its exclusive cooperating-writer
lock and a SQLite `BEGIN IMMEDIATE` transaction. The prepared evidence must match
that new audit exactly. A supplied report, hash or plan alone grants no authority.

This is local operator intent and an observed custody boundary. It is not a promise
of future bytes, remote availability, replica independence, publisher authority or
permission to serve. Every receipt has `retention_confirmed`, `serving_authorized`
and `publisher_authorized` false. `durable_committed` is true only after commit.
The contained preparation object still has `durable_committed=False`; the receipt
is the persisted-operation evidence. No downloaded code is executed.

## Explicit enablement and compatibility

Constructing a store performs no I/O. Supply the owned object directory, original
ledger directory, exact store ID and admission policy, and explicit
`RetentionStoreLimits(max_intents, max_operations, max_metadata_bytes)`. Call
`enable(expected_ledger_revision=..., at=...)` deliberately. It atomically upgrades
catalog version 1 to 2, creates retention tables and freezes the limits and original
store/directory/database identities. Version-1 readers reject version 2. This is a
one-way opt-in; there is no downgrade, reset, automatic migration or restoration.
Repeating enablement with the same policy only returns existing historical state.
It does not advance the clock or renew an intent.

Missing or damaged version-2 tables, owner markers, claims, checksums and policies
fail closed. Opening a store never regenerates them. Copied databases or directories
do not inherit authority. Preserve evidence and charges under the separate
[restoration gate](artifact-restoration-gate.md). Do not feed peer databases to the
local SQLite helpers. Parents must remain trusted and stable; this is not a sandbox
against a hostile local user. A same-inode rollback of all metadata is not detected
by these local checksums and is outside the supported original-store boundary.

## Commit one prepared step

After enablement, obtain a new `audit_owned_inventory` report and prepare terms and
an accept record with `RetentionPlan.advance`. Pass that plan, **all** admission
manifest selections, `expected_revision` (the prior intent record count),
`expected_ledger_revision`, and an explicit `max_read_bytes` to `store.commit`.
Acceptance and renewal rehash the complete selected inventory, subject to the
existing 64-MiB read ceiling and owned-accounting checks. Unrelated valid admissions
remain charged; the selected retention fragment must be freshly verified.

Each admission can have only **one lifetime intent**, even after expiry. This
bounded first backend deliberately offers no new epoch or reset. A renewal needs
a fresh audit, a matching prefix and operation ID, a nondecreasing committed clock,
and a deadline that extends the previous deadline before it elapses. The existing
per-intent maximum duration, artifact expiry and record bounds remain in force.
The caller supplies trusted local time; this is not an authenticated clock.

Explicit expiry passes empty selections and a zero read budget. It needs no payload
read and preserves the last observed evidence and **all actual payload charges**.
It never deletes bytes, frees admission quota or authorizes reclamation. Expiry is
terminal. No timer runs automatically. Historical `active_count` means “not explicitly
expired”; it is not a current availability count. This API offers no availability
claim: callers must not treat a stored deadline or snapshot as fresh custody proof.

## Atomicity, retries and bounds

The encoded plan, global operation-ID/request-digest index, clock high-water mark,
and global admission revision commit in the same original SQLite transaction. Full
admission records and payload charges remain unchanged. The database retains the
existing DELETE journal / synchronous EXTRA policy and 4-MiB ceiling. A post-commit
ownership fence precedes the receipt. Cancellation is checked before work, during
the bounded audit, before mutation, and after commit before acknowledgment.

An exception or lost reply after commit is an **uncertain acknowledgment**. Retry
the identical plan, selections and revision/read-budget arguments against the intact
original store. Exact replay returns the original operation together with the
**current history**, without hashing payloads, extending a deadline, changing the
clock or resurrecting an expired intent. Any changed request under the same global
operation ID conflicts. Stale new requests conflict; obtain fresh evidence and
prepare again. A historical replay is not new custody verification.

Limits are 1–32 lifetime intents, 2–1,024 total operations and 4 KiB–1 MiB of encoded
plan metadata. Each intent retains its 2–64 record and 64-KiB envelope ceilings.
Acceptance/renewal preserve one operation, one global revision and 1,024 encoded
bytes per nonterminal intent for its final expiry record. Ordinary admission writes
respect those revision reservations too. The global 4,096 lifetime-revision ceiling
is unchanged. Metadata limits include every stored plan plus expiry reserves; the
physical database ceiling separately bounds tables/indexes. Logical reservations
are not physical disk reservations. Disk-full can still prevent an expiry write;
it never extends authority or releases charges.

Envelope and catalog checksums detect accidental corruption. They do not authenticate
a publisher, prevent deliberate recomputation, prove ownership of copied state, or
establish rollback-resistant time. No network advertising, negotiation, repair,
scheduler, credential, background service, automatic cleanup or transport extension
is part of this slice. GitHub and release/publisher identities remain separate.

## Validation boundary

Tests use disposable local stores and real local subprocesses: schema-enable crash
rollback, competing intent writers, pre-commit crash, post-commit/pre-reply crash,
reopen and exact retry, fresh-payload rejection, explicit expiry with missing bytes,
clock/CAS conflicts, cancellation, capacity reserves, SQLite full, missing tables,
corrupt metadata and lost/copy ownership. Installed-wheel acceptance exercises
accept/renew/expire/reopen/replay and charge preservation.

These checks do not establish two-machine, Internet, physical-power-loss, hostile
local-user or hardware durability. No retained-content claim or STORE-1 completion
is inferred from them.
