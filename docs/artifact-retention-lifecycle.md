# Local retention lifecycle preparation

**Status: bounded executable record contract and persistence design only.**
`artifact_retention` prepares explicit local intent, renewal and expiry records.
It does not persist them, establish current custody, serve bytes, advertise to
peers, delete files or release charges. `durable_committed` and
`retention_confirmed` are always false. This is preparation for durable local
records, not a durable retained-object service or a network lease.

## Implemented contract

Build `RetentionTerms` with `retention_terms(manifest, report, request_id, ...)`
from a caller-selected manifest and an `OwnedInventoryReport`. Explicitly provide
store ID, frozen admission limits, maximum intent duration and record count.
Terms bind directory/database device/inode identities, exact admission/project/
manifest/fragment/content/transfer IDs, artifact expiry and operator-selected
bounds. Store ID and policy are caller declarations until a future backend checks
them against the intact live ledger. A copied report or a matching hash grants no
ownership, publisher, serving or retention authority.

Create `RetentionPlan(intent_id, terms)`, then call `advance` with an explicit
operation ID, expected record count, action and caller-selected timestamp:

| Action | Required inputs and behavior |
| --- | --- |
| `accept` | Empty history, verified selected-fragment observation, explicit `until`; prepare local intent only. |
| `renew` | Existing nonexpired intent, verified observation, strictly extended `until`, nondecreasing timestamp, global/selected-admission revisions and observed charges. |
| `expire` | Explicit call at or after the recorded deadline; preserve that deadline and last evidence/accounting observation; terminal. |

Acceptance/renewal require `at < until <= artifact expiry`, with duration no longer
than the explicit policy ceiling (at most seven days). Renewal at/after the old
deadline is rejected. An expired history cannot be revived; an exact retry returns
the current history without resurrecting anything. An operation ID reused with
changed intent fails. New operations require the exact current record count.
These are pure conflict semantics, not an implemented database compare-and-swap.

The last evidence binds the supplied complete ledger snapshot, all observation
statuses, directory/database identities, read budget and observed regular bytes.
Only a verified selected admission is eligible for acceptance/renewal. Missing,
unverified or quarantined selected payloads reject it. Different ownership,
manifest/admission bindings, excess quota or regressed accounting reject it.
The complete supplied snapshot is hashed; the helper cannot independently prove
that it is complete, recent or belongs to the current on-disk store. An unchanged
observation can hash alike. **No wall-clock timestamp or hash proves freshness.**

Expiry preserves the last *observed* charge total in its history. It never writes
that old value back to the admission ledger, and never reduces actual charges for
admissions that may have arrived since the observation. Missing or corrupt bytes
still consume their original admission. An intent duration is not an eviction TTL.
Stopping a future availability claim, disposing of bytes and releasing quota are
three separate operations; only the first has lifecycle semantics here.

The canonical version-1 encoding has exact fields, chained record digests and a
root commitment to terms/intent ID. It rejects duplicate operation IDs, broken
links, invalid transitions, unknown fields/versions and noncanonical JSON. Hashes
provide accidental-corruption/binding checks, not authenticity, anti-rollback or
operator authority. Validly re-encoded fabricated histories remain untrusted data.
Encoding is not an acknowledgment that anything has reached durable storage.

Bounds: 64 KiB encoded plan, 2–64 lifetime records per intent, one slot reserved
for terminal expiry, existing 192-admission/64-MiB report limits, exact integer
clock/revision fields and bounded identifiers. Exhausting renewal slots blocks
renewal while preserving the final expiry slot. Cancellation propagates without
returning a partial plan. There are no timers, background workers, filesystem or
network operations in this module. This per-intent bound does not create a new
aggregate storage budget; the future backend must fit the existing store limits.

## Required durable backend — not implemented

A durable implementation must be a separately reviewed change. It must not bolt an
automatically created second authority onto a damaged or ambiguously owned store.
The [lost-binding restoration gate](artifact-restoration-gate.md) remains blocked.

1. **Explicit enablement and schema boundary.** Bind the format to the original
   admission store/policy and intact directory marker. Decide an explicit schema
   transition with old-reader rejection; never interpret a missing/corrupt retention
   table or marker as permission to initialize new history. Opening is read-only
   with respect to schema. No new credentials or changed trust are needed/authorized.
2. **One transaction and writer boundary.** Hold the existing directory writer
   lock, validate the live marker and both physical identities, then take the
   original ledger's SQLite write transaction. Check the exact admission snapshot,
   expected intent revision and operation digest. No separate ledger can authorize
   custody or overwrite charges. Enforce at most one active intent per admission;
   a new intent after terminal expiry requires explicit selection and fresh audit.
   Retain operation tombstones across intent IDs so a new ID cannot reset the
   shared lifetime revision/capacity limits. Specify bounded lock acquisition and avoid nested
   directory-lock acquisition or a SQLite read-to-write upgrade race.
3. **Fresh byte evidence.** Acceptance/renewal must run bounded owned-file accounting
   and fresh verification inside that same cooperative writer scope, with exact
   manifest bindings and the full charged read allowance. Recheck ownership,
   accounting and ledger state immediately before commit. Refactor the existing
   audit into a reusable locked helper only with its original fence tests intact.
   A caller-supplied `RetentionPlan` or report cannot replace this check.
4. **Atomic records and acknowledgment.** Commit the intent record, idempotency
   mapping, clock high-water value and global metadata revision atomically in the
   existing owned database. Preserve every admission and full payload charges.
   Stay within the current 4-MiB database and 4,096 lifetime-revision ceilings;
   reserve terminal-record capacity before accepting work. Use the existing
   synchronous SQLite policy and define exactly when acknowledgment is returned.
   Logical quota is not a physical disk-space reservation or power-loss guarantee.
5. **Uncertain outcomes and restart.** Before commit, failure/cancellation leaves
   no accepted record. After commit but before acknowledgment, reconcile the exact
   operation ID and request digest from the intact original store; never blindly
   repeat a renewal, recreate metadata, release capacity or infer failure from a
   lost reply. Restart revalidates custody before any availability use. A valid
   historical record by itself is not current byte verification.
6. **Clock and expiry semantics.** Persist clock high-water observations used for
   lifecycle decisions. Clock rollback or unavailable time must fail closed for
   new acceptance/renewal/availability claims while preserving records and charges.
   Explicit expiry is terminal even after clock correction. Elapsed deadlines
   suppress any future claim before an expiry record is written; failure to record
   expiry cannot extend responsibility. No automatic deletion or reclamation follows.
7. **Scope of a future claim.** Even an acknowledged local record can establish only
   the tested local custody observation and operator intent. It cannot promise
   remote availability, independent replicas, network durability or publisher rights.
   Transport/service protocol, renewal negotiation, serving budgets and remote proof
   are separate milestones. No hosting capability should be advertised by this slice.

## Acceptance status

- Implemented: pure explicit transition/replay contract, bounded canonical records,
  exact binding checks, accounting preservation, terminal expiry and cancellation.
- Implemented tests: acceptance/renewal/expiry roundtrip; exact retries and conflicting
  IDs; stale revisions; backward clocks; duration/artifact expiry and record budgets;
  missing/corrupt/quarantined evidence; owner/manifest/quota mismatches; complete
  observation commitments; malformed/reordered records; purity and cancellation.
- Required before durable completion: real cooperating-process concurrency against
  one store (including competing intent IDs for one admission), stale writer
  rejection, fresh locked audit integration, atomic revision
  and operation persistence, reopen/replay, disk/database-bound exhaustion, and
  crashes immediately before commit, immediately after commit and before reply.
- Required negative persistence cases: copied database, replaced directory, missing/
  truncated marker or retention table, schema mismatch, stale backup, changed payload,
  orphan/staging/quarantine conflicts and cancellation at every commit boundary.
  All failures must preserve bytes and full charges without restoring ownership.
- Still unimplemented: durable retention commit/renewal/expiry, actual claim serving,
  reclamation, repair execution, authenticated remote retention or replication.

The original owned-inventory and pure repair-planning foundations remain usable.
This preparation does not mark STORE-1 complete or authorize network hosting,
paid resources, larger quotas, new identities, app orchestration or restoration.
