# Owned inventory audit and retention readiness

`artifact_owned_inventory.audit_owned_inventory` combines intact local ownership,
canonical owned paths, bounded directory accounting and fresh fragment verification.
It is a read-only input to operator diagnosis and future repair planning. Its
`retention_confirmed`, `publisher_authorized` and `repair_authorized` flags are
always false. Nothing is advertised to peers or persisted as availability.

## Current audit

Pass the owned directory, ledger location/store ID/frozen policy, a tuple of
`OwnedInventorySelection(request_id, manifest)` for every admitted request exactly
once, and `max_read_bytes`. The trusted ledger supplies each pinned manifest root
and exact project/index/size/content/transfer binding. Callers cannot redirect an
admission to an arbitrary payload path: final paths are derived as
`<owned-directory>/<request>.fragment`.

The audit requires intact ownership and holds the existing exclusive nonblocking
directory lock for the whole pass. Before any payload open, it validates the
complete selection (at most 192 records), requires the full charged logical read
budget (at most 64 MiB), and checks the bounded directory inventory (385 entries).
Unsafe/orphan entries, unknown links, overages, unfinished staging and mixed
final/quarantine aliases reject the whole operation. Resolve positively owned
staging through the explicit recovery/quarantine APIs first. Unknown ownership
still hits the [deferred restoration gate](artifact-restoration-gate.md).

Each admission receives one observation:

- `verified`: the canonical final was freshly checked against content and transfer
  hashes. Prior `verified_observation` ledger metadata is ignored.
- `missing`: the canonical final could not be found by the verifier.
- `unverified`: verification of an otherwise accounted final failed. Filesystem
  errors or bad content grant no deletion or repair authority.
- `quarantined`: accounting identifies a retained quarantine name. Its payload is
  not opened or counted as usable, and the whole original admission stays charged.

Payload verification reuses the existing reads of at most 32 KiB. Cancellation
exceptions propagate even when they are ValueError/OSError; they are not downgraded
to an inventory status. Final accounting, ownership and complete ledger snapshot
checks reject observed changes. No partial report is returned. Missing and
quarantined objects consume the conservative read-budget allowance even when no
payload is read. The report includes the full ledger snapshot, directory/database
identities and observed logical regular-file bytes; it changes no records, revisions,
charges or payloads.

This is a sequence of observations under cooperative locking, not an atomic
filesystem snapshot or transferable capability. A report cannot prove future
availability. Reverify at the time of any later serving or repair use. Trusted
stable physical directories and cooperative writers remain required; advisory
locks do not isolate hostile same-user code. Logical budgets do not establish RSS,
physical disk reservation, hardware durability or network availability. Existing
SQLite waits remain bounded but synchronous; OS calls are not callback-preemptible.

## Gates before advertising retention

A fresh local audit is necessary input, not enough evidence for a retention claim.
A future retained-store service must separately define and test this lifecycle:

1. **Accept responsibility explicitly.** Pin exact project/artifact/fragment and
   request bindings, operator-selected duration and resource scope, serving policy,
   aggregate capacity and concurrency budgets. No upload or hash implicitly opts a
   node into hosting. Publisher and transport authority stay separate.
2. **Establish retained state.** Complete admission, file verification, publication
   and synchronization; reconcile after restart before any advertisement. Specify
   the persistence/acknowledgment boundary and uncertain-outcome retry semantics.
   Current metadata and process-crash tests do not prove power-loss durability.
3. **Define the claim protocol.** Decide what an offer, accepted responsibility,
   availability observation, expiration, renewal and withdrawal mean, including
   versioning and stale/duplicate messages. No distributed lease protocol or new
   credential mechanism is selected here. Peer assertions are not independent
   retention proof, and a service must not advertise this capability prematurely.
4. **Maintain evidence within budgets.** Specify revalidation/invalidation and
   bounded serving behavior under corruption, restart, disk pressure, stalled
   clients, cancellation and failed transfers. Detect degraded availability and
   withdraw stale claims. Test clock rollback and expiry semantics; timestamps
   alone never release ownership or charges.
5. **Retire safely.** Separate stopping advertisements from writer quiescence,
   payload disposition and capacity release. Every failed or uncertain transition
   preserves admission charges until an independently reviewed reclaim procedure
   proves that reuse is safe. There is currently no such release API.

Retention testing must cover actual cooperating processes and acknowledgment-loss
boundaries first, then separately authorized machines/filesystems and measured
resource behavior. No loopback or installed-package test is Internet, independent
failure-domain, ARM-training or power-loss proof. App orchestration remains outside
this SDK.

## Concrete next repair-planning slice

After this audit is reviewed, add a pure bounded planner for **one explicitly
selected artifact root** using a fresh audit and caller-selected target indices.
This is a proposed next slice, not an implemented planner or authorization to repair.
It should produce only candidate inputs, missing targets, conservative resource
estimates and explicit blocked reasons. It must not fetch, decode, reserve, write,
advertise, schedule work or reclaim charges.

Require exact project/root/index binding and at least four distinct verified
fragment indices per affected 4+2 stripe. Multiple request IDs for the same index
count once; missing, unverified and quarantined bytes never count. Keep existing
manifest/fragment bounds, cap total planned input reads and output bytes, and make
unavailable capacity or inadequate inputs explicit. Local copies do not establish
independent operators or failure domains. Bind the plan to the audit snapshot,
but require fresh ownership and byte verification again before any future executor
uses it. Planning is neither admission nor a serving lease.

Before adding an executor, separately specify scratch/output admission, whole-job
memory and I/O bounds, cancellation and partial-output handling, no-overwrite
publication, post-write verification, and changed-inventory rejection. Existing
in-memory codec limits are not a measured small-device repair-memory guarantee.
Network source selection, authenticated retrieval, retention renewal, repair
scheduling and replication remain later milestones. The lost-binding gate stays
independent and blocked throughout.
