# Retention-aware local repair planning

`plan_retained_artifact_repair` connects the original durable retention stores,
owned inventory audits and pure repair planner. It produces a **candidate**, never
permission to serve, execute repair, overwrite data or delete/reclaim anything.
No listener, scheduler, timer, account or network operation starts.

Supply one to eight `RetainedRepairSource` entries, each with an explicit original
`RetentionStore`, source ID, configured failure-domain label, complete manifest
selections and expected global ledger revision. Supply the pinned artifact digest,
target indices, `RepairPlanLimits`, aggregate `max_audit_read_bytes` and caller time
`now`. The time and domain labels are local caller assumptions, not authenticated
clock evidence or proof of independent hosts.

The utility acquires all selected original-directory locks in path order and opens
SQLite `BEGIN IMMEDIATE` transactions before auditing. It checks expected revisions,
frozen retention policy/ownership and the recorded clock high-water values. Duplicate
sources/directories/databases fail closed. The shared owned audit rehashes bytes and
checks complete accounting under these locks. A final ownership and full retention
snapshot comparison precedes return. All locks and transactions are released before
the caller receives a result. No metadata, recorded clock, charge or payload changes.
The existing SQLite busy timeout remains bounded; cancellation is checked between
stores, throughout audits and during planning. At most eight stores are held.

The aggregate audit budget covers **all charged admissions**, including expired,
missing and quarantined payloads, up to 64 MiB. It is separate from the repair
candidate's existing read/write/inflight/target budgets. Quarantined payloads are
accounted for but never opened by the audit. A blocked plan does not release quota.

A source fragment is eligible only when its new observation is verified, its exact
admission has a persisted nonterminal intent, the recorded admission revision still
matches, and `now` precedes its deadline. Changed admission revisions are ineligible
until an explicit fresh retention operation establishes matching evidence. An elapsed
deadline excludes a source even before an explicit expiry record is written. Explicit
expiry remains terminal. Missing, corrupt, unretained and quarantined data cannot be
selected as retained repair inputs. Every exclusion remains in the full evidence;
no observation is relabeled or removed to manufacture eligibility.

`RepairInventory.eligible_request_ids` is a new optional **restrictive filter** for
the existing pure planner. `None` preserves its prior behavior; an explicit empty
tuple allows no sources. IDs must be unique members of the complete supplied report.
The pure filter is caller-supplied policy, not durable evidence or authority. Existing
verified target fragments still block duplicate-target planning even if ineligible
as sources. The integration derives filters from the fresh locked retained evidence.

The returned `RetainedRepairCandidate` contains the pure candidate, complete store
snapshots and inventory reports, per-admission eligibility reasons, caller observation
time and charged audit budget. All serving/deletion/retention authority flags remain
false, and the nested candidate also denies publisher/execution/domain authority.
`requires_revalidation` remains true: results can become stale immediately after
locks are released. They must not be saved and later treated as authorization.
Caller time is checked against persisted mutations; this read-only operation does
not persist later clock observations and is not a rollback-resistant clock service.

Tests use disposable synthetic stores and local subprocesses. They cover fresh
selection, elapsed/explicit expiry, unchanged charges for unretained/quarantined
payloads, missing/corrupt bytes, stale admission/global revisions, stale injected
reports, final snapshot changes, aggregate budgets, cancellation and process restart.
A cooperating store writer is blocked by the directory lock. A separately opened
**direct admission writer**, which does not take that directory lock, reaches its
mutation and receives SQLite BUSY while all planner transactions are held; its
charges and revisions remain unchanged. That test uses a short fixture-only busy
timeout. Installed-wheel acceptance exercises fresh and elapsed eligibility.

This is a single-host namespace of inode identities. It does not prove multi-machine
availability, Internet connectivity, power-loss durability, hostile local-user safety
or independent failure domains. Same-inode metadata rollback and lost-binding
restoration remain outside the supported boundary. See the
[durable intent store](artifact-retention-store.md) and
[restoration gate](artifact-restoration-gate.md). GitHub, transport identities and
project publisher/release authority remain separate.
