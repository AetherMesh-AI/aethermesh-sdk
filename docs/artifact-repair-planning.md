# Pure bounded repair candidates

`artifact_repair_plan.plan_artifact_repair` prepares a candidate for one explicitly
pinned artifact using local owned-inventory audit objects. It performs no file or
network I/O, decoding, resource reservation, scheduling, publication, deletion or
charge release. It never repairs a marker or adopts a copied database. Importing
this utility does not start work or advertise a capability.

## Inputs and explicit bounds

Supply the manifest and trusted root, one to 32 `RepairInventory` selections,
unique target indices, an explicit `RepairPlanLimits`, a caller-selected `now`,
and an optional cancellation callback. Each selection provides a stable local
source label, a configured failure-domain label and a complete
`OwnedInventoryReport`. These reports must come from explicit local audits, not
untrusted peer-deserialized objects. They remain caller-held observations; this
pure function cannot authenticate their origin or establish current availability.

`RepairPlanLimits` requires all four values; there are no implicit job budgets:

- `max_read_bytes`: 0–16 MiB of selected fragment payload input.
- `max_write_bytes`: 0–16 MiB of proposed target payload output.
- `max_inflight`: 1–8 inputs in each proposed batch.
- `max_targets`: 1–192 unique target fragment indices, inside the manifest bounds.

Existing artifact/fragment caps remain unchanged. At most 32 bounded reports of
192 admissions each are inspected, and at most 192 verified candidate source rows
are accepted. No input payloads are loaded. These are metadata/planned-payload
limits, not CPU/RSS, codec temporary-buffer, network overhead, physical disk or
shared-resource reservations. A future executor needs separate whole-job memory,
scratch, network and aggregate concurrency admission.

The current codec reconstructs the entire artifact before re-encoding requested
targets. Consequently this planner requires four input fragments for **every
stripe**, including stripes without a requested target. It budgets exactly those
selected payload lengths and all requested output lengths. It does not imply that
an executor may repair one stripe with the existing whole-artifact codec or omit
intermediate buffers from its resource budget.

## Evidence selection and failure domains

Validate complete report/admission agreement, aggregate charge/read allowance,
exact project/root/index/size/content/transfer binding and false authority flags.
Only `verified` observations for the selected artifact count. Missing, unverified,
quarantined and other-root observations never become candidate inputs. Different
request IDs for one index do not create distinct erasure inputs.

Each stripe requires four distinct fragment indices and four distinct configured
failure domains. A bounded deterministic matching step can move an earlier choice
to another domain when needed; it does not depend on input inventory order or
mistake a duplicate index for extra redundancy. Repeated source labels, directory
identities or database identities reject the input even if relabeled with another
domain. Local inode identities share one host namespace; remote-host inventory
transport and identity scoping are not supported by this slice.

Configured domains are assumptions selected by the caller, **not proof of physical
or operator independence**. Several directories on one Mac are not independent
failure domains. The result always sets `failure_domains_verified=False`, as well
as false execution, retention and publisher-authority flags. A future cross-host
planner/executor must define and validate provenance and failure-domain policy
separately; arbitrary domain strings and peer reports cannot establish it.

Reports may already be stale. The plan carries the exact input reports/snapshots
and sets `requires_revalidation=True`; it has no freshness lease. Before any future
execution, revalidate intact ownership, complete current ledger state, selected
bytes, serving/source eligibility and applicable resource/domain policy. Age or a
copied report cannot grant recovery authority. The caller's `now` only blocks
expired artifacts, never proves ownership or releases capacity. Deleted artifacts
are also blocked without changing their charge or bytes.

## Results and remaining execution gate

A `candidate` lists four inputs per stripe, canonical target indices, input batches
no larger than `max_inflight`, and required read/write payload bytes. Batches are
only a proposed sequential input phase; there is no promise to overlap fetching,
decoding or output writes safely. The plan makes no target-store reservation.

A `blocked` result has no selected inputs or batches and reports one reason:
`expired`, `deleted`, `byte_budget`, `target_already_observed`, or
`insufficient_distinct_indices_or_domains`. Required payload amounts remain visible
for diagnosis. A target already observed as verified needs a separately designed
copy/replication decision, not this missing-fragment reconstruction plan. Malformed,
misbound, repeated or oversized metadata raises instead of returning a partial
candidate. Cancellation likewise raises with no partial result or side effects.

Before implementing an executor, define resource admission for inputs, whole-codec
memory and scratch/output, freshness rechecks, source authorization, cancellation,
partial-output recovery, no-overwrite publication and post-write verification.
The [deferred restoration gate](artifact-restoration-gate.md) remains blocked, and
[retention advertising](artifact-owned-inventory.md#gates-before-advertising-retention)
requires its own lifecycle/protocol evidence. No distributed lease or credential
mechanism is selected here. Retention, repair execution, replication and network
storage hosting remain unfinished.

Tests exercise matching, distinct indices/domains, repeated local identities,
non-authoritative stale/copy scenarios, all-stripe budgets, cancellation and pure
no-I/O behavior. Installed acceptance uses synthetic fragments and multiple local
directories with explicitly fictitious domain labels. That is API integration
proof, not independence, multi-machine, Internet or power-loss evidence.
