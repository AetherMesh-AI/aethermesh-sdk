# Bounded opaque artifact foundation

`aethermesh_core.artifact_codec` provides opt-in local packaging, reconstruction,
no-overwrite publication, and bounded repair of **synthetic or public** opaque
bytes. `artifact_placement` provides a deterministic placement simulator. Neither
module starts services, loads models, trains, runs received code, creates keys,
or changes trust. GitHub remains operational; project publisher authority,
release channels, SDK versions, and TLS peer identities remain separate.

This first slice is capped at **8 MiB per original artifact** and **192 logical
fragments**, with a 16 MiB encoded-byte budget. These limits can only be lowered.
It is not a large-dataset storage system. A logical fragment has no embedded
header; its complete persisted bytes are at most **5,000,000 bytes**, including
all bytes of the object. Metadata lives in a separate canonical manifest capped
at 32,768 bytes when decoded. With the current 8 MiB original limit, default
fragments are at most 2 MiB; 5,000,000 is a ceiling, not a target.

## Exact erasure profile

Manifest version `1`, codec `rs4+2-gf256-11d-systematic-v1`:

- Split the source into consecutive stripes of at most `4 * fragment_bytes`.
- For stripe length L, coding length is `ceil(L/4)`. Split into four consecutive
  equal-length data fragments, padding only the final data tail with zero bytes.
- Arithmetic is GF(256), polynomial `x^8+x^4+x^3+x^2+1` (`0x11d`), alpha `2`.
  For rows i=0..5 and columns j=0..3, define `V[i,j]=(alpha^i)^j` in that field.
  The systematic generator is `V * inverse(V[0:4])`: the four identity rows,
  followed by `(64,120,54,15)` and `(231,210,87,99)`.
- Any four distinct indices recover a stripe. Ordered manifest position i binds
  stripe `i//6` and coding row `i%6`; duplicate input indices are rejected.
  Equal hashes at distinct indices are legal, including all-zero content.
- Source bytes `00..0f`, coding length four, have parity `14 15 16 17` and
  `43 42 41 40`. Tests use a literal independently calculated bit-serial GF and
  Gaussian-elimination oracle plus all 15 four-of-six subsets. This profile is
  not a claim of interoperability with another implementation or RFC wire format.

The manifest binds project, kind (`dataset`/`model`), classification
(`synthetic`/`public`), expiry, original length/hash, fragment cap, codec/version,
and every ordered fragment hash and unchanged transport-manifest hash. Locations
and peer identities are external. Canonical JSON uses sorted keys, compact
separators, ASCII escaping, and no trailing newline. Unknown fields, versions,
noncanonical encodings, bool-as-integer values, invalid counts/lengths and
cross-project documents fail. The caller must select the expected manifest digest
through its own trusted policy; accepting a root supplied by the same untrusted
peer provides no publisher authorization.

```python
from aethermesh_core.artifact_codec import (
    ArtifactLimits, package_artifact, reconstruct_artifact,
)

package = package_artifact(
    b"disposable synthetic example", project="experiment", kind="dataset",
    data_class="synthetic", expires_at=2_000_000_000,
    limits=ArtifactLimits(fragment_bytes=1024),
)
selected = tuple((i, package.fragments[i]) for i in (0, 2, 4, 5))
source = reconstruct_artifact(
    package.manifest, selected, expected_digest=package.manifest.digest,
)
```

Every supplied fragment is checked before decoding; a corrupt extra fragment
fails the operation even if four others are usable. Callers can quarantine it and
retry explicitly. Reconstructed data and regenerated parity must match all
manifest references, padding must be zero, and the whole original length/hash
must match. `publish_artifact` performs complete verification before private-mode
(0600), atomic, no-overwrite local publication. Its result explicitly reports
`integrity_verified=True`, `publisher_authorized=False`. It provides the existing
POSIX publication semantics, not power-loss durability or protection against a
hostile process running as the same user. An error after the atomic link may
leave a published destination: inspect it instead of overwriting it.

## Transport and budgets

Logical erasure fragments are distinct from 32,768-byte transport chunks. One
5,000,000-byte object requires 153 transport chunks, within the existing 8 MiB
transfer-object and 65,536-byte frame limits. The installed-package smoke selects
one fragment, fetches it through unchanged mutual TLS, verifies its transport
binding, and combines it with three local fragments. This proves installed-wheel
loopback interoperability, not two-machine, Internet, discovery/NAT/relay, or
multi-node durable storage.

`NodeBudget` requires caller-configured node, operator and failure-domain IDs plus
free storage, object slots, payload memory, ingress/egress and concurrency budgets.
`PlacementLimits` adds aggregate transfer and concurrent-transfer limits and a
minimum operator count. The planner aggregates reservations across all stripes;
it does not multiply a 64 MiB per-store allowance into unlimited node capacity.
Every complete stripe needs six distinct configured failure domains. Distinct
node IDs do not establish different operators or independent failure domains.
These labels are assertions supplied by trusted configuration, not attestations.
Insufficient domains/operators/capacity produce explicit `degraded` output and
an unplaced stripe; failed trial reservations roll back. A later smaller stripe
may still fit. The greedy plan is conservative, not an optimal allocation.

Transfer budget units are raw fragment plus transfer-manifest response bytes,
reserving one complete fetch per hosted fragment. They exclude base64, TLS,
framing, handshake, retransmission and physical wire overhead. Payload-memory
reservation is total hosted bytes plus twice the largest fragment times allowed
in-flight transfers. Python objects, TLS buffers and allocator overhead are
excluded: this is not a hard RSS limit. Packaging/reconstruction hold multiple
bounded buffers and the whole original in memory; fragment size is **not** a
training-memory bound. Cancellation callbacks run at coding blocks of at most
32 KiB and operation boundaries; hash/file work remains bounded by this small
artifact cap. Callback latency is not a measured hard real-time guarantee.

Plans neither reserve live resources nor coordinate simultaneous planners. A
future hosting integration must enforce one shared atomic quota ledger across
all stores/jobs and actual transfers before enabling multi-object services.
Current `TransferStore` remains one in-memory snapshot, not retained storage.
`simulate_domains` distinguishes available data (four online fragments), temporary
unavailability (four retained but fewer online), and insufficient retained
fragments (including never-placed fragments). It makes no availability/durability
percentage claim and no claim that every original copy was lost globally.
Unknown outage/loss domain labels, duplicate node IDs, unknown/duplicate/out-of-range
assignments, repeated stripe failure domains and inconsistent unplaced indices
are rejected rather than silently producing optimistic availability.

## Repair, privacy, and next slices

`repair_artifact_fragments` performs one explicit local calculation. It requires
a trusted root, at least four verified fragments per stripe, distinct missing
targets, explicit read/write/count budgets, and caller-supplied trusted time.
Shape, indices, lengths and aggregate input bytes are admitted before fragment
hashing or transfer-manifest construction. Read/write budgets count logical
supplied input and returned repair output, not total CPU work or intermediate
memory: reconstruction and re-encoding revisit bytes and allocate buffers under
the separate hard artifact cap.
Expired or tombstoned artifacts cannot be repaired. It does not run a network
repair queue, reserve remote writes, or interpret a timeout as permanent loss.

Classification is a caller declaration, not a content scanner. Erasure coding
is **not encryption**; systematic fragments directly expose original bytes and
parity can also reveal information. Hashes and metadata reveal correlations.
Use only disposable synthetic/public bytes in this foundation. Expiry and local
tombstones suppress new plans/repair, but cannot prove remote deletion, erase
already copied content, or enforce another operator's retention promises. No
private dataset, user model, credential or real user key is required by tests.

Feature-sized sequence after independent review of this foundation:

1. Streaming stripe packaging/recovery with bounded RAM/temp-disk, cancellation,
   aggregate host quotas, immutable retained-object inventory, and explicit
   retention/deletion receipts. Raise artifact size only with these bounds tested.
2. Bounded deduplicated repair queue with leases, retry backoff/jitter, per-job and
   per-node quotas, foreground-transfer priority, and no expired/deleted repair.
   Simulate correlated outages, permanent loss and stalled nodes before deployment.
3. CPU-only useful AER expert workloads with configurable model/expert/resource
   sizes. Benchmark small ARM hardware for peak RAM, speed, cancellation and
   checkpoint latency, and thermal endurance. Desktop arm64 tests do not prove
   small-board suitability. Training remains outside the SDK's service utilities.
4. Deterministic safe controller, then bounded telemetry/outcomes and a small
   learned controller proposing priorities, model sizes, training effort and
   placement/redundancy within standing node limits. Independent evaluation,
   promotions, versioned actions, rollback and circuit breakers remain outside
   the candidate's control. No self-granted trust, credentials, spending or
   resource ceilings; defer ineligible jobs while continuing eligible work.

These next slices, learned control, distributed repair and useful ARM training
are not implemented here. No persistent service, credential, trust change,
external data upload or automatic downloaded-code execution is authorized by
importing or using these local utilities.

Coverage qualification: repository coverage configuration excludes raise-*Error
lines and selected exception handlers. Reported 100% SDK branch coverage is not
proof of rejection-path completeness; targeted adversarial tests are separate
evidence. The synthetic numeric example has no general-workload claim.
