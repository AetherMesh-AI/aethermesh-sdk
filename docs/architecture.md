# AetherMesh SDK architecture

## Responsibility

AetherMesh SDK is a headless, application-independent connection and protocol
layer for a future decentralized AI network. [SDK scope](sdk-scope.md) defines
its end objectives, current gaps and ownership boundaries.

Eidolon or another application owns UI, user workflows, persistent application
agents and provider choice. It can connect to AetherMesh through an optional
adapter. Other AI providers must not require the SDK.

## Current implementation

The `aethermesh_core` namespace is retained for compatibility. Today it contains:

- Local identity, lifecycle and runtime configuration
- Capability/expert manifests and input/output schemas
- Job envelopes, results, failures and version metadata
- In-memory messages, file-backed inboxes and local peer/node registries
- Deterministic reference scheduling, execution and simulations
- Local validation, hashes, receipts, lineage and contribution accounting
- A localhost JSON API and diagnostic CLI over the reusable runtime service

These pieces exercise SDK contracts without claiming a distributed deployment.
There is no authenticated P2P connection/discovery layer, remote inference
stream, distributed release authority or production consensus.

## Intended boundaries

1. **Client API:** explicit connect/disconnect, capability lookup, submit/result,
   stream/cancel, timeouts and typed failures, independent of any UI.
2. **Protocol:** versioned peer/capability/work/result/verification records, with
   negotiated compatibility and bounded payload/resource policies.
3. **Transport adapters:** discovery, authenticated sessions, message/byte transfer
   and retry behavior. Local fixtures remain useful conformance adapters.
4. **Reference node:** deterministic handlers and lifecycle used for integration
   tests; separate this from consumer imports incrementally, preserving contracts.
5. **Verification and project metadata:** hashes, attribution, artifact manifests,
   publisher authority and evidence. Keep release authority scoped to each project.

This is a target decomposition, not a claim that these modules already exist.
Do not replace working primitives with empty interfaces or rename every import
as part of a cosmetic cleanup.

## Network context

The broader AetherMesh network may use AER (Adaptive Expert Routing) across
replicated expert services. REVA describes Router → Expert → Validator →
Aggregator; AEF describes the supporting Aether Expert Fabric. These are
network/service roles, distinct from Eidolon's executive/manager/worker agents.

The SDK needs contracts to access and contribute to those services. The SDK does
not own model-training engines, application conversation/orchestration state,
full build scheduling, native installers or product dashboards.

## Source, builds and updates

The SDK may eventually discover source revisions, request/query build jobs and
transfer verified artifacts. GitHub is an initial source/distribution option;
P2P distribution is future work. Source format, signatures, channel policy,
rollback protection and independent builder evidence need a separate design.
See the [design proposals](sdk-scope.md#design-proposals-not-implemented-guarantees).

A peer serving artifact bytes cannot approve releases for another project.
Eidolon and AetherMesh must have separate project identities, versions, channels
and authorized publishers, even if they share a transport.

## Safety and validation

Keep the current unauthenticated development API on localhost. Test schema
rejection, tampered results, missing attribution and invalid state transitions.
Future network work must add threat-modelled authentication, authorization,
privacy and resource controls before exposure beyond a trusted local boundary.
A local receipt is evidence of a local check, not distributed consensus.

Connecting for AI use or updates must not silently enable training, hosting,
seeding, paid execution or unbounded bandwidth. Those modes require explicit
application/operator policy. Existing local tests do not prove these future
network properties.
