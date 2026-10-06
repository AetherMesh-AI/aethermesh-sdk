# AetherMesh SDK persistent goal

Build a small, trustworthy, headless SDK that lets Eidolon and other applications
connect to a future decentralized AetherMesh AI network without coupling them to
one UI, agent hierarchy, model engine or hosting provider.

The repository currently provides local prototype foundations. It does not yet
provide public P2P discovery, authenticated peer sessions or remote AI inference.
[SDK scope and end objectives](sdk-scope.md) is the current work boundary;
[architecture](architecture.md) explains the target layering.

## Priorities

1. Preserve usable protocol/schema, validation and provenance foundations.
2. Keep an installed SDK usable independently of desktop UI and application state.
3. Specify stable client APIs and version/compatibility boundaries.
4. Add real, authenticated peer communication in small testable slices.
5. Support capability negotiation, AI requests, streaming and cancellation.
6. Transfer and verify results/artifacts with project-scoped trust.
7. Keep participation, privacy, resource use and spending explicit.
8. Prove behavior through deterministic local fixtures and real integration tests.

AER, REVA and AEF remain context for the broader network's expert-routing,
validation and aggregation services. They do not turn this package into the
entire network platform. Training, builders, deployment and native updates have
separate owners; the SDK can expose their protocol interfaces when real services
exist.

## Decision rule

A change should improve the SDK's real connectivity, public contracts,
verification, interoperability or developer usability. Keep reference-node code
when it provides meaningful test evidence. Remove application-only scaffolding
and stale claims; preserve compatibility through an explicit migration plan.

Avoid placeholder networking, unsupported decentralization claims, dashboard or
installer work, application agent orchestration, tokenomics and broad rewrites
without an immediate tested result. Do not infer authorization for releases,
resource contribution or code execution from network participation.
