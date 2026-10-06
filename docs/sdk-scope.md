# SDK scope and end objectives

## Product boundary

The SDK connects an application to a future decentralized AetherMesh network.
It is usable independently of Eidolon. An Eidolon integration is optional, and
Eidolon's other AI providers must continue to work without this SDK.

There is **no current public P2P implementation or deployed network** in this
repository. Local simulations, file-backed inboxes and localhost endpoints are
reference foundations. An advertised local capability is not proof that a peer
can execute a remote AI request.

## End objectives and acceptance evidence

1. **A headless application-facing SDK.** Keep a documented, versioned API for
   connecting, discovering capabilities, submitting work and reading results.
   Separate SDK version from protocol/schema and project release versions.
   Prove a minimal installed package works without a UI, daemon or model engine.
   Current: compatible Python exports and local execution exist; a unified
   network client and independent protocol negotiation remain planned.
2. **Real peer connectivity.** Define replaceable discovery/session/transport
   boundaries, peer authentication, reconnects and protocol negotiation.
   Test two separate processes and then machines, with unavailable/malicious
   peers and bounded retry behavior. Current: in-memory/file local transport,
   peer records and heartbeat fixtures; no socket-based P2P discovery or trust.
3. **Usable AI and job contracts.** Support capability discovery, input/output
   schema negotiation, inference requests, streaming, cancellation, timeouts,
   backpressure and stable typed failures. Distinguish submitted, executing,
   completed and independently validated results. Current: substantial local
   job/capability/result schemas and deterministic echo-style execution;
   remote inference, streaming and cancellation remain planned.
4. **Verifiable data and artifacts.** Retain result validation, creator
   attribution, receipts and lineage. Add bounded, content-addressed transfer,
   integrity verification, interrupted-transfer recovery and explicit provenance
   evaluation. Current: local hashing, receipts, schema validation and artifact
   references exist; remote byte transfer and adversarial distributed validation
   remain planned. A valid hash alone does not establish publisher authority.
5. **Project-scoped source/build/update interfaces.** Represent project identity,
   source revisions, build requests/results, platform artifacts, release channels
   and verification decisions. Eidolon and AetherMesh share transport without
   sharing release authority, versions or channels. GitHub can remain the initial
   source/artifact adapter while P2P distribution is built. Current: local version
   metadata and repository release helpers exist; distributed project registries,
   peer builds and verified update channels remain planned.
6. **Explicit participation and privacy.** Distinguish AI-client and update-only
   connections from hosting, building, training or seeding. Require explicit
   application/operator choices for each contribution role, resource/bandwidth
   limits and any spend; never infer consent from connecting. Keep private
   prompts, credentials and unrelated application state out of peer metadata.
   Current: local-only defaults and some audit redaction; network consent,
   authorization and privacy enforcement are not implemented.
7. **App-neutral integration.** Provide optional adapters usable by Eidolon and
   other UIs/services. Network-side routing is distinct from application agents.
   AER (Adaptive Expert Routing), REVA (Router → Expert → Validator → Aggregator)
   and AEF (Aether Expert Fabric) describe future services the SDK can access;
   they do not require implementing an entire AI platform in this package.
8. **Trustworthy delivery.** Maintain repeatable package builds, installed-package
   examples, compatibility/conformance tests and negative security tests. Prove
   unsupported operations fail honestly. Do not release a placeholder network
   client that reports success before real peer operations are implemented.

## Design proposals, not implemented guarantees

The following are candidates for the network/update design and require a
separate specification, threat model and implementation review:

- Reuse Git objects/bundles for source history instead of inventing a commit format.
- Use content-addressed artifacts and signed, project-scoped manifests pinning
  source, platform, artifact hashes and build provenance.
- Verify publisher authority, metadata freshness and rollback protection using
  established update-framework principles; verify independent build evidence
  where policy requires it. Peer count or hosting bytes grants no release authority.
- Let AI evaluate evidence and recommend alpha/beta/stable promotion. Promotion
  must satisfy explicit project policy and authorized publisher decisions; an AI
  recommendation is not itself permission to release.
- Expose build/training participation and artifact APIs while running trainers,
  sandboxed builders, scheduling and app installation in their owning services.
- Keep update discovery usable without requiring the application's primary AI
  node to be running. This does not make OS installation an SDK responsibility.

No blockchain, token, payment ledger or reward mechanism is needed for this
cleanup or for initial SDK connectivity.

## Repository inventory

### Keep and develop

- `messages.py`, `job_envelope.py`, `job_result_schema.py`, `job_failure_schema.py`,
  `capability_record.py`, `expert_manifest.py`: local contracts and validation.
- `identity.py`, `version_metadata.py`, `local_audit_event.py`, `receipts.py`,
  `validation_receipt_schema.py`, `result_hash.py`, lineage/contribution records:
  attribution and local verification foundations. Local contribution units are
  measurements, not financial assets or network rewards.
- `local_transport.py`, `message_bus.py`, `peer_registry.py`, `node_registry.py`,
  `node_announcement.py`: reference transport/discovery seams, not live P2P.
- `execution.py`, `runner.py`, `scheduler.py`, `simulation.py`, `dispatch.py`,
  `node_service.py`, `runtime_service.py` and lifecycle helpers: preserve reusable
  reference-node behavior until explicit client/protocol/reference-node modules
  can be extracted with compatibility tests.
- `api.py` JSON routes and `app_cli.py` diagnostics: local development interfaces.
- `examples/`, schema/provenance tests and `wordlists/node-names/`: fixtures and
  runtime identity data. Do not remove data that current runtime paths load.
- Repository quality gates and generic release metadata/check-policy helpers:
  SDK maintenance tooling, not a distributed CI product.

### Removed application scaffolding

- `desktop/`: Electron windows/UI, native sidecar packaging and service installers.
- Root `package.json`/`package-lock.json`: Electron-only workspace dependencies.
- Desktop build/release workflow and its desktop-only publication action.
- Desktop test/build CI jobs and assertions that referenced removed installers.
- Embedded HTML dashboard, browser-opening CLI behavior and OS daemon control.

Node-based source-analysis tools in SDK CI remain legitimate development tools.
Historical graph snapshots remain available for navigation, but predate this
cleanup and are not an authoritative description of the current tree.

## Cleanup and compatibility

- Distribution `aethermesh`, import namespace `aethermesh_core`, the current
  version, exported Python symbols and existing protocol/schema versions remain
  unchanged. A later package/API rename needs an explicit migration plan.
- `aethermesh`, `aethermesh-node` and `aethermesh-core` entrypoints remain.
  `[api]` is the headless server extra; `[ui]` remains a compatibility alias.
- `aethermesh ui` is retired. `GET /` now returns a JSON service index instead of
  HTML. Consumers wanting UI must provide it themselves.
- `aethermesh node start` runs the foreground local API or detects one already
  running. It no longer reads `desktop-settings.json` or invokes OS background
  services. `node stop` records local stopped status; stop the foreground server
  with Ctrl+C. It is not an OS process manager.
- Existing JSON routes, including legacy `/shutdown` and `/restart` request flags,
  remain compatible. Those flags do not install, stop or relaunch an OS process;
  a consuming supervisor must explicitly implement that behavior.
- `release_update.py` and existing CLI update commands are retained as legacy
  package-management compatibility helpers. They invoke pip and target the old
  GitHub release URL. They are not a verified SDK update channel, do not provide
  project signature/freshness/rollback checks, and currently allow missing
  checksums. Do not use them as the future P2P trust boundary. Prefer an explicit,
  reviewed package version; retire or replace these helpers in a separate change.
- No user data, local runtime state, Git history, existing tags/releases, remote
  branches, repository settings or pending pull requests are changed by cleanup.

## Next implementation slices

1. Specify the client/protocol/reference-node split and migration surface.
2. Add a client that performs real operations against the existing local API,
   with typed results, errors and explicit unsupported capabilities.
3. Implement and test one authenticated transport between two independent peers.
4. Add inference/stream/cancel contracts and one real provider integration.
5. Specify project manifests and transfer verification before update distribution.
6. Add opt-in build/training interfaces only when an implementing service exists.

Each slice needs runnable evidence and its own security/compatibility review.
This cleanup does not claim to implement those slices.
