# AetherMesh SDK

AetherMesh SDK is the headless foundation for applications connecting to a
future decentralized AetherMesh AI network. Eidolon can use it as an optional
provider adapter; other applications can use it independently.

**Current status: authenticated direct-peer foundation, no public network.**
The SDK and optional headless service share a versioned peer protocol. Explicitly
configured peers can exchange bounded status and opt-in echo requests over mutual
TLS, with cancellation and timeouts. These are connectivity diagnostics, not AI
inference. Automatic discovery, NAT traversal, relays, streaming inference and
peer-distributed releases are not implemented. Existing local contracts, reference
execution, validation, provenance and the localhost development API remain.

The Python distribution remains `aethermesh` and the import path remains
`aethermesh_core` for compatibility. Neither is being silently renamed as part
of the repository cleanup.

## Install from source

Requires Python 3.11 or newer. From a checkout of this repository:

```bash
python -m pip install .
```

Use the package directly without a desktop application or API server:

```python
from aethermesh_core import Job, LocalRunner, NodeIdentity, validate_job_result

job = Job(job_id="hello", job_type="echo", payload={"message": "hello mesh"})
result = LocalRunner(NodeIdentity(node_id="example-node")).run(job)
validation = validate_job_result(job, result)
print(result.to_dict())
assert validation.valid
```

This is an in-process reference job, not a request to a remote AI network.
Run the complete no-network example with `python examples/sdk_smoke.py`.

## SDK and optional peer service

The base install includes `aethermesh_core.network.PeerClient` and
`aethermesh-peer serve`, backed by the same protocol and trust implementation.
An application using the client does not listen, host models, train, seed files
or start a background process. Running a service is an explicit operator choice;
its default capability is status, and echo requires `--enable-echo`.

Existing hardware-derived node IDs and four-word names can be explicitly shared
with `--node-identity PATH` or the SDK's `load_node_profile`/`node_profile` APIs.
The selected saved identity is reused without rewriting it. Optional
`--share-hardware` adds coarse hardware categories; raw MAC addresses, device
identifiers and private keys are never profile fields. Certificate pins still
authenticate connections, and advertised names/hardware remain self-reported.

Start with the [network foundation guide](docs/network-foundation.md) for
certificate/pin configuration, the installed SDK example, protocol contracts,
security limits and two-process verification. No privileged installer or extra
network dependency is required.

## Optional local API and diagnostic CLI

```bash
python -m pip install -e ".[dev,api]"
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh init
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh status
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh node start
```

The development API defaults to `127.0.0.1:7280` and serves JSON. Keep it on
localhost: it is not an authenticated public gateway. `aethermesh-core` retains
the deterministic flow/debug commands. The old `[ui]` dependency extra is an
alias for `[api]`; it no longer installs a dashboard.

See [CLI and local API](docs/ui-and-cli.md) and the
[cleanup compatibility notes](docs/sdk-scope.md#cleanup-and-compatibility).

## Scope and end objectives

Read [SDK scope and objectives](docs/sdk-scope.md) for implemented, partial and
planned capabilities and the exact ownership boundaries. In brief:

- Stable, versioned application-facing APIs and protocol contracts
- Authenticated direct connections and capability negotiation; discovery planned
- AI/job submission, streaming, cancellation and predictable failure handling
- Validated results, attribution, lineage and content-addressed artifact transfer
- Project-isolated source/build/release metadata and verification interfaces
- Explicit participation and resource/privacy controls
- App-neutral adapters, package examples and conformance/security tests

Desktop UI, native app installation/relaunch/rollback, Eidolon's agent hierarchy,
training engines and full build scheduling belong to their owning applications
or services. Existing local runtime and scheduling fixtures remain useful for
SDK development; they are not a claim that the network is deployed.

## Testing

```bash
python -m pip install -e ".[dev,api]" pytest pytest-cov ruff mypy
PYTHONDONTWRITEBYTECODE=1 python scripts/full_test.py --mode fast --base origin/main --keep-going
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m unittest discover -s tests -v
```

The fast gate checks tests, 100% branch coverage, lint/format/type checks and
repository policy. `scripts/full_test.py --list` lists the additional full gates.
No desktop application or Electron installation is required. Real-network tests
use loopback sockets and the OpenSSL executable to create temporary test identities;
they do not install certificates or modify the operating system trust store.

## Repository layout

- `src/aethermesh_core/network/`: shared peer protocol, SDK client and opt-in service
- `src/aethermesh_core/`: compatible Python imports, contracts and local foundations
- `examples/`: deterministic manifests, schema fixtures and SDK usage example
- `docs/`: SDK boundaries, reference architecture and prototype contracts
- `tests/`: unit, protocol and local API/CLI tests
- `scripts/`: repository validation and package/release metadata helpers
- `.github/`: SDK CI and contribution templates
- `graphify-out/`: historical generated navigation, not current scope evidence

[Architecture](docs/architecture.md) · [Persistent goal](docs/persistent-goal.md) ·
[Local identity](docs/local-node-identity.md) · [Local lifecycle](docs/local-node-lifecycle.md)
