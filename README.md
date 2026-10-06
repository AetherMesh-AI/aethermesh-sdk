# AetherMesh SDK

AetherMesh SDK is the headless foundation for applications connecting to a
future decentralized AetherMesh AI network. Eidolon can use it as an optional
provider adapter; other applications can use it independently.

**Current status: local prototype, no public P2P network.** This repository has
reusable Python contracts, deterministic reference execution, file/in-memory
transport, validation, provenance and a localhost development API. It does not
yet provide authenticated peer discovery, remote AI inference, streaming,
network cancellation or peer-distributed releases.

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
- Future peer discovery, authenticated connections and capability negotiation
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
No desktop application or Electron installation is required.

## Repository layout

- `src/aethermesh_core/`: compatible Python imports, contracts and local foundations
- `examples/`: deterministic manifests, schema fixtures and SDK usage example
- `docs/`: SDK boundaries, reference architecture and prototype contracts
- `tests/`: unit, protocol and local API/CLI tests
- `scripts/`: repository validation and package/release metadata helpers
- `.github/`: SDK CI and contribution templates
- `graphify-out/`: historical generated navigation, not current scope evidence

[Architecture](docs/architecture.md) · [Persistent goal](docs/persistent-goal.md) ·
[Local identity](docs/local-node-identity.md) · [Local lifecycle](docs/local-node-lifecycle.md)
