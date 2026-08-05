# AetherMesh Core

AetherMesh Core is an early local-first prototype for a decentralized AI mesh. It focuses on the mechanics a real network will need later: local node identity, capability advertisement, deterministic job dispatch, result validation, contribution accounting, receipts, and auditable flow artifacts.

The long-term architecture direction is **AER: Adaptive Expert Routing**, where work is routed across expert services, validators, aggregators, and supporting nodes. This repository is not that full distributed network yet. It is the small, testable foundation that proves the node/work/validation loop before public networking, production peer discovery, or economics are added.

![GitHub code size in bytes](https://img.shields.io/github/languages/code-size/AetherMesh-AI/aethermesh-core?style=flat-square&label=Code%20Size)

![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-raw/AetherMesh-AI/aethermesh-core?style=flat-square&label=Open%20Issues)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-closed-raw/AetherMesh-AI/aethermesh-core?style=flat-square&label=Closed%20Issues&color=%2350C878)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-pr-raw/AetherMesh-AI/aethermesh-core?style=flat-square&label=Open%20Pull%20Requests)
![GitHub Issues or Pull Requests](https://img.shields.io/github/issues-pr-closed-raw/AetherMesh-AI/aethermesh-core?style=flat-square&label=Closed%20Pull%20Requests&color=%2350C878)

![GitHub number of milestones](https://img.shields.io/github/milestones/open/AetherMesh-AI/aethermesh-core?style=flat-square&label=Active%20Milestones)

## Current status

This repo currently provides:

- A Python package named `aethermesh`
- A first-class local CLI: `aethermesh`
- A legacy/prototype flow CLI: `aethermesh-core`
- A localhost FastAPI status/dashboard surface
- Local-only node lifecycle, identity, manifest, dispatch, transport, validation, ledger, receipt, and aggregation primitives
- Deterministic examples and tests for the local prototype flows
- Electron desktop packaging scaffolding for launching the local runtime

Current non-goals:

- No public peer-to-peer networking yet
- No production consensus or distributed trust layer yet
- No rewards, payouts, tokens, or economic fairness model yet
- No claim that the prototype is already decentralized in production

## Capability resource hints

Local capability entries may include optional `resource_hints` in local
`config.json` under `capabilities.resource_hints`, keyed by registered capability
identifier. Supported descriptive fields are `cpu_class`, `ram_range`,
`disk_needs`, `expected_duration`, `network_sensitivity`, `accelerator_type`,
`energy_profile`, `operator_cost_label`, and `operator_notes`.

These fields are advisory, local, and non-binding. They are practical routing
metadata only: they do not replace the capability manifest ID, creator node ID,
validation receipt references, lineage, or contribution attribution, and they
do not represent a price, payment, reward, token value, staking, yield, or
financial settlement. Token-economic field names and wording are rejected.

## Local expert input schemas

Version 2 local model/expert manifests require `input_schema_ref`. It is a safe
relative reference resolved from the manifest directory without network access.
Manifest loading fails when the field is missing, empty, unresolved, or does
not point to a JSON Schema draft 2020-12 file that declares required input
fields, accepted types, and validation constraints. Passed validation receipts
record the manifest ID, creator node ID, input schema reference, and pass
status alongside the existing lineage and contribution attribution evidence.

## Install for development

Requires Python 3.11 or newer.

```bash
python -m pip install -e ".[dev,ui]"
```

This installs:

- `aethermesh` for normal local node use
- `aethermesh-core` for lower-level prototype/demo flows
- development and local UI/API dependencies

For isolated local state, set `AETHERMESH_HOME`:

```bash
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh init
```

By default, local node config, identity, data, and logs live under `~/.aethermesh`.

## Quick start

Initialize local state:

```bash
aethermesh init
```

Check status:

```bash
aethermesh status
aethermesh node status
```

Start the local node API/dashboard on localhost:

```bash
aethermesh node start
```

Open the local dashboard:

```bash
aethermesh ui
```

The local API binds to `127.0.0.1:7280` by default.

## Prototype flow examples

Run one local demo job without installing the package:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli run-demo --node-id local-demo-node --message "hello mesh"
```

Run the deterministic local multi-node simulation:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli simulate-local
```

Run a manifest-backed local batch:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli run-local-batch --manifest examples/local-batch.json
```

Run the full local dispatch, worker, ledger, receipt, and audit artifact flow:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli run-local-flow --manifest examples/local-batch.json --output-dir /tmp/aethermesh-local-flow
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli audit-local-flow --output-dir /tmp/aethermesh-local-flow
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m aethermesh_core.cli aggregate-local-flow --output-dir /tmp/aethermesh-local-flow
```

Use CLI help for the full command list:

```bash
aethermesh --help
aethermesh-core --help
```

## Testing

Run the unit test suite:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python -m unittest discover -s tests -v
```

Run the repo quality gate list:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python scripts/full_test.py --list
```

Run the default full test wrapper:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python scripts/full_test.py
```

The project is configured for strict deterministic behavior and 100% branch coverage in CI.

## Repository layout

```text
src/aethermesh_core/       Core Python package
examples/                  Local manifests and fixture data
docs/                      Architecture and local prototype docs
scripts/                   CI, release, and full-test helpers
tests/                     Unit and integration-style prototype tests
desktop/                   Electron desktop launcher and packaging work
.github/                   Issue templates, PR template, and GitHub Actions
```

## Documentation

Start here:

- [Architecture](docs/architecture.md)
- [Persistent goal](docs/persistent-goal.md)
- [CLI and local UI](docs/ui-and-cli.md)
- [Local node identity](docs/local-node-identity.md)
- [Local node lifecycle](docs/local-node-lifecycle.md)
- [Desktop launcher](desktop/docs/desktop.md)

## Development principles

Keep changes small, validated, and local-first.

Prefer:

- Runnable behavior over placeholders
- Deterministic local flows before distributed claims
- Validation-gated contribution tracking before rewards
- Simple routing and transport seams before production networking
- Clear tests and docs before broad abstraction

Avoid:

- Tokenomics before useful validated work exists
- Dashboard polish before core runtime behavior
- Unsupported claims about decentralization
- Large rewrites that do not improve the local proof
- Treating AER as monolithic MoE inside one model

## License

License information has not been added yet.
