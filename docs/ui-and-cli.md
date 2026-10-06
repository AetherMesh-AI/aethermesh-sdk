# Diagnostic CLI and local JSON API

The Python package provides a local development CLI and headless API over
`NodeRuntimeService`. UI, native service registration and installers belong to
consumer applications. The filename is retained so existing documentation links
continue to resolve.

## Install and run

From a source checkout:

```bash
python -m pip install -e ".[dev,api]"
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh init
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh status
AETHERMESH_HOME=/tmp/aethermesh-dev aethermesh node start
```

`aethermesh node start` runs the foreground API at `127.0.0.1:7280`. Stop that
foreground server with Ctrl+C. `aethermesh node stop` records local stopped state;
it is not a process supervisor. No command registers or controls OS services.
Without `AETHERMESH_HOME`, runtime state defaults to `~/.aethermesh`.

Other diagnostics are `aethermesh --version`, `node status`, `peers` and `jobs`.
`aethermesh-core --help` lists deterministic local flow tools.
`aethermesh-node` remains an alias for the same CLI. The legacy `[ui]` extra
aliases `[api]`; the `ui` command and bundled dashboard are retired.

## API boundary

`GET /` is a JSON service index. `GET /health`, `/api/status`, `/api/node`,
`/api/peers`, `/api/capabilities`, `/api/jobs`, `/api/network`, `/api/package`,
`/api/logs` and `/api/events` expose local development state. Existing aliases and
schema/provenance inspection endpoints remain available. Inspect `/openapi.json`
for the actual endpoint inventory and
[the API contract](phase-1-api-schema-contract.md) for schema details.

`POST /api/jobs` records and validates a local work submission; it does not
execute arbitrary code or establish remote AI execution. Legacy `POST /shutdown`
and `/restart` set process-control request flags only; a consumer supervisor must
explicitly handle them. They are not native installation/relaunch implementations.

Keep this unauthenticated API on localhost. Do not expose it to LAN/public peers
without authentication, authorization, privacy and resource-limit design. It is
not the future public P2P transport.

For SDK use without runtime state or a server, see `examples/sdk_smoke.py`.
For the legacy package updater limitations and all retired behaviors, see
[cleanup compatibility](sdk-scope.md#cleanup-and-compatibility).
