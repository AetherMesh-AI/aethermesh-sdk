# Two-machine peer operations and application handoff

This runbook prepares a controlled direct connection between two real machines.
It is a deployment plan, not evidence that such a connection has been tested.
Current acceptance uses isolated installed packages, separate processes and
loopback TLS. No discovery, NAT traversal, relay or automatic reconnect exists.
Keep GitHub available and keep received bundles opaque.

## Inputs and permissions before a real run

Record the following without putting private keys in tickets or chat:

| Input | Required decision |
| --- | --- |
| Machines A and B | Operator, OS, Python 3.11+ with TLS 1.3, and approved access to each machine; two processes on one machine do not qualify. |
| Installation | Exact reviewed wheel and its independently verified SHA-256 on both machines; permission for separate application-owned virtual environments. Package name is `aethermesh`, import name is `aethermesh_core`. |
| Endpoint | A's specific reachable interface address, selected TCP port, and B's route to it. Prefer an existing approved private LAN or VPN. `127.0.0.1` is not a remote endpoint. |
| Exposure | Explicit permission to start A's foreground listener on that interface and for B to connect. If routing/firewall/VPN changes are needed, obtain separate approval; this runbook makes none. Avoid a wildcard/public listener. |
| Credentials | Distinct existing operator-managed A/server and B/client PEM certificates, matching unencrypted private keys, and appropriate CA trust files. A's SAN must match B's configured server name; certificates need valid dates and role usages. |
| Trust selection | Independently verified A and B leaf-certificate pins, project ID, permitted readers, certificate expiry/rotation owner, and trusted channel for exchanging public certificates and digests. Never exchange private keys. |
| Content | Explicitly approved nonsecret file up to 8 MiB, or a complete trusted local SHA-1 repository, full branch ref and pinned commit. Sharing a bundle shares reachable history, including historical secrets if present. |
| Local storage | Owned physical directories outside active application checkouts, supported POSIX filesystem, sufficient free space for snapshot/checkpoints/output, and explicit retention/cleanup owner. |

No GPU or model runtime is needed. Windows and filesystems lacking no-follow,
directory descriptors, hard links, directory fsync or advisory locks are not
validated transfer targets. Use an already available compatible environment;
this runbook does not install a VM/container or change security settings.
Credential issuance, persistent trust enrollment, certificate rotation, service
installation and network exposure are separate operator actions requiring their
specific authorization. Test fixtures are not deployment credentials.

## Install and preflight

After installation permission, create a separate virtual environment on each
machine and install the selected, hash-verified wheel using its physical path.
Use the same reviewed build for the first acceptance; an older v2 client may
reject transfer capabilities even though it supports diagnostic connections.

```sh
python3 -m venv peer-venv
peer-venv/bin/python -m pip install /physical/path/to/approved/aethermesh-VERSION-py3-none-any.whl
peer-venv/bin/python -c 'import ssl; from importlib.metadata import version; print(version("aethermesh"), ssl.OPENSSL_VERSION, ssl.HAS_TLSv1_3)'
peer-venv/bin/aethermesh-peer --help
peer-venv/bin/aethermesh-peer fingerprint --certificate /physical/path/to/local-cert.pem
```

`VERSION` and uppercase values below are placeholders, not defaults or commands
to run unchanged. Use the environment's `aethermesh-peer` executable throughout.
Fingerprint output has `trust_established: false`: compare public certificate
pins through the independently trusted channel. Both operators confirm matching
project selection and intended readership before opening a listener. Do not
repair authentication failures by disabling verification or copying another
machine's private key. Node identity and hardware sharing are unnecessary here.

## First connection: A serves, B connects

On A, after listener authorization, run in a dedicated foreground terminal:

```sh
aethermesh-peer serve --project PROJECT_ID \
  --certificate A_SERVER_CERT.pem --private-key A_SERVER_KEY.pem \
  --trust-store A_CLIENT_CA.pem --allow-peer B_CLIENT_LEAF_SHA256 \
  --host A_APPROVED_INTERFACE_IP --port 7443
```

Wait for the flushed readiness JSON. It proves local startup, not B's reachability.
On B, using A's routeable address and certificate SAN name:

```sh
aethermesh-peer check --project PROJECT_ID \
  --certificate B_CLIENT_CERT.pem --private-key B_CLIENT_KEY.pem \
  --trust-store B_SERVER_CA.pem --peer-fingerprint A_SERVER_LEAF_SHA256 \
  --host A_REACHABLE_IP --port 7443 --server-name A_CERTIFICATE_SAN_NAME \
  --timeout 5
```

`--host` chooses the TCP destination; `--server-name` independently chooses the
certificate DNS/IP SAN to verify. Do not leave `localhost` as the server name
unless it is the deliberately issued and verified SAN. Success is exit 0 and
one JSON record with `status: "ok"`, selected project, server pin, negotiated
protocol and capabilities. No listener or saved profile is created on B.
Ctrl+C on A ends hosting. Repeat B's check after shutdown and record its bounded
failure; do not require one particular error code from different networks.

## Selected bytes and repository export

For a repository, A first runs the [trusted export](bundle-exchange.md) command
against an approved source. Do not export the active Eidolon checkout or ingest
a received repository to prepare this test. Select the full commit OID through
local review; the exporter requires that it still matches the full branch ref.

```sh
aethermesh-peer bundle-export --project PROJECT_ID \
  --source /physical/path/to/approved/trusted-repository \
  --ref refs/heads/SELECTED_BRANCH --expected-oid FULL_40_HEX_COMMIT_OID \
  --output selected.bundle --timeout 15
```

Record the export's `sha256`, `size`, `ref` and `claimed_oid`. No output inside
source Git metadata is allowed. Unsupported/shallow/partial/alternate/linked
worktree sources fail; do not silently hydrate them or relax the exporter.
A full history exceeding the 8-MiB limit is outside this milestone.

Stop the status-only service, then explicitly restart A with the selected file:

```sh
aethermesh-peer serve --project PROJECT_ID \
  --certificate A_SERVER_CERT.pem --private-key A_SERVER_KEY.pem \
  --trust-store A_CLIENT_CA.pem --allow-peer B_CLIENT_LEAF_SHA256 \
  --host A_APPROVED_INTERFACE_IP --port 7443 \
  --share-transfer selected.bundle --content-budget 33554432
```

The 32-MiB lifetime budget is an explicit example, charged for manifest/chunk
responses before send, including cancelled or repeated responses. Restarting
creates a new budget. Every allowed peer can read the entire selected snapshot.
Use separate services/allowlists if project readership differs.

A conveys the readiness `manifest_sha256` and independently selected whole-file
SHA-256 to B through the trusted channel. These are **different digests**: one
identifies the canonical manifest, the other the bundle bytes. Neither proves
publisher authority. On B, in an owned physical working directory:

```sh
aethermesh-peer fetch-transfer --project PROJECT_ID \
  --certificate B_CLIENT_CERT.pem --private-key B_CLIENT_KEY.pem \
  --trust-store B_SERVER_CA.pem --peer-fingerprint A_SERVER_LEAF_SHA256 \
  --host A_REACHABLE_IP --port 7443 --server-name A_CERTIFICATE_SAN_NAME \
  --manifest-sha256 EXPECTED_MANIFEST_SHA256 \
  --staging new-checkpoints --output received.bundle --timeout 30

aethermesh-peer bundle-inspect --project PROJECT_ID \
  --file received.bundle --sha256 EXPECTED_WHOLE_BUNDLE_SHA256
```

The staging directory must be new for the first fetch; its parent must already
exist. On interruption, repeat the exact fetch with `--resume`, the same manifest
and staging path, and an absent output path. Existing chunks are rehashed. Do not
delete checkpoints just because a connection failed. If output already exists
after an error, publication may have committed: inspect its selected hash before
choosing a new output name. Never automatically overwrite or roll back output.

Successful fetch reports `integrity_verified: true`, `publisher_authorized:
false` and `chunks_fetched`. Inspection must still report `pack_validated: false`
and `publisher_authorized: false`; compare header claims to the locally selected
ref/OID. Do not run `git bundle verify`, `clone`, `fetch`, `index-pack`, checkout,
archive extraction, build or installation on received packs. No received bytes
are passed to Git until a separate containment implementation is reviewed.

## Diagnose without changing trust

| Observation | Next bounded check |
| --- | --- |
| `invalid_configuration` | Check selected local paths, private-key/certificate pairing, PEM inputs, physical path components, supported filesystem and argument limits. Do not log key contents. |
| `authentication_failed` | Operators compare independent pins, CA selection, certificate dates, SAN/server name and A's client allowlist. A server-side rejection may instead appear as connection closure. |
| `peer_unavailable`, `connection_closed` or `timeout` | Check A's readiness and selected address/port, then ask the network owner to verify the approved route. Dropped packets, project rejection and authentication rejection can be indistinguishable to B. |
| `protocol_error` | Compare exact installed builds and project ID; older strict v2 capability whitelists may reject transfer-enabled services. Do not force a protocol downgrade for transfer. |
| `remote_error` during fetch | Check advertised transfer capabilities, selected manifest and remaining serving budget. A diagnostic success does not guarantee transfer availability. |
| `output_exists`, `local_io_error` or local staging rejection | Check ownership, private permissions, absent output and unchanged manifest selection. Preserve suspicious state for local inspection; do not auto-delete arbitrary files. |

Check/fetch exit codes: 0 success, 1 operation/configuration failure, 2 syntax,
130 interruption. Error JSON is static and does not pinpoint every TLS or network
cause. `--timeout` bounds network awaits, not hard synchronous filesystem latency.
There is no automatic retry; choose a finite manual attempt count and deadline.
Never print credential contents, disable pin/hostname checks, open firewall ports,
or install background services as an automatic diagnostic remedy.

## Evidence needed to call it two-machine acceptance

Record UTC times, two machine labels/OS versions, Python/OpenSSL and exact wheel
hash/version, the selected project, intended direction, approved interface/port,
public pins and expected content digests (retain sensitive topology privately).
Save exit codes and JSON reports, with no keys or private source paths in shared
logs. Complete these steps on actual distinct machines:

1. Status succeeds from B to A; a deliberately incorrect pin and project fail
   using command arguments only, without changing stored trust.
2. Approved bytes transfer with exact manifest and whole-file integrity. For a
   bundle, inspection preserves the unvalidated/unauthorized flags.
3. Interrupt a fetch before completion, preserve private checkpoints, explicitly
   restart/resume, and record successful integrity plus fewer fetched chunks.
   If transfer finishes before interruption, report that resume was not exercised;
   do not claim success or alter network settings to manufacture it.
4. Stop A; B fails within its bounded connection attempt. Restart explicitly and
   reconnect; no automatic service restart or retry is implied.
5. If bidirectional sharing is required, separately authorize B's listener and
   A's client role, supply certificates with the appropriate usages and allowlists,
   then repeat in reverse. One successful direction proves only that direction.

Until those results exist, deployment remains blocked on the selected second
machine, operator-provided credentials/trust, reachable endpoint and explicit
installation/listener permissions. SDK loopback results cannot resolve those
inputs. Existing GitHub access remains the source/review fallback throughout.

See [the application integration contract](peer-integration-contract.md) for the
separate Eidolon handoff. No application checkout is changed by this runbook.
