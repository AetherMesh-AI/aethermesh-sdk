# Authenticated peer foundation

This slice provides real TCP connectivity between an application SDK and an
independently running, opt-in reference peer service. It is a diagnostic network
foundation, not a deployed public P2P network or an AI execution service.

## Component boundary

`aethermesh_core.network` is an application-neutral Python API:

- `PeerClient` owns outbound sessions, capability negotiation, correlated
  requests, bounded deadlines, cancellation, and explicit reconnects.
- `PeerService` is the separately started reference service. It exposes `status`
  and, only when explicitly enabled, deterministic diagnostic `echo`.
- `TLSIdentity`, `PeerEndpoint`, `Limits`, and the protocol codec are shared
  client/service foundations. Importing or constructing the SDK does not start
  a listener, host work, or consent to contributing resources.

The lightweight client is the default application integration. Applications
that wish to host a service opt in separately by constructing `PeerService` or
launching `aethermesh-peer serve`. Neither path starts model engines, installs OS
services, or orchestrates application agents. The installed SDK is usable by
Eidolon or another application independently of either application's releases.

SDK/package version, peer protocol version, schema versions, and project release
versions are separate namespaces. This protocol prefers version `2`, with a
version `1` fallback for diagnostic peers without node-profile support. Connecting
to a project does not adopt that project's release channel or publisher authority.

## Configure identities and trust explicitly

Each side needs an operator-provided PEM certificate, corresponding unencrypted
private key, and PEM trust store. Password-protected keys fail without prompting;
interactive key-password entry is deliberately unsupported. TLS 1.3 mutual certificate authentication is mandatory.
The client checks the server certificate against its trust store, verifies its
configured DNS name or IP SAN, and compares the leaf certificate's SHA-256
fingerprint with the explicitly configured pin. The service validates the client
certificate chain and requires its leaf fingerprint in its allowlist.

Both participants must use the same project ID. The authenticated connection
then negotiates protocol version and diagnostic capabilities. A matching project
string scopes the session; it is not proof of project membership or permission
to publish a release.

Exchange trust roots, peer fingerprints, and project IDs over an independently
trusted channel. Use distinct client/server identities. Restrict private-key
files to their owner, arrange certificate renewal, and explicitly replace pins
when certificates rotate. These APIs do not issue TLS credentials, manage
revocation, rotate keys, or discover whom an operator should trust. Optional local
node identities, described below, are separate from certificate authentication.

Compute a fingerprint from a certificate already verified through that trusted
channel:

```python
from pathlib import Path
from aethermesh_core.network import certificate_fingerprint

print(certificate_fingerprint(Path("server.pem")))
```

Pins hash the DER-encoded leaf certificate, not the text of its PEM file. A TLS
peer identity is not artifact publisher authority. Artifact hashes, project
release authorization, signed manifests, provenance policy, rollback protection,
and metadata freshness require a separate implementation and review.

## Run the service and an installed client

Install the package into a Python 3.11+ environment with OpenSSL supporting TLS
1.3. Provide real operator-managed identity files; no identity is generated
implicitly by the application. The following examples bind only to loopback.
Replace the uppercase values with your own paths, pins, and project identifier.

```sh
python -m pip install .
aethermesh-peer serve \
  --certificate SERVER_CERT.pem --private-key SERVER_KEY.pem \
  --trust-store CA.pem --allow-peer CLIENT_LEAF_SHA256 \
  --project PROJECT_ID --host 127.0.0.1 --port 7443 --enable-echo
```

The service prints one flushed JSON readiness record with `host`, `port`,
`project`, and `capabilities`. A port of `0` asks the OS for an available port;
read the assigned port from that record. The service runs in the foreground;
Ctrl+C stops it. Omit `--enable-echo` for status-only behavior. Hosting is always
an explicit action.

In a separate process, using the installed package:

```sh
python examples/network_client.py \
  --certificate CLIENT_CERT.pem --private-key CLIENT_KEY.pem \
  --trust-store CA.pem --peer-fingerprint SERVER_LEAF_SHA256 \
  --project PROJECT_ID --host 127.0.0.1 --port 7443 \
  --server-name localhost --text 'hello from an independent SDK process'
```

The example performs `status`, then optional `echo`, prints their JSON results,
and closes its connection. Omitting `--text` performs only `status`. It can run
outside the repository when copied alongside an installed package; it does not
modify Python's import path or depend on repository fixtures.

Application code can use the same API directly:

```python
from pathlib import Path
from aethermesh_core.network import PeerClient, PeerEndpoint, TLSIdentity

identity = TLSIdentity(Path("client.pem"), Path("client.key"), Path("ca.pem"))
endpoint = PeerEndpoint("127.0.0.1", 7443, "localhost", "SERVER_LEAF_SHA256")

async def diagnose():
    async with PeerClient(identity, project_id="example-project") as client:
        await client.connect(endpoint)
        status = await client.request("status", timeout=5.0)
        if "echo" in client.capabilities:
            result = await client.request("echo", {"text": "hello"}, timeout=5.0)
            return status, result
        return status, None
```

Unknown or disabled operations fail honestly. Capability advertisement describes
these diagnostic handlers only; it is not evidence of inference capacity.

## Optional saved node identity and coarse hardware

Node metadata is opt-in. Without `node_profile` or `--node-identity`, neither
client nor service loads or creates a node identity or probes hardware. Connecting
still requires the same TLS certificate, trust store, hostname and certificate
pin/allowlist checks. The identity metadata is exchanged only after these checks.

`load_node_profile(Path("identity.json"))` reads the existing local runtime identity
format produced by `aethermesh_core.identity.load_or_create_identity`. It returns
an immutable `NodeProfile(node_id, node_name, hardware=None)`. The saved ID and name
are reused verbatim without rewriting the file. An older identity without a name
keeps `node_name=None`; loading does not generate a replacement name or migrate the
file. Missing or invalid identity files fail rather than being silently replaced.
This loader is for the runtime identity format, not a generic metadata document.

New local identities can be created explicitly with
`load_node_profile(path, create=True)`, or service flags
`--node-identity PATH --create-node-identity`. Creation uses the existing
hardware-derived ID and readable-name generator. Existing files are still reused.
This creates a local identity document only; TLS certificates must still be
provided independently. A saved identity remains stable when hardware changes.

To share current coarse hardware, additionally pass `include_hardware=True` or
`--share-hardware`. This is a separate disclosure choice, requires an explicit
identity path in the CLI/example, and never rewrites the saved ID/name. The four
hardware fields are strictly allowlisted:

- `cpu_architecture`: `x86_64`, `aarch64`, `arm`, `x86`, `other` or `unknown`
- `ram_gb_bucket`: `unknown`, `under16`, `16to31`, `32to63`, `64to127` or `128plus`
- `gpu_available`: a boolean or `null` for unknown; the current collector reports
  `true` when evidence exists, otherwise `null`, rather than claiming absence
- `gpu_vram_gb_bucket`: `unknown`, `under8`, `8to15`, `16to31` or `32plus`

Raw MAC addresses, serial/device identifiers, CPU/GPU model strings, private keys,
local file paths, provenance and unrelated identity-file metadata are not part of
the profile. Hardware collection can inspect more information locally to derive
those coarse categories; its raw inputs are not serialized onto the peer protocol.
The pre-existing node ID itself is hardware-derived and potentially linkable
across sessions or projects. It is a stable pseudonymous identifier, not anonymous
or proof that the same physical machine is connected. The readable name and
coarse hardware categories can also aid correlation. Choose peers and disclosure
settings accordingly, even when hardware sharing is disabled.

```python
from pathlib import Path
from aethermesh_core.network import (
    PeerClient, PeerEndpoint, PeerService, TLSIdentity, load_node_profile,
)

client_tls = TLSIdentity(Path("client.pem"), Path("client.key"), Path("ca.pem"))
server_tls = TLSIdentity(Path("server.pem"), Path("server.key"), Path("ca.pem"))
profile = load_node_profile(Path("existing-identity.json"))
endpoint = PeerEndpoint(
    "127.0.0.1", 7443, "localhost", server_certificate_pin,
    expected_node_id="expected-server-node-id",
)

async def inspect_peer():
    async with PeerClient(
        client_tls, project_id="example-project", node_profile=profile
    ) as client:
        await client.connect(endpoint)
        info = client.peer_info
        # info.fingerprint comes from the authenticated TLS certificate.
        # info.profile is the bounded, self-reported handshake claim.
        # info.identity_pinned says an explicitly configured ID matched that claim.
        return info, await client.request("status")

service = PeerService(
    server_tls,
    project_id="example-project",
    allowed_peers=frozenset({client_certificate_pin}),
    node_profile=profile,
    expected_peer_ids={client_certificate_pin: "expected-client-node-id"},
)
# Constructing the service does not start its listener.
```

Replace the pin variables above with independently verified SHA-256 leaf
fingerprints. `peer_info` and
`protocol_version` are available after successful negotiation and clear when the
client disconnects. `service.peers` is an immutable tuple of active `PeerInfo`
records, one per authenticated session. Multiple sessions may claim identical
names or IDs, including sessions from different certificates. Claims do not
replace another session's record or grant another certificate admission. Names
must never be used as ownership, takeover or authorization keys.

To bind an expected node ID to a known certificate, set
`PeerEndpoint(..., expected_node_id=...)` on the client and/or
`PeerService(..., expected_peer_ids={fingerprint: node_id})` on the service.
The CLI equivalent is repeated `--expect-peer-id LEAF_SHA256=NODE_ID`, alongside
`--allow-peer LEAF_SHA256`. A missing or mismatched claim fails negotiation;
version 1 cannot satisfy an expected node-ID binding. The pin/ID mapping must be
configured independently: the service never learns trust automatically from a
peer's claim. All names, IDs and hardware details remain self-reported, even
when `identity_pinned=True`. A matching configured claim is not hardware
attestation, proof of capacity, certificate ownership beyond TLS, or publisher
authority. Only diagnostic `status` and optional `echo` are implemented.

Add optional flags to the earlier service/client commands:

```sh
# Existing service identity; no generation or coarse hardware disclosure.
aethermesh-peer serve ... --node-identity SERVER_IDENTITY.json \
  --expect-peer-id CLIENT_LEAF_SHA256=EXPECTED_CLIENT_NODE_ID

# Existing client identity and a pinned server node claim.
python examples/network_client.py ... --node-identity CLIENT_IDENTITY.json \
  --expected-node-id EXPECTED_SERVER_NODE_ID
```

The `...` represents the required TLS/project/address arguments shown above.
The example never creates a missing identity. When `--node-identity` or
`--expected-node-id` is supplied it prints a `peer_info` JSON record before
`status` and optional `echo`; without those flags its original output record
count is unchanged. Hardware sharing is disabled unless explicitly requested.

## Protocol and resource limits

The authenticated stream carries strict JSON objects framed by a four-byte
unsigned network-order payload length. Frames must be nonempty and at most
65,536 bytes. Duplicate JSON keys, non-finite numbers, malformed UTF-8, invalid
schemas, unsupported protocol versions, mismatched projects, and reused request
IDs are rejected. There is no pickle, arbitrary code execution, provider loading,
or implicit operation dispatch.

Envelopes have exact key sets. The hello offers one or more integer versions;
welcome selects a supported version. Version 2 then exchanges an `identify`
frame and a `ready` acknowledgement before any request is accepted. Request IDs
are positive, strictly increasing integers within a connection and cannot be
reused, even after a
request completes. Result/error responses correlate to that request ID.

```json
{"type":"hello","versions":[2,1],"project":"example-project"}
{"type":"welcome","version":2,"project":"example-project","capabilities":["echo","status"],"node":{"node_id":"server-id","node_name":"server-name","hardware":null}}
{"type":"identify","node":{"node_id":"client-id","node_name":null,"hardware":null}}
{"type":"ready"}
{"type":"request","id":1,"operation":"echo","payload":{"text":"hello"}}
{"type":"result","id":1,"result":{"text":"hello"}}
{"type":"request","id":2,"operation":"echo","payload":{"text":"later","delay_ms":1000}}
{"type":"cancel","id":2}
{"type":"error","id":2,"code":"cancelled","message":"request cancelled"}
```

Each displayed object is its own length-prefixed frame, not a JSON-lines
transport. Errors use stable codes such as `unsupported_operation`,
`invalid_request`, `busy`, and `cancelled`. Invalid negotiations or malformed
frames close the connection instead of returning an untrusted request ID.

An omitted local profile is encoded as `"node":null` in version-2 welcome and
identify frames. The hello always has exactly `type`, `versions`, and `project`;
it never contains node metadata. A version-1 peer receives the original welcome,
status and request shapes, with no `node`, `identify` or `ready`. A new client
can therefore connect to a version-1 server without disclosing its configured
local profile. Conversely, a new server accepts an old client when no configured
expected node-ID binding requires version 2. Capability lists remain `status`
and optional `echo` in both versions; metadata support is indicated by protocol
version, not an advertised inference or hosting capability.

`status` reports protocol version and advertised capabilities. In version 2 it
also includes `node`, which must equal the server profile accepted during the
handshake. A changed claim closes the client connection rather than silently
replacing the authenticated session's metadata. Reconnect to negotiate a changed
profile. In version 1 the status object retains exactly its original two fields.
Optional `echo` accepts an object with `text` (at most 4,096 UTF-8 bytes) and optional `delay_ms`
(an integer from 0 through 1,000), and returns `{ "text": "..." }`. Delay is a
bounded diagnostic used to exercise timeouts and cancellation.

`Limits` defaults to 16 connections, 8 concurrent requests per connection, 1,024
requests per connection, a 5-second handshake deadline, and a 30-second I/O
deadline. The client's per-request timeout defaults to 10 seconds. Local
cancellation or deadline expiry sends a best-effort protocol cancellation;
service shutdown cancels and awaits admitted sessions and work it owns.
TLS handshakes already accepted by asyncio retire within `handshake_timeout`.
Python 3.11 may return from service shutdown before those pre-handshake sockets
retire; later runtimes wait for them. A stopped listener's delayed handshake is
never admitted into a restarted listener generation. Cancellation does not promise
that an already completed operation can be undone.

Use `PeerError` subclasses to distinguish authentication failures
(`AuthenticationError`), protocol violations (`ProtocolError`), disconnections
(`ConnectionClosed`), deadline expiry (`RequestTimeout`), and peer-reported
operation errors (`RemoteError`). Applications reconnect explicitly with
`await client.connect(endpoint)` after a disconnect. There is no unbounded
background retry loop, automatic replay, or exactly-once side-effect guarantee.

## Evidence and reproducible checks

`tests/test_network_integration.py` runs a service in a separate OS process and
an SDK in the test process, using real loopback TLS sockets. It exercises useful
requests, unsupported operations, timeout/cancellation, shutdown, reconnect, and
negative authentication/protocol cases. These are not in-memory transport mocks.
`tests/test_network_identity_integration.py` adds saved ID/name reuse across
service restarts without file writes, a separate client example process, matching
certificate/ID bindings, spoofed or missing claims, coarse-metadata wire capture,
real TLS version-1 fallback without identity disclosure, and rejection of a
status identity swap. Service tests additionally verify distinct session records
for duplicate claims. The ordinary unittest suite discovers these tests.

```sh
PYTHONPATH=src python -m unittest \
  tests.test_network_integration tests.test_network_identity_integration -v
```

Tests require the installed official `openssl` CLI and TLS 1.3 in Python's SSL
module. `tests/network_test_support.py` generates a short-lived test CA and
separate server/client/unauthorized peer certificates inside a temporary
directory. CA constraints, SANs, and client/server extended key usages are
explicit. Private keys and trust files are deleted with that directory and are
never checked in. Missing TLS/OpenSSL dependencies or a forbidden socket bind
are failures to investigate, not skipped acceptance evidence.

## Security boundary and deliberate limitations

This foundation is suitable for controlled development environments with
preconfigured peers. It is not production Internet hardening. TLS handshakes
consume resources before application-level connection admission, and an
unauthenticated attacker can still target that layer. Operators need OS/network
rate limiting, firewall restrictions, certificate lifecycle policy, deployment
review, and monitoring before exposing a listener. Application limits do not
replace those controls.

Traffic confidentiality protects the wire, not a compromised endpoint. An
allowlisted peer can lie about its own state or echo result. Successful TLS and a
matching pin authenticate a configured certificate, not the integrity of the
machine, its outputs, or any project artifact. The diagnostic service does not
persist prompts, execute arbitrary work, or expose an administrative command
channel; nevertheless, send it only data the chosen peer is allowed to see.

Not implemented by this slice:

- Public peer discovery, routing, NAT traversal, relays, or cross-machine
  operational acceptance evidence
- AI inference, streaming model output, AER/REVA/AEF routing, or provider adapters
- Artifact transfer, distributed builds, signed project update distribution,
  package/OS installation, release governance, or publisher trust policy
- Model training, seeding, contribution scheduling, payments, or rewards

The existing local simulations, HTTP interfaces, and legacy update helpers keep
their separate compatibility behavior. They do not gain these TLS guarantees
merely by sharing the package namespace.
