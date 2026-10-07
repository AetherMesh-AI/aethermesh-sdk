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
versions are separate namespaces. This protocol's version is `1`; connecting to
a project does not adopt that project's release channel or publisher authority.

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
when certificates rotate. These APIs do not issue identities, manage revocation,
rotate keys, or discover whom an operator should trust.

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

## Protocol and resource limits

The authenticated stream carries strict JSON objects framed by a four-byte
unsigned network-order payload length. Frames must be nonempty and at most
65,536 bytes. Duplicate JSON keys, non-finite numbers, malformed UTF-8, invalid
schemas, unsupported protocol versions, mismatched projects, and reused request
IDs are rejected. There is no pickle, arbitrary code execution, provider loading,
or implicit operation dispatch.

Envelopes have exact key sets. The hello offers one or more integer versions;
welcome selects a supported version. Request IDs are positive, strictly
increasing integers within a connection and cannot be reused, even after a
request completes. Result/error responses correlate to that request ID.

```json
{"type":"hello","versions":[1],"project":"example-project"}
{"type":"welcome","version":1,"project":"example-project","capabilities":["echo","status"]}
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

`status` reports protocol version and advertised capabilities. Optional `echo`
accepts an object with `text` (at most 4,096 UTF-8 bytes) and optional `delay_ms`
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
The ordinary unittest suite discovers these tests.

```sh
PYTHONPATH=src python -m unittest tests.test_network_integration -v
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
