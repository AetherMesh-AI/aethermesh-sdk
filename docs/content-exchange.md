# Small project content exchange

`content.get.v1` is an opt-in protocol-v2 operation for fetching opaque bytes
from an explicitly trusted TLS peer. It is a foundation for source/artifact
exchange, not a release system or a replacement for GitHub. Keep GitHub available
while validating this path. Large Git bundles, chunking, resume, discovery and
publisher verification are follow-up work.

## SDK usage

Supply operator-managed TLS identities and independently verified certificate
pins as described in [network foundation](network-foundation.md). Construction
and imports create no credentials, listeners, filesystem writes or background
services. The application explicitly starts the host:

```python
from aethermesh_core.network import ContentStore, PeerClient, PeerService

store = ContentStore("aethermesh-sdk", (b"small source snapshot",),
                     max_served_bytes=1048576)
reference = store.references[0]  # pass through an independently trusted channel
service = PeerService(server_tls, project_id="aethermesh-sdk",
                      allowed_peers=frozenset({client_pin}), content_store=store)

async def exchange():
    async with service:  # defaults to loopback and an ephemeral port
        # endpoint uses service.address, expected TLS hostname and server pin
        async with PeerClient(client_tls, project_id="aethermesh-sdk") as client:
            await client.connect(endpoint)
            data = await client.fetch_content(reference, timeout=5.0)
            return data  # still untrusted; no extraction, execution or installation
```

`ContentRef(project_id, sha256, size)` identifies expected content with a canonical
lowercase SHA-256 and exact size. The host accepts only an explicit tuple of
immutable byte strings, snapshots it in memory, and never accepts remote paths.
All certificate-allowlisted peers admitted to that service can retrieve the
selected bytes if they know the reference. Use separate services, stores and
allowlists when project readership differs. A project string is a namespace,
not proof of membership. There is no remote listing or upload operation.

Both raw `request("content.get.v1", reference.to_dict())` and `fetch_content`
validate the returned reference, exact length, canonical base64 and SHA-256.
Invalid responses close the session with `ProtocolError`; `fetch_content` returns
only verified bytes. Missing content or wrong length yields `RemoteError` code
`not_found`; another project yields `invalid_request`. A reference for another
project passed to `fetch_content` fails locally before sending anything.

## Limits, cancellation and compatibility

- 32,768 bytes per item, 64 input items, 1 MiB total input bytes per store.
- Serving budget defaults to 8 MiB, configurable from zero to 64 MiB. Every
  successful lookup consumes its size before responding, including transfers
  later cancelled or disconnected. Reusing the store across service restarts
  preserves the budget. Empty items consume no byte budget but still consume
  the session's bounded request count. Exhaustion returns `resource_limit`.
- Base64 responses fit the existing 65,536-byte frame limit. Connection,
  in-flight request, request-count and I/O limits apply unchanged. The store's
  small synchronous lookup/hash encoding does not start workers or disk I/O.
- Cancel the fetch asyncio task to send best-effort cancellation. Deadlines use
  the existing `RequestTimeout` contract. Cancellation cannot undo served bytes.
  Failed transfers return no partial bytes. Retry/reconnect is explicit and
  re-fetches the entire item; there is no persistent partial-file state.
- Protocol v1 remains status/echo only. Default v2 services are unchanged.
  Content-enabled v2 services advertise `content.get.v1`; older v2 clients that
  reject unfamiliar capabilities must upgrade. No silent protocol-version bump
  or claim of compatibility with those strict clients is made.

Budgets apply to one store instance in one process, not a global deployment.
Operators choose how many instances to construct. TLS resource exhaustion still
needs the deployment protections described in the network guide.

## Installed-package, two-process loopback example

Install a wheel in an isolated Python environment. Copy
`examples/network_content.py` outside the checkout and run it there. It imports
only the installed SDK and the standard library. Supply existing operator-managed
PEM files; this example never generates them or modifies trust.

```sh
python network_content.py serve --project aethermesh-sdk \
  --certificate SERVER.pem --private-key SERVER.key --trust-store CA.pem \
  --peer-fingerprint CLIENT_PIN --file selected-small-file --budget 1048576
```

The foreground host prints its loopback port and content reference. Independently
verify the expected reference; do not infer publisher authority from host output.
In a second process:

```sh
python network_content.py fetch --project aethermesh-sdk \
  --certificate CLIENT.pem --private-key CLIENT.key --trust-store CA.pem \
  --peer-fingerprint SERVER_PIN --port PORT --sha256 EXPECTED_HASH --size SIZE
```

The client prints an integrity report without rendering peer bytes or writing
files. Stop the host with Ctrl+C. The example deliberately exposes no external
bind option. Tests use ephemeral local test identities that are deleted after
validation; loopback evidence is not two-machine or Internet acceptance.

## Trust boundary and next steps

TLS authentication establishes the configured peer certificate. A hash establishes
byte integrity relative to the selected reference. Neither establishes an
artifact publisher, signed release, source revision, release channel, freshness,
rollback protection, safe archive paths, build provenance or code safety. Do not
automatically run, import, extract, install or build fetched content. AetherMesh
and Eidolon retain separate publisher authorities, release identities and channels.
No application checkout, Git configuration or release settings are changed.

Next slices: installed CLI diagnostics/setup ergonomics; bounded chunked transfer
and resumable Git bundles; separately reviewed signed project manifests and
publisher/freshness policy. Build scheduling, sandbox execution, app orchestration
and installation remain outside this SDK.
