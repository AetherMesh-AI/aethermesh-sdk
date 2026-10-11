# Small project content exchange

`content.get.v1` is an opt-in protocol-v2 operation for fetching opaque bytes
from an explicitly trusted TLS peer. It is a foundation for source/artifact
exchange, not a release system or a replacement for GitHub. Keep GitHub available
while validating this path. The separate opt-in [bounded resumable transfer](resumable-content.md) supports
larger opaque snapshots. Git integration, discovery and publisher verification
remain follow-up work.

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

## Installed small-content CLI

The installed CLI can explicitly share one selected regular file. Sharing has no
default byte budget: both flags are required. The file is read once into the
bounded in-memory snapshot before listening; later disk edits are not seeded.

```sh
aethermesh-peer serve --project aethermesh-sdk \
  --certificate SERVER.pem --private-key SERVER.key --trust-store CA.pem \
  --allow-peer CLIENT_PIN --share-file selected.bin --content-budget 1048576
```

The default is a foreground loopback listener on an ephemeral port. Its readiness
JSON adds `content` (the selected reference) and `content_budget_bytes` only when
sharing was explicitly enabled. It never advertises the local source path.
Every admitted peer can request this snapshot; choose the allowlist accordingly.
The budget is 0..67,108,864 bytes for this invocation; restarting this command
constructs a new store and budget. The underlying SDK rules for per-item size,
request limits, empty items and cancelled-response accounting still apply.

In another process, use an independently selected project/hash/size reference:

```sh
aethermesh-peer fetch --project aethermesh-sdk \
  --certificate CLIENT.pem --private-key CLIENT_KEY.pem --trust-store CA.pem \
  --peer-fingerprint SERVER_PIN --port PORT --server-name localhost \
  --sha256 EXPECTED_HASH --size SIZE --output received.bin --timeout 5
```

`fetch` verifies the complete response before writing anything. It reports
`project`, `sha256`, `size`, `saved: true`, `integrity_verified: true`, and
`publisher_authorized: false`; it never renders received bytes to the terminal.
The peer supplies no filename. There is no archive extraction, code execution,
automatic build, import, installation, directory seeding or repository sync.

File handling is deliberately strict:

- Input must be a regular file of at most 32 KiB; directories, FIFOs, devices,
  symlinks and oversized inputs fail. Reads remain bounded if the file grows.
- Both paths reject `..` and symlink components, including parent directories.
  Use physical directory paths or relative paths from the intended working
  directory. For example, macOS `/tmp` is a symlink; use `/private/tmp` or a
  relative path instead. Existing parent directories are required.
- The chosen parent directory is held open across the transfer. Replacing its
  path with a symlink cannot redirect publication; a concurrent rename leaves
  the original directory as the destination. Choose directories you own and
  trust. This is not a sandbox against a hostile local user or filesystem.
- Existing destinations, including dangling symlinks, are rejected before
  connecting. Publication also atomically refuses a destination created during
  transfer. Existing files are never replaced or truncated.
- After verification and connection cleanup, a same-directory temporary file
  is written with mode 0600 (subject to umask), then atomically hard-linked to
  the requested new name. Failed publication cleans its temporary file. A crash
  can leave a temporary file; no crash-durability or rollback guarantee is made.
  Linking is the commit point: later cleanup/output errors or interruption may
  leave the complete destination in place. Inspect it before retrying.
- These file commands require POSIX directory-descriptor/no-follow support and
  same-directory hard links (tested on macOS and Linux). Unsupported platforms
  or filesystems fail closed. SDK byte exchange and ordinary diagnostics retain
  their existing platform behavior.

The connection/request deadline and diagnostic exit-code conventions also apply
to `fetch`. Bounded regular-file I/O is synchronous and outside that network
deadline. Additional static stderr error codes are `output_exists` and
`local_io_error`; remote refusal, including budget exhaustion, is `remote_error`.
Cancellation before publication creates no destination. Once a response owns
its peer-side write, cancellation may not stop already-served bytes or refund
its budget; it does not publish a locally cancelled fetch.

The hosted installed-wheel acceptance test now transfers a 32 KiB binary file
between separate CLI processes, verifies exact bytes and private permissions,
and checks overwrite/symlink refusal and budget exhaustion. The TLS backpressure
test pauses client reads and observes actual buffered output and paused writes,
then cancels the affected fetch. It verifies cancellation delivery, no duplicate
response, no serving-budget refund, and successful session recovery after reads
resume. This is loopback evidence, not Internet or two-machine validation.

## Earlier Python example


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

See [bounded resumable transfer](resumable-content.md) for chunked opaque files.
Git bundle integration and signed publisher/freshness policy remain separate slices. Build scheduling, sandbox execution, app orchestration
and installation remain outside this SDK.
