# Public resumable-download API

`aethermesh_core.network.download_transfer` owns one outbound peer session and
retrieves one explicitly selected manifest into private checkpoints and a new
output file. It reuses the CLI's integrity, checkpoint and no-overwrite publication
machinery. It never hosts, reconnects automatically, enrolls trust, imports Git
packs, extracts archives, executes bytes or authorizes a publisher.

## Installed-package use

Use an approved installed package and existing operator-provided TLS files and
verified peer pin. The [two-peer runbook](two-peer-operations.md) covers deployment
inputs; the example is not permission to expose a listener or create credentials.
Paths must be physical, owned and supported by the POSIX file facilities described
in [resumable content](resumable-content.md). Parents must already exist.

```python
import asyncio
from pathlib import Path
from aethermesh_core.network import (
    DownloadProgress, PeerEndpoint, TLSIdentity, download_transfer,
)

async def report(progress: DownloadProgress) -> None:
    print(progress.phase, progress.verified_bytes, progress.manifest.size)

async def main() -> None:
    result = await download_transfer(
        TLSIdentity(Path("client.pem"), Path("client.key"), Path("ca.pem")),
        PeerEndpoint("127.0.0.1", 7443, "localhost", "VERIFIED_SERVER_LEAF_SHA256"),
        project_id="selected-project",
        manifest_sha256="INDEPENDENTLY_SELECTED_MANIFEST_SHA256",
        staging=Path("new-private-checkpoints"),
        output=Path("new-opaque-output"),
        timeout=30.0,
        on_progress=report,
    )
    print(result.output, result.integrity_verified, result.publisher_authorized)

asyncio.run(main())
```

Replace the uppercase placeholders. Project, endpoint and manifest selection
remain caller policy. Optional expected node-ID binding is provided through
`PeerEndpoint`; it still does not attest hardware or publisher authority. No
saved profile or hardware metadata is shared by this function.

A subsequent explicit call with `resume=True` must select the same staging path
and manifest digest. It rehashes cached chunks and fetches missing/corrupt ones.
An absent output path is required; success never replaces an existing file. No
private helper imports or application-owned chunk persistence are necessary.

## Progress and completion facts

`DownloadProgress` and `DownloadResult` are frozen dataclasses. Their `manifest`
is the immutable digest-verified `TransferManifest`, containing project, whole-file
SHA-256, size and ordered chunk digests. No remote filenames enter local paths.

| Progress field | Meaning |
| --- | --- |
| `phase` | `manifest` after manifest validation; `chunk` after one checkpoint is verified/saved; `verified` after complete assembly and whole-file verification. All precede publication. |
| `verified_bytes`, `verified_chunks` | Counts visited and verified in this invocation. Start at zero even on resume; increase as cached or fetched chunks are verified. Not wire throughput, and not a promise of output publication. |
| `chunks_fetched`, `chunks_reused` | Newly downloaded/saved chunks versus rehashed cached chunks in this invocation; their sum equals `verified_chunks`. |

Callbacks are async callables awaited serially in the download's own task. There
is no callback queue, hidden worker or event history retained by the SDK. At most
258 notifications occur: one manifest, up to 256 chunks and one full verification.
An empty file has only manifest/verified notifications. Applications choose their
own bounded UI/event storage. Callbacks must return promptly, cooperate with
cancellation and avoid blocking filesystem/CPU work on the event loop. They are
trusted application code, not sandboxed peer code. A callback can fail or cancel
before publication, including at `verified`; committed checkpoints remain.

Progress may lag checkpoints when cancellation arrives after a save but before
notification. Rehash on resume is authoritative. Complete-file verification does
not validate Git object structure, prove intended revision or establish publisher
identity. There is deliberately **no callback after publication**: callback failure
cannot turn completed output into a misleading pre-publication failure.

A returned `DownloadResult` contains `manifest`, the caller-selected `output` path,
`chunks_fetched`, `chunks_reused`, `integrity_verified=True` and
`publisher_authorized=False`. Return means publication and its directory fsync
completed. File/hardware durability still depends on the filesystem honoring
fsync; this is not a power-loss or hostile-filesystem guarantee.

## Cancellation, deadlines and stable failures

Cancel the asyncio task awaiting `download_transfer`; cancellation propagates as
`asyncio.CancelledError`. Each invocation creates and cleans up its own client,
so unrelated application sessions are unaffected. It sends best-effort protocol
cancellation and closes/awaits owned transport/receiver resources. There is no
implicit retry, background service, rollback or exactly-once guarantee. A sent
server response retains its budget charge even if the client cancels.

The finite `timeout` (default 30 seconds, greater than zero and at most 3600)
covers connection, network operations and cooperative callbacks. Individual
protocol limits remain in force. Connection cleanup may add its existing bounded
shutdown grace; the deadline is rechecked before publication. Cached-chunk work
also yields to cancellation. Hashing, bounded regular-file I/O, fsync and the final
publication remain synchronous and cannot promise hard CPU/disk deadlines.
No await occurs once publication begins, so asyncio task cancellation is handled
before that boundary, not as rollback of an already linked output. Process death,
OS interruption and disk failure can still leave complete output or temporary
files. Staging is retained on success and failure; cleanup is an explicit choice.

- Invalid top-level project/digest/options raise `ValueError` before connecting.
- Existing `AuthenticationError`, `ProtocolError`, `ConnectionClosed`,
  `RemoteError` and `RequestTimeout` retain their public meanings; total deadline
  expiry becomes `RequestTimeout`.
- `DownloadError` is a `PeerError` with stable `code`: `output_exists`,
  `invalid_state`, `local_io`, `callback_failed` or `publication_failed`.
  Configuration/path/checkpoint errors inside the operation become local errors;
  callbacks' ordinary exceptions become `callback_failed`. A callback's
  `CancelledError` propagates as cancellation instead.
- `DownloadError.publication_attempted` is false before publication is entered.
  When true, output may already have committed, including after an fsync or
  cleanup failure. This is deliberately conservative; inspect the selected
  output/hash before retrying. A competing local writer can also create output;
  no-overwrite protects it. Never interpret an error as permission to delete or
  replace output. No automatic rollback occurs.
- Error messages are static; chained causes can contain local paths or sensitive
  callback text. Applications must choose what to log rather than blindly publish
  tracebacks. No error code grants publisher authority.

The 8-MiB/256-chunk cap, checkpoint entry limits, one sequential chunk request,
roughly two-payload assembly memory, private files/locks and bounded peer frames
remain unchanged. A callback's own allocations or spawned tasks are application
responsibility, not part of the SDK resource guarantee.

## Evidence and scope

Tests cover immutable progress, cached/corrupt resume, empty input, callback
failure, cancellation before publication, callback timeout, delayed cleanup,
output races and post-publication failures. A real TLS pressure test deliberately
saturates the owned session with bounded concurrent test requests, cancels the API
at actual transport backpressure, verifies resource cleanup and an unrelated
session, then resumes explicitly. Normal API downloads remain sequential.

Installed-wheel acceptance outside the checkout calls only public exports against
a separate foreground loopback service. It uses 8 MiB of distinct chunks, cancels
after three checkpoints and exits that installed SDK process, resumes in another
installed process, fails a callback after full verification, then resumes
from all 256 cached chunks without fetching them again. No credentials or trust
are persisted, no received pack is ingested, and no two-machine or Internet
validation is claimed.
