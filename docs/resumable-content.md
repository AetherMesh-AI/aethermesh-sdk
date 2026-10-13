# Bounded resumable opaque-file transfer

This opt-in SDK/foreground CLI feature transfers one immutable byte snapshot of
at most 8 MiB in 32 KiB chunks (at most 256). This is an initial bounded acceptance
target, not general repository support. Git bundles can be opaque input bytes;
this feature never invokes Git, ingests packs, extracts files, checks out code,
moves refs, installs, or executes received content. Keep GitHub operational.

## Explicit identities and references

Use existing operator-managed TLS identities, independently verified peer pins,
and a separately selected project and expected manifest SHA-256. Neither the
server readiness record nor a TLS identity establishes publisher authority.
Project names are namespaces, not membership credentials. Every admitted peer
can retrieve the selected snapshot. Use distinct services/allowlists where
readership differs. AetherMesh and Eidolon keep separate publisher identities,
release channels, and authorization policies.

```sh
aethermesh-peer serve --project aethermesh-sdk \
  --certificate SERVER.pem --private-key SERVER.key --trust-store CA.pem \
  --allow-peer CLIENT_PIN --share-transfer selected.bin --content-budget 33554432

aethermesh-peer fetch-transfer --project aethermesh-sdk \
  --certificate CLIENT.pem --private-key CLIENT.key --trust-store CA.pem \
  --peer-fingerprint SERVER_PIN --port PORT --server-name localhost \
  --manifest-sha256 EXPECTED_MANIFEST_HASH \
  --staging private-checkpoints --output new-received.bin --timeout 30
```

Serving defaults to foreground loopback and an ephemeral port. `--share-file`
and `--share-transfer` are mutually exclusive. Both require an explicit budget;
readiness reports `manifest_sha256` and `content_budget_bytes`, never a source
path. The selected regular file is read once into a bounded memory snapshot.
Disk edits after snapshot creation are not shared. No credentials or persistent
trust are generated or changed.

After a failed or interrupted invocation, repeat the fetch command with
`--resume` and the same explicitly selected staging directory and manifest digest.
There are no automatic retries, peer discovery, reconnects or background workers.
A new invocation can explicitly choose an endpoint; content identity must remain
the same. A completed download is published to a new local filename only.

`fetch-transfer` uses the public `download_transfer` SDK lifecycle. To select
alternative operator-provided addresses for the same trusted peer, append up to
seven `--alternate HOST PORT SERVER_NAME` groups, for example:

```sh
--alternate 127.0.0.1 7444 localhost
```

The primary address is tried first, followed by alternatives in the supplied
order. All inherit `--peer-fingerprint` and `--expected-node-id`; no alternative
can supply new trust. Duplicate endpoints, invalid ports and more than eight
total candidates fail before connection. The single `--timeout` covers selection,
transfer and the pre-publication deadline check. Only pre-negotiation transport
failures allow fallback; identity/project/protocol failures and any manifest or
chunk failure are terminal. Resume still requires a new explicit invocation.

Success JSON remains unchanged. Local checkpoint/path failures use the existing
static `invalid_configuration` or `local_io_error` codes. Publication failures
report `publication_failed`; any SDK error after publication is entered also
includes `"publication_attempted": true`. This conservatively means complete
output may exist, even when the CLI exits unsuccessfully. Inspect the selected
output and expected hash before retrying; an error never permits replacement or
deletion. Absence of this field does not prove another process created no output.
No peer or exception text is echoed. Cleanup grace, synchronous disk-I/O limits,
and the irreversible no-overwrite publication boundary remain unchanged.

## Canonical manifest and wire operations

`TransferManifest` encodes exactly six fields: `version` (integer 1), `project`,
`sha256` (whole-file lowercase SHA-256), `size` (integer 0..8388608), `chunk_size`
(integer 32768), and `chunks` (ordered lowercase SHA-256 strings). The number of
chunks must equal `ceil(size / 32768)`, including zero for an empty file. All but
the final chunk are exactly 32768 bytes; final size is derived from total size.

Canonical bytes are ASCII JSON produced with sorted keys, `ensure_ascii=True`,
compact comma/colon separators, and no trailing newline. The expected manifest
digest hashes these exact bytes, not a parsed/reformatted equivalent. Alternate
whitespace, field sets, duplicate keys, unknown versions, project substitutions,
invalid types, excessive counts/sizes and noncanonical encodings are rejected.
Manifest responses are capped at 32768 bytes before parsing; the fixed 256-hash
schema encodes below that bound.

Protocol v2 gains two **explicitly opt-in** capabilities:

- `content.manifest.v1`: request `{project, sha256}` selects the exact manifest.
- `content.chunk.v1`: request `{project, manifest, index, sha256, size}` selects a
  chunk by manifest digest and integer index, with its expected digest and size.

Both return the existing bounded `{project, sha256, size, data}` content envelope,
with canonical base64. The SDK validates these responses even through raw
`request`; `fetch_manifest` and `fetch_chunk` build the exact reference requests.
A chunk must match the store's manifest/index and caller's expected chunk digest.
Whole-file size and SHA-256 are verified again before local publication.

Existing `ContentRef`, `ContentStore`, `content.get.v1`, default services and
protocol frame limits remain unchanged. Protocol v1 continues status/echo only.
Pre-transfer v2 clients reject the new capabilities: upgrade them before using a
transfer-enabled service. This is explicit capability incompatibility, not silent
protocol negotiation or a claim that old clients can resume. New clients retain
legacy v1 and existing v2 operation support.

## Checkpoints, crashes and local trust

The local operator selects both output and staging paths. Every component uses
anchored directory descriptors and no-follow opens; `..` and symlinks are refused.
Use physical paths (`/private/tmp` on macOS) or relative paths in an owned directory.
POSIX directory-descriptor, no-follow, hard-link, directory fsync and advisory lock
support are required. Unsupported platforms/filesystems fail closed. Parent
folders must already exist and be trusted; this is not a sandbox against hostile
same-user processes or a malicious filesystem.

A new invocation exclusively creates its staging directory with mode 0700;
`--resume` explicitly opens an existing directory. The directory must belong to
the current user and grant no group/other access. A nonblocking exclusive directory
lock rejects concurrent cooperative writers. Every file is checked for regular
file type, current ownership, private permissions, one link and bounded size.
At most 258 entries are examined and accepted; names are restricted to this
manifest, its numbered chunks and recognizable temporary checkpoints.

The canonical `manifest.json` and numbered `000.chunk` through `255.chunk` files
are the checkpoint state. There is no trusted bitmap or serialized Python object.
On resume, the stored manifest must match the freshly fetched, digest-verified
manifest exactly. Every existing chunk is rehashed; corrupt private regular chunks
are removed and requested again. Unsafe file types, permissions, links, manifest
mismatches and unknown entries fail closed. Bounded private orphan temporary files
from an interrupted write are removed under the staging lock. A temporary file
with two hard links is accepted only when its other link is a recognized
checkpoint in that same directory, covering a crash between publication and
temporary-link removal. Unrelated hard links remain an error.

Each checkpoint writes a mode-0600 temporary regular file, fsyncs its data,
atomically links it to the new checkpoint name without replacement, removes the
temporary link and fsyncs the directory. An interrupted checkpoint may survive
or be absent; resume revalidates either outcome. The parent directory is fsynced
when creating staging. Filesystem and hardware must honor fsync for power-loss
durability; this feature does not promise it for arbitrary network filesystems.

Assembly rehashes all chunks and the complete file. Output uses a same-directory
private temporary file, file fsync, atomic no-overwrite link and directory fsync.
The link is the irreversible commit point: later fsync, cleanup, interruption or
stdout failure can leave complete output despite a reported failure. There is no
rollback guarantee. A crash can leave an output temporary file. Checkpoints are
retained after success; cleanup is an explicit local decision. This new path's
fsync behavior does not change the older small-content CLI's durability contract.

## Resource and cancellation boundaries

- One 8-MiB immutable snapshot per transfer store; at most 256 chunk files and a
  manifest, with at most 258 total staging entries including crash remnants. No remote filenames/listing,
  upload operation, compressed parsing, or arbitrary byte ranges.
- Explicit serving budget 0..64 MiB shared by manifest and chunk responses. Each
  accepted response charges raw bytes before send; duplicate requests, cancellation
  and disconnection never refund the budget. Reusing a store preserves it;
  restarting the CLI constructs a new store/budget.
- Existing TLS, connection, frame, in-flight and request-count limits apply.
  Fetches are sequential. Repeated manifests also cost bytes. Empty manifests
  still consume bytes and requests. Budgets are per store/process, not global.
- Regular staging file contents are bounded by 258 times 32 KiB (8 MiB plus
  64 KiB), including recognized crash remnants, excluding filesystem metadata. Assembly temporarily holds both chunks and the joined file in memory
  (roughly twice the 8-MiB payload limit), in addition to bounded network buffers.
- `--timeout` bounds network awaits for the invocation; individual requests also
  have finite deadlines. Regular-file hashing, reads, writes and fsync are bounded
  in byte count but synchronous: the network deadline is not a hard disk-I/O or
  CPU deadline. Cancellation is delivered at async suspension points.
- Cancellation preserves already committed verified checkpoints and creates no
  final output before publication. An already-started server response retains
  ownership of its frame; cancellation cannot reclaim sent bytes or force that
  frame to remain backpressured. No duplicate response is generated.

Reports distinguish `integrity_verified: true` from `publisher_authorized: false`
and include `chunks_fetched`. File integrity does not establish revision intent,
publisher authorization, release freshness, rollback protection or code safety.

## Validation and deferred work

Installed-wheel acceptance uses disposable loopback identities, an exact 8-MiB
file and separate CLI processes outside the checkout. The first server has budget
for the manifest and three chunks; fetch exits on exhaustion and leaves no output.
An installed-SDK subprocess then exits abruptly after linking the fourth chunk
but before unlinking its temporary name. Both CLI processes restart explicitly.
The second server has budget for the manifest and exactly 252 missing chunks; success and the fetched count demonstrate reuse of
verified chunks. Tests cover corrupt checkpoints/manifests, ownership/mode/type and
symlink rejection, directory renames, concurrent resume, fsync failures, deadlines,
malicious responses, old capability whitelists and real TLS backpressure cancellation.
This is loopback evidence, not Internet or two-machine proof.

See [trusted bundle export and header inspection](bundle-exchange.md) for the
bounded producer/inspection path. Quarantine pack import and publisher/channel
signatures remain separate future slices. In particular, compressed-pack size does not bound object
expansion. No pack ingestion should be added before enforceable subprocess CPU,
memory and disk isolation is established and independently reviewed. Trusted
branches and active application checkouts remain outside this feature.
