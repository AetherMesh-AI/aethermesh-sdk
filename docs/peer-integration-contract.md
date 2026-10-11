# Minimal application peer integration contract

This is a proposed application handoff using existing SDK/CLI interfaces, not an
implemented Eidolon adapter or a new wire schema. Eidolon owns its integration
and remains usable with other providers when AetherMesh is absent or disconnected.
No UI, model engine, build scheduler or application lifecycle moves into the SDK.

## Ownership and inputs

| Owner | Responsibility |
| --- | --- |
| Operator/application | Explicit project selection; endpoint host/port; certificate SAN server name; existing TLS identity/trust paths; verified leaf pin; optional expected node ID; finite operation timeout; user-selected content digest, destination and participation. |
| SDK | Mutual TLS, pin/hostname verification, project/version negotiation, immutable session facts, bounded requests, integrity validation, typed failures and explicit close/cancellation. |
| Application | UI state, local configuration storage and permissions, finite retry policy, download retention, per-project publisher/channel policy and separately authorized application updates. |
| Optional foreground service | One selected project and explicit reader allowlist; selected immutable shared snapshot and lifetime byte budget. Starting the client never consents to hosting. |

AetherMesh and Eidolon have distinct project IDs, publisher identities, release
versions and channels. Sharing one transport implementation does not merge any of
these authorities. A project string is a namespace, a certificate pin authenticates
a TLS endpoint, and a content hash identifies bytes. None authorizes a release.
Do not persist trust inferred from readiness, discovery claims or received headers.

## Smallest useful adapter

For a Python application, use public `aethermesh_core.network` exports:
`TLSIdentity`, `PeerEndpoint`, `PeerClient` and `PeerError` subclasses. Imports and
constructors perform no hosting. Within an async lifecycle, connect once, read
negotiated facts, request status with a finite deadline and close in `finally`
or an async context manager. The [network guide](network-foundation.md) has the
executable API example. Connecting, connected, failed and disconnected are
application states, not a new SDK protocol. Capability advertisements describe
handlers; they do not attest capacity, code safety or publisher authority.

Use `fetch_manifest` and `fetch_chunk` for bounded SDK-level byte retrieval;
these calls do not themselves provide on-disk resume/publication. The public
[`download_transfer` API](download-api.md) and installed `fetch-transfer` command
provide that workflow. Prefer the public API for a Python adapter. Initially an application can
invoke the installed CLI with an argument array, no shell, in its own isolated
Python environment. Resolve that executable through application configuration,
never a peer-supplied path. Pass only operator-selected paths and validated values.
Do not import private `network.transfer_files` helpers as a stable public API.

For this minimal CLI adapter:

- `check`: exit 0 returns one stdout JSON result; operational failures at exit 1
  return static failure JSON on stderr. Syntax errors at exit 2 print plain usage
  and error text; interruption at exit 130 does not promise JSON. Branch on exit
  status before parsing output; see [the runbook](two-peer-operations.md).
- `fetch-transfer`: explicitly selected manifest digest, private staging and new
  output; success separates integrity from publisher authorization. Resume is a
  new explicit invocation with `--resume`, not implicit replay.
- `bundle-inspect`: local opaque-header report; `pack_validated` and
  `publisher_authorized` remain false. Header commit/ref fields remain claims.
- An app that opts into hosting may supervise `serve` as a separate foreground
  child, read its readiness record and stop/await it on shutdown. This needs
  separate listener/resource consent; no launch agent, daemon or startup install.

Bound captured stdout/stderr and child lifetime, surface failures without exposing
secrets, and distinguish child exit from successful output publication. On user
cancellation, forward interruption, await cleanup with a finite application-owned
grace period, then handle any forced termination as interrupted—not rollback.
Checkpoints and a completely published output can survive interruption. The CLI
uses synchronous local I/O; an app timeout is not proof that fsync was undone.
Do not make the UI block while a child runs. An adapter implementing these rules
needs its own application tests and review before shipping; none is added here.

For in-process calls, handle `AuthenticationError`, `ProtocolError`,
`ConnectionClosed`, `RequestTimeout` and `RemoteError` distinctly where useful.
Local cancellation propagates through async task cancellation; protocol cancel is
best effort. Reconnect is explicit. Do not automatically retry security/protocol
failures or replay an operation with assumed exactly-once semantics.

## Acceptance and next SDK usability decision

Before wiring Eidolon UI actions, prove installed CLI diagnostics and selected-byte
transfer between the approved machines using the runbook. Then test the adapter
with SDK missing, service absent, bad pin/project, incompatible capabilities,
timeout, cancellation, resume, pre-existing output and service shutdown. Existing
providers must remain available throughout. Do not start hosting or share saved
node/hardware metadata as a side effect of opening a screen.

The public [resumable-download API](download-api.md) now provides immutable
progress, scoped cancellation and explicit publication outcomes using the current
checkpoint and integrity machinery. The next application step is an independently
reviewed adapter using that API, with UI/lifecycle tests and the operator-approved
connection acceptance above. It does not introduce pack import, a build scheduler,
automatic trust or release installation.

Untrusted Git ingestion still requires separately proven process, memory, disk,
filesystem and network containment. Even validated Git objects would not authorize
branch promotion, checkout, execution or release updates. Keep received artifacts
outside active application checkouts and keep GitHub operational.
