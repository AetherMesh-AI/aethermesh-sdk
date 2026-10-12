# Explicit trusted peer candidates

`connect_peer_candidates` is a bounded connection convenience for **one already
trusted peer** at alternative operator-supplied endpoints. It is a first local
NET-1 contract slice, not discovery, NAT traversal, a relay, public deployment or
proof of two-machine reachability. Existing `PeerClient.connect` remains available
for a single explicit endpoint.

```python
from aethermesh_core.network import connect_peer_candidates

connection = await connect_peer_candidates(
    existing_identity,
    (primary_endpoint, alternate_endpoint),
    project_id="selected-project",
    timeout=10.0,
)
async with connection:
    status = await connection.client.request("status", {}, timeout=2.0)
```

The caller provides an existing `TLSIdentity`, a tuple of one to eight distinct
`PeerEndpoint` values, one project, and a finite total timeout in `(0, 3600]` seconds.
Every candidate must use the same exact certificate fingerprint and expected node
ID (or all omit that ID). Host, port and expected SAN name may differ only as
explicitly configured. Each endpoint still undergoes TLS 1.3 mutual authentication,
CA/SAN/pin checks, optional node-ID checking and project/protocol negotiation.
The helper never learns trust or endpoints from a peer, persists configuration,
creates keys, discovers addresses or starts a listener.

Candidates are tried once in caller order, using a new client for each attempt.
One monotonic deadline covers the entire invocation; it is never reset between
attempts. Existing `Limits.handshake_timeout` may bound an individual attempt more
narrowly. Async task cancellation propagates, closes the owned attempt and does
not advance to another candidate. Cleanup uses the existing bounded socket-close
mechanism and may finish after cancellation/deadline delivery; synchronous local
certificate loading is not preemptible. This is not a hard wall-clock guarantee.

Only two typed failures permit another attempt:

- `EndpointUnavailable`, a `ConnectionClosed` subtype: transport establishment
  failed before a TLS transport was handed to the protocol negotiator.
- `EndpointTimeout`, a `RequestTimeout` subtype: establishment timed out before
  protocol negotiation began.

Once transport establishment succeeds, **every** negotiation failure is terminal,
including EOF or timeout. This distinction matters because a server can reject a
project by closing without an explicit protocol error. Authentication failures,
wrong pins/SANs/node IDs, malformed/incompatible protocol, configuration errors and
unexpected exceptions never trigger fallback. Cleanup failures stop attempts; when
cleaning up a terminal failure, its original type (including cancellation) is
preserved and an ordinary cleanup exception is attached as its cause. A new
cancellation during cleanup remains terminal. Existing handlers for the base
`ConnectionClosed` and `RequestTimeout` classes continue to work. Exhausting all
candidates raises static `ConnectionClosed`; exhausting the total deadline raises
`RequestTimeout`. Only expiry of the helper’s own timer is translated to this
shared-deadline diagnostic; unrelated local/cleanup timeouts remain terminal
without being relabeled. No peer-supplied error strings or TLS paths enter new diagnostics.

`CandidateConnection` returns the selected endpoint, attempt count and caller-owned
client. Close it explicitly or use its async context manager. Successful connection
is an observation, not a guarantee the peer remains live. No requests are issued
beyond normal connection negotiation and any explicitly supplied profile announcement.
There is no operation callback, application retry, automatic reconnect or operation
replay. After handoff, request failures and uncertain outcomes remain the caller's
responsibility under the existing SDK contracts.

Tests use mocks for deterministic deadline boundaries and disposable TLS loopback
fixtures for refusal/fallback, identity/project rejection, transport versus
negotiation timeout, cancellation and an explicitly requested echo. Installed-wheel
acceptance verifies an unavailable address followed by an authenticated connection
and a caller-issued status request. These are not independent-machine or Internet
acceptance. Full NET-1 still needs separately reviewed discovery/relay interfaces,
operator-authorized infrastructure and two-machine evidence.

This connection utility does not grant publisher, release, serving, retention,
repair-execution or deletion authority. Retention's trusted nondecreasing-time or
durable observation-high-water gate, fresh ownership/byte checks and separate
authority remain mandatory before future serving or execution. GitHub remains
operational; project publishers, channels and transport identities remain separate.

Endpoints are frozen caller-owned configuration values. Normal field assignment
is rejected, and replacement values are validated before connection. This API is
not a sandbox against code in the same process deliberately bypassing Python
immutability or changing trusted identity files.
