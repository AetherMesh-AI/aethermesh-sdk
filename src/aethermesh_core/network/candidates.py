"""Explicit bounded alternatives for one trusted peer, never discovery or replay."""

from __future__ import annotations

import asyncio
import math
from dataclasses import dataclass
from typing import Self

from .client import PeerClient
from .config import Limits, PeerEndpoint, TLSIdentity
from .errors import (
    ConnectionClosed,
    EndpointTimeout,
    EndpointUnavailable,
    RequestTimeout,
)
from .profile import NodeProfile


@dataclass(frozen=True)
class CandidateConnection:
    """Caller-owned session; close explicitly or use an async context manager."""

    client: PeerClient
    endpoint: PeerEndpoint
    attempted_count: int

    async def close(self) -> None:
        await self.client.close()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()


def _validate(
    identity: TLSIdentity,
    endpoints: tuple[PeerEndpoint, ...],
    timeout: float,
    limits: Limits | None,
) -> None:
    if not isinstance(identity, TLSIdentity) or (
        limits is not None and not isinstance(limits, Limits)
    ):
        raise TypeError("explicit TLS identity and typed limits required")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or not 0 < timeout <= 3600
    ):
        raise ValueError("candidate timeout must be finite and within (0, 3600]")
    if (
        not isinstance(endpoints, tuple)
        or not 1 <= len(endpoints) <= 8
        or any(not isinstance(row, PeerEndpoint) for row in endpoints)
        or len(set(endpoints)) != len(endpoints)
    ):
        raise ValueError("select one to eight distinct explicit peer endpoints")
    if len({(row.fingerprint, row.expected_node_id) for row in endpoints}) != 1:
        raise ValueError("all candidates must pin the same peer identity")


async def connect_peer_candidates(
    identity: TLSIdentity,
    endpoints: tuple[PeerEndpoint, ...],
    *,
    project_id: str,
    timeout: float,
    limits: Limits | None = None,
    node_profile: NodeProfile | None = None,
) -> CandidateConnection:
    """Try each trusted endpoint once, in caller order, under one total deadline.

    Only pre-negotiation transport unavailability or timeout permits fallback.
    Authentication/protocol/configuration failures and cancellation are terminal.
    No requests are sent beyond normal peer negotiation/profile announcement.
    This does not discover, trust, reconnect, relay or replay application work.
    The deadline bounds attempts; cooperative cancellation awaits existing bounded
    socket cleanup, so it is not a hard wall-clock limit on synchronous local I/O.
    """
    _validate(identity, endpoints, timeout, limits)
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    budget = asyncio.timeout_at(deadline)
    try:
        async with budget:
            for count, endpoint in enumerate(endpoints, 1):
                if loop.time() >= deadline:
                    raise RequestTimeout("Candidate connection deadline expired.")
                client = PeerClient(
                    identity,
                    project_id=project_id,
                    limits=limits,
                    node_profile=node_profile,
                )
                try:
                    await client.connect(endpoint)
                    if loop.time() >= deadline:
                        raise RequestTimeout("Candidate connection deadline expired.")
                except (EndpointUnavailable, EndpointTimeout):
                    await client.close()
                    if loop.time() >= deadline:
                        raise RequestTimeout(
                            "Candidate connection deadline expired."
                        ) from None
                except BaseException as failure:
                    try:
                        await client.close()
                    except Exception as cleanup:
                        raise failure from cleanup
                    raise
                else:
                    return CandidateConnection(client, endpoint, count)
    except TimeoutError as exc:
        if not budget.expired():
            raise
        raise RequestTimeout("Candidate connection deadline expired.") from exc
    raise ConnectionClosed("No selected endpoint established the peer session.")
