"""Opt-in, project-scoped diagnostic peer service.

Only established TLS sessions count against ``max_connections``. TLS handshakes
are time-bounded, but an Internet-facing deployment still needs connection/rate
limits before TLS (for example, a firewall or a trusted reverse proxy).
Pending TLS handshakes are owned by asyncio and expire within handshake_timeout.
Python 3.12.1+ waits for these during close; Python 3.11 may return before their
bounded expiry. Authenticated application sessions are always drained first.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from dataclasses import dataclass, field
from functools import partial
from types import MappingProxyType
from typing import Any, Self

from .config import (
    Limits,
    TLSIdentity,
    peer_fingerprint,
    validate_fingerprint,
    validate_project_id,
)
from .errors import PeerError, ProtocolError, RemoteError
from .profile import NodeProfile, PeerInfo, validate_node_id
from .protocol import SUPPORTED_PROTOCOL_VERSIONS, close_writer, read_frame, write_frame

_DEFAULT_LIMITS = Limits()


@dataclass
class _Session:
    writer: asyncio.StreamWriter
    requests: dict[int, asyncio.Task[None]] = field(default_factory=dict)
    responding: set[int] = field(default_factory=set)
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    last_id: int = 0
    request_count: int = 0
    protocol_version: int = 1


class PeerService:
    """A bounded, explicitly started TLS peer exposing status and optional echo.

    Constructing this object performs no I/O. It never starts a daemon, executes
    arbitrary work, or enables hosting/inference as a side effect of connecting.
    ``close`` is idempotent, and the same service can subsequently be restarted.
    """

    def __init__(
        self,
        identity: TLSIdentity,
        *,
        project_id: str,
        allowed_peers: frozenset[str],
        capabilities: frozenset[str] = frozenset(),
        limits: Limits = _DEFAULT_LIMITS,
        node_profile: NodeProfile | None = None,
        expected_peer_ids: Mapping[str, str] | None = None,
    ) -> None:
        validate_project_id(project_id)
        if not isinstance(allowed_peers, frozenset):
            raise TypeError("allowed_peers must be a frozenset of SHA-256 fingerprints")
        for pin in allowed_peers:
            validate_fingerprint(pin)
        if not isinstance(capabilities, frozenset) or not capabilities <= {
            "status",
            "echo",
        }:
            raise ValueError("unsupported peer capability")
        if node_profile is not None and not isinstance(node_profile, NodeProfile):
            raise TypeError("node_profile must be a NodeProfile or None")
        if expected_peer_ids is not None and not isinstance(expected_peer_ids, Mapping):
            raise TypeError(
                "expected_peer_ids must map allowed fingerprints to node IDs"
            )
        bindings = dict(expected_peer_ids or {})
        for pin, node_id in bindings.items():
            validate_fingerprint(pin)
            validate_node_id(node_id)
            if pin not in allowed_peers:
                raise ValueError(
                    "node identity bindings require an allowed fingerprint"
                )
        self._node_profile = node_profile
        self._expected_peer_ids: Mapping[str, str] = MappingProxyType(bindings)
        self._peers: dict[asyncio.StreamWriter, PeerInfo] = {}
        self.identity = identity
        self.project_id = project_id
        self.capabilities = frozenset({"status"}) | capabilities
        self.allowed_peers = allowed_peers
        self.limits = limits
        self._server: asyncio.Server | None = None
        self._connections: set[asyncio.Task[None]] = set()
        self._writers: set[asyncio.StreamWriter] = set()
        self._lifecycle_lock = asyncio.Lock()
        self._closing = True
        self._stopped = asyncio.Event()
        self._generation = object()
        self._shutdown_task: asyncio.Task[None] | None = None

    @property
    def node_profile(self) -> NodeProfile | None:
        """The immutable, explicitly shared profile configured for this service."""
        return self._node_profile

    @property
    def expected_peer_ids(self) -> Mapping[str, str]:
        """A read-only snapshot of configured certificate-to-node-ID bindings."""
        return self._expected_peer_ids

    @property
    def peers(self) -> tuple[PeerInfo, ...]:
        """Authenticated active sessions; claimed identities never establish trust.

        Duplicate node IDs and names remain separate entries, including multiple
        sessions authenticated by the same certificate. Entries are not persisted.
        """
        return tuple(self._peers.values())

    @property
    def address(self) -> tuple[str, int]:
        """The listening address; unavailable until ``start`` completes."""
        if self._server is None:
            raise PeerError("peer service is not listening")
        address = self._server.sockets[0].getsockname()
        return str(address[0]), int(address[1])

    async def start(self, host: str = "127.0.0.1", port: int = 0) -> None:
        """Listen explicitly; loopback and an ephemeral port are the defaults."""
        if not isinstance(host, str) or not host:
            raise ValueError("host must be nonempty")
        if type(port) is not int or not 0 <= port <= 65535:
            raise ValueError("port must be an integer between 0 and 65535")
        async with self._lifecycle_lock:
            if self._server is not None:
                raise PeerError("peer service is already listening")
            if self._shutdown_task is not None:
                await asyncio.shield(self._shutdown_task)
            context = self.identity.server_context()
            self._closing = False
            self._stopped = asyncio.Event()
            self._generation = object()
            self._server = await asyncio.start_server(
                partial(self._accept, generation=self._generation),
                host=host,
                port=port,
                ssl=context,
                ssl_handshake_timeout=self.limits.handshake_timeout,
                ssl_shutdown_timeout=self.limits.io_timeout,
                limit=65536,
            )
            self._shutdown_task = None

    def _accept(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        *,
        generation: object,
    ) -> None:
        # Reject without spawning tasks when capacity is exhausted. This budget
        # applies after TLS; the TLS implementation owns pre-handshake sockets.
        if (
            generation is not self._generation
            or self._closing
            or len(self._connections) >= self.limits.max_connections
        ):
            writer.transport.abort()
            return
        try:
            fingerprint = peer_fingerprint(writer)
        except PeerError:
            writer.transport.abort()
            return
        if fingerprint not in self.allowed_peers:
            writer.transport.abort()
            return
        self._writers.add(writer)
        task = asyncio.create_task(self._connection(reader, writer, fingerprint))
        self._connections.add(task)
        task.add_done_callback(self._connections.discard)

    async def _send(self, session: _Session, message: dict[str, Any]) -> None:
        # Bound both lock acquisition and drain so slow readers cannot retain
        # resources indefinitely, even with several concurrent responses.
        async with asyncio.timeout(self.limits.io_timeout):
            async with session.write_lock:
                await write_frame(session.writer, message)

    async def _negotiate(
        self, reader: asyncio.StreamReader, session: _Session, fingerprint: str
    ) -> None:
        hello = await read_frame(reader)
        versions = hello.get("versions")
        if (
            set(hello) != {"type", "versions", "project"}
            or hello["type"] != "hello"
            or hello["project"] != self.project_id
            or not isinstance(versions, list)
            or not 1 <= len(versions) <= 16
            or any(type(version) is not int for version in versions)
        ):
            raise ProtocolError("invalid peer negotiation")
        common = set(versions).intersection(SUPPORTED_PROTOCOL_VERSIONS)
        if not common:
            raise ProtocolError("unsupported peer protocol version")
        session.protocol_version = max(common)
        expected = self.expected_peer_ids.get(fingerprint)
        if session.protocol_version == 1 and expected is not None:
            raise ProtocolError(
                "peer node identity binding requires protocol version 2"
            )
        welcome: dict[str, Any] = {
            "type": "welcome",
            "version": session.protocol_version,
            "project": self.project_id,
            "capabilities": sorted(self.capabilities),
        }
        profile = None
        if session.protocol_version == 2:
            welcome["node"] = self.node_profile.to_dict() if self.node_profile else None
        await self._send(session, welcome)
        if session.protocol_version == 2:
            identify = await read_frame(reader)
            if set(identify) != {"type", "node"} or identify["type"] != "identify":
                raise ProtocolError("invalid peer identity exchange")
            if identify["node"] is not None:
                profile = NodeProfile.from_dict(identify["node"])
            if expected is not None and (
                profile is None or profile.node_id != expected
            ):
                raise ProtocolError(
                    "peer node identity does not match its configured binding"
                )
        self._peers[session.writer] = PeerInfo(
            fingerprint,
            session.protocol_version,
            profile,
            identity_pinned=expected is not None,
        )
        if session.protocol_version == 2:
            await self._send(session, {"type": "ready"})

    async def _connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        fingerprint: str,
    ) -> None:
        session = _Session(writer)
        try:
            # The full application exchange, including both outbound drains, is
            # bounded by one deadline. No requests are processed before readiness.
            async with asyncio.timeout(self.limits.handshake_timeout):
                await self._negotiate(reader, session, fingerprint)
            while True:
                message = await asyncio.wait_for(
                    read_frame(reader), self.limits.io_timeout
                )
                if not await self._receive(session, message):
                    break
        except (PeerError, OSError, TimeoutError):
            # Deliberately do not log attacker-controlled payloads or identity
            # paths. A failed transport/negotiation has no trusted response ID.
            pass
        finally:
            self._peers.pop(writer, None)
            requests = list(session.requests.values())
            for request in requests:
                request.cancel()
            await asyncio.gather(*requests, return_exceptions=True)
            await close_writer(writer)
            self._writers.discard(writer)

    async def _receive(self, session: _Session, message: dict[str, Any]) -> bool:
        request_id = message.get("id")
        if type(request_id) is not int or request_id <= 0:
            raise ProtocolError("invalid request id")
        if message.get("type") == "cancel":
            if set(message) != {"type", "id"}:
                raise ProtocolError("invalid cancellation")
            task = session.requests.get(request_id)
            if task is not None and request_id not in session.responding:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await self._send_error(
                    session, request_id, "cancelled", "request cancelled"
                )
            # Cancellation racing an already emitted result is harmless. No
            # duplicate response is produced for completed or unknown IDs.
            return True
        if (
            set(message) != {"type", "id", "operation", "payload"}
            or message["type"] != "request"
            or not isinstance(message["operation"], str)
            or not isinstance(message["payload"], dict)
            or request_id <= session.last_id
        ):
            raise ProtocolError("invalid request")
        session.last_id = request_id
        session.request_count += 1
        if session.request_count > self.limits.max_requests:
            await self._send_error(
                session, request_id, "busy", "session request limit reached"
            )
            return False
        if len(session.requests) >= self.limits.max_inflight:
            await self._send_error(session, request_id, "busy", "peer is busy")
            return True
        task = asyncio.create_task(
            self._execute(session, request_id, message["operation"], message["payload"])
        )
        session.requests[request_id] = task
        task.add_done_callback(
            lambda done: self._request_done(session, request_id, done)
        )
        return True

    @staticmethod
    def _request_done(
        session: _Session, request_id: int, task: asyncio.Task[None]
    ) -> None:
        session.requests.pop(request_id, None)
        session.responding.discard(request_id)
        if not task.cancelled() and task.exception() is not None:
            session.writer.transport.abort()

    async def _send_error(
        self, session: _Session, request_id: int, code: str, message: str
    ) -> None:
        await self._send(
            session,
            {"type": "error", "id": request_id, "code": code, "message": message},
        )

    async def _execute(
        self,
        session: _Session,
        request_id: int,
        operation: str,
        payload: dict[str, Any],
    ) -> None:
        try:
            result = await self._operate(
                operation, payload, protocol_version=session.protocol_version
            )
        except RemoteError as exc:
            response = {
                "type": "error",
                "id": request_id,
                "code": exc.code,
                "message": str(exc),
            }
        else:
            response = {"type": "result", "id": request_id, "result": result}
        # Once a response owns the write, a later cancel cannot generate a
        # second response. This task stays tracked until the drain completes.
        session.responding.add(request_id)
        await self._send(session, response)

    async def _operate(
        self, operation: str, payload: dict[str, Any], *, protocol_version: int = 1
    ) -> dict[str, Any]:
        if operation not in self.capabilities:
            raise RemoteError("unsupported_operation", "operation is not enabled")
        if operation == "status":
            if payload:
                raise RemoteError("invalid_request", "status payload must be empty")
            status: dict[str, Any] = {
                "protocol_version": protocol_version,
                "capabilities": sorted(self.capabilities),
            }
            if protocol_version == 2:
                status["node"] = (
                    self.node_profile.to_dict() if self.node_profile else None
                )
            return status
        text = payload.get("text")
        delay = payload.get("delay_ms", 0)
        if (
            not {"text"} <= set(payload) <= {"text", "delay_ms"}
            or not isinstance(text, str)
            or len(text.encode("utf-8")) > 4096
            or type(delay) is not int
            or not 0 <= delay <= 1000
        ):
            raise RemoteError("invalid_request", "invalid diagnostic echo payload")
        await asyncio.sleep(delay / 1000)
        return {"text": text}

    async def _shutdown(self, server: asyncio.Server) -> None:
        connections = list(self._connections)
        for connection in connections:
            connection.cancel()
        await asyncio.gather(*connections, return_exceptions=True)
        # A task cancelled before its coroutine starts cannot run its finally
        # block, so retain independent ownership of every admitted transport.
        await asyncio.gather(*(close_writer(writer) for writer in self._writers))
        self._writers.clear()
        self._peers.clear()
        await server.wait_closed()
        self._stopped.set()

    async def close(self) -> None:
        """Stop accepting, cancel and await active work, and close transports.

        Cleanup continues if the caller awaiting ``close`` is cancelled.
        """
        await self._close()

    async def _close(self, expected_server: asyncio.Server | None = None) -> None:
        async with self._lifecycle_lock:
            if expected_server is not None and self._server is not expected_server:
                return
            if self._server is not None:
                self._closing = True
                server, self._server = self._server, None
                server.close()
                self._shutdown_task = asyncio.create_task(self._shutdown(server))
            task = self._shutdown_task
        if task is not None:
            await asyncio.shield(task)

    async def serve_forever(self) -> None:
        """Wait on an explicitly started listener until cancelled or closed."""
        if self._server is None:
            raise PeerError("peer service is not listening")
        server = self._server
        stopped = self._stopped
        try:
            await stopped.wait()
        finally:
            await self._close(server)

    async def __aenter__(self) -> Self:
        await self.start()
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()
