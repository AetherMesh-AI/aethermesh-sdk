"""Explicit, lightweight connections to authenticated AetherMesh peers."""

from __future__ import annotations

import asyncio
import math
import ssl
from collections.abc import Mapping
from typing import Any, Self

from .config import (
    Limits,
    PeerEndpoint,
    TLSIdentity,
    peer_fingerprint,
    validate_project_id,
)
from .errors import (
    AuthenticationError,
    ConnectionClosed,
    PeerError,
    ProtocolError,
    RemoteError,
    RequestTimeout,
)
from .profile import NodeProfile, PeerInfo
from .protocol import (
    SUPPORTED_PROTOCOL_VERSIONS,
    Message,
    close_writer,
    read_frame,
    write_frame,
)

_ERRORS = {
    "invalid_request": "The peer rejected the request payload.",
    "unsupported_operation": "The peer does not offer this operation.",
    "busy": "The peer has reached its request limit.",
    "cancelled": "The peer cancelled the request.",
}


class PeerClient:
    """A single explicit peer session; construction never opens a connection.

    Requests are multiplexed, bounded and cancellable. Cancelling a request's
    asyncio task sends best-effort cancellation to the peer. There are no
    automatic retries: a disconnect can leave an operation's outcome unknown.
    Create a separate client per endpoint; explicit ``connect`` supports reuse
    after ``close`` or a transport failure.
    """

    def __init__(
        self,
        identity: TLSIdentity,
        *,
        project_id: str,
        limits: Limits | None = None,
        node_profile: NodeProfile | None = None,
    ) -> None:
        validate_project_id(project_id)
        if node_profile is not None and not isinstance(node_profile, NodeProfile):
            raise ValueError("node_profile must be a NodeProfile or None.")
        self._node_profile = node_profile
        self._peer_info: PeerInfo | None = None
        self._identity = identity
        self._project = project_id
        self._limits = limits or Limits()
        self._lifecycle_lock = asyncio.Lock()
        self._send_lock = asyncio.Lock()
        self._writer: asyncio.StreamWriter | None = None
        self._receiver: asyncio.Task[None] | None = None
        self._pending: dict[int, asyncio.Future[Message]] = {}
        self._next_id = 1
        self._capabilities: tuple[str, ...] = ()

    @property
    def connected(self) -> bool:
        """Whether protocol negotiation has completed on a live transport."""
        return self._writer is not None and not self._writer.is_closing()

    @property
    def capabilities(self) -> tuple[str, ...]:
        """Negotiated diagnostic operations, empty while disconnected."""
        return self._capabilities

    @property
    def peer_info(self) -> PeerInfo | None:
        """Authenticated certificate identity and self-reported profile, if connected."""
        return self._peer_info

    @property
    def protocol_version(self) -> int | None:
        """Negotiated protocol; legacy v1 connections do not exchange profiles."""
        return self._peer_info.protocol_version if self._peer_info is not None else None

    @property
    def identity_announced(self) -> bool:
        """Whether this session completed explicit local profile announcement."""
        return self.protocol_version == 2 and self._node_profile is not None

    async def connect(self, endpoint: PeerEndpoint) -> None:
        """Authenticate and negotiate, or fail without retaining a socket."""
        async with self._lifecycle_lock:
            if self.connected:
                raise PeerError("Close the existing peer session before connecting.")
            await self._close_session()
            writer: asyncio.StreamWriter | None = None
            try:
                async with asyncio.timeout(self._limits.handshake_timeout):
                    reader, writer = await asyncio.open_connection(
                        endpoint.host,
                        endpoint.port,
                        ssl=self._identity.client_context(),
                        server_hostname=endpoint.server_name,
                        ssl_handshake_timeout=self._limits.handshake_timeout,
                        ssl_shutdown_timeout=1.0,
                    )
                    if peer_fingerprint(writer) != endpoint.fingerprint:
                        raise AuthenticationError(
                            "The peer certificate pin did not match."
                        )
                    await write_frame(
                        writer,
                        {
                            "type": "hello",
                            "versions": list(SUPPORTED_PROTOCOL_VERSIONS),
                            "project": self._project,
                        },
                    )
                    welcome = await read_frame(reader)
                    capabilities = self._welcome(welcome)
                    info = self._peer(welcome, endpoint)
                    await self._announce_identity(reader, writer, info.protocol_version)
                self._writer = writer
                self._capabilities = capabilities
                self._peer_info = info
                self._next_id = 1
                self._receiver = asyncio.create_task(self._receive(reader, writer))
                writer = None
            except ssl.SSLError as exc:
                raise AuthenticationError("Peer TLS authentication failed.") from exc
            except TimeoutError as exc:
                raise RequestTimeout("Peer connection timed out.") from exc
            except (OSError, asyncio.IncompleteReadError) as exc:
                raise ConnectionClosed("Could not establish the peer session.") from exc
            finally:
                if writer is not None:
                    await close_writer(writer)

    async def _announce_identity(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        version: int,
    ) -> None:
        """Finish v2 identity exchange inside the caller's handshake deadline.

        Keeping the legacy return explicit also makes this protocol branch
        independent of interpreter-specific async-context exit instructions.
        """
        if version == 1:
            return
        await write_frame(
            writer,
            {
                "type": "identify",
                "node": self._node_profile.to_dict()
                if self._node_profile is not None
                else None,
            },
        )
        if await read_frame(reader) != {"type": "ready"}:
            raise ProtocolError("The peer did not complete identity negotiation.")

    def _welcome(self, message: Message) -> tuple[str, ...]:
        version = message.get("version")
        fields = {"type", "version", "project", "capabilities"}
        if version == 2:
            fields.add("node")
        if (
            set(message) != fields
            or message["type"] != "welcome"
            or type(message["version"]) is not int
            or version not in SUPPORTED_PROTOCOL_VERSIONS
            or message["project"] != self._project
        ):
            raise ProtocolError("The peer did not negotiate this project and protocol.")
        capabilities = message["capabilities"]
        if (
            not isinstance(capabilities, list)
            or not all(isinstance(item, str) for item in capabilities)
            or len(capabilities) != len(set(capabilities))
            or "status" not in capabilities
            or not set(capabilities).issubset({"status", "echo"})
        ):
            raise ProtocolError("The peer advertised invalid capabilities.")
        return tuple(capabilities)

    @staticmethod
    def _peer(message: Message, endpoint: PeerEndpoint) -> PeerInfo:
        value = message.get("node")
        profile = None if value is None else NodeProfile.from_dict(value)
        expected = endpoint.expected_node_id
        if expected is not None and (profile is None or profile.node_id != expected):
            raise AuthenticationError(
                "The peer did not provide the configured node identity."
            )
        return PeerInfo(
            endpoint.fingerprint, message["version"], profile, expected is not None
        )

    @staticmethod
    def _validate_status(
        result: Message, peer: PeerInfo, capabilities: tuple[str, ...]
    ) -> None:
        if (
            set(result) != {"protocol_version", "capabilities", "node"}
            or type(result["protocol_version"]) is not int
            or result["protocol_version"] != peer.protocol_version
            or result["capabilities"] != list(capabilities)
        ):
            raise ProtocolError(
                "The peer status does not match its negotiated identity and capabilities."
            )
        value = result["node"]
        profile = None if value is None else NodeProfile.from_dict(value)
        if profile != peer.profile:
            raise ProtocolError("The peer changed its negotiated node profile.")

    async def request(
        self,
        operation: str,
        payload: Mapping[str, Any] | None = None,
        *,
        timeout: float = 10.0,
    ) -> Message:
        """Run a bounded diagnostic operation and return its versioned result.

        ``status`` takes an empty payload. Explicitly enabled ``echo`` takes
        ``text`` (up to 4096 UTF-8 bytes) and optional ``delay_ms`` (0..1000).
        Echo is a connectivity diagnostic, not inference or job execution.
        """
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= 3600
            or not math.isfinite(timeout)
        ):
            raise ValueError("timeout must be finite and between 0 and 3600 seconds.")
        if not isinstance(operation, str) or not 1 <= len(operation) <= 64:
            raise ValueError(
                "operation must be a nonempty string of up to 64 characters."
            )
        if payload is not None and not isinstance(payload, Mapping):
            raise ValueError("payload must be a mapping.")
        if not self.connected:
            raise ConnectionClosed("Connect to a peer before requesting work.")
        if len(self._pending) >= self._limits.max_inflight:
            raise PeerError("The client has reached its in-flight request limit.")
        pending = self._pending
        session_writer = self._writer
        peer = self._peer_info
        capabilities = self._capabilities
        request_id = 0
        future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        try:
            async with asyncio.timeout(timeout):
                async with self._send_lock:
                    writer = self._writer
                    if writer is None or writer is not session_writer:
                        raise ConnectionClosed(
                            "The peer session closed before submission."
                        )
                    if self._next_id > self._limits.max_requests:
                        raise PeerError(
                            "Reconnect to start a new bounded request session."
                        )
                    # Recheck after waiting for a previous writer's backpressure.
                    if len(self._pending) >= self._limits.max_inflight:
                        raise PeerError(
                            "The client has reached its in-flight request limit."
                        )
                    request_id = self._next_id
                    self._next_id += 1
                    pending[request_id] = future
                    await write_frame(
                        writer,
                        {
                            "type": "request",
                            "id": request_id,
                            "operation": operation,
                            "payload": dict(payload or {}),
                        },
                    )
                result = await future
                if (
                    operation == "status"
                    and peer is not None
                    and peer.protocol_version == 2
                ):
                    self._validate_status(result, peer, capabilities)
                return result
        except TimeoutError as exc:
            await self._cancel_request(request_id, session_writer)
            raise RequestTimeout(
                "The peer request timed out; its result is unknown."
            ) from exc
        except asyncio.CancelledError:
            await self._cancel_request(request_id, session_writer)
            raise
        except ProtocolError:
            if self._writer is session_writer:
                await self.close()
            raise
        except (ConnectionClosed, OSError, asyncio.IncompleteReadError) as exc:
            if self._writer is session_writer:
                await self.close()
            raise ConnectionClosed("The peer disconnected during submission.") from exc
        finally:
            pending.pop(request_id, None)
            if not future.done():
                future.cancel()
            elif not future.cancelled():
                # Consume an exception set during failed submission/shutdown.
                future.exception()

    async def _cancel_request(
        self, request_id: int, writer: asyncio.StreamWriter | None
    ) -> None:
        if not request_id:
            return
        try:
            async with asyncio.timeout(1.0):
                async with self._send_lock:
                    if writer is not None and self._writer is writer:
                        await write_frame(writer, {"type": "cancel", "id": request_id})
        except (OSError, PeerError, TimeoutError):
            # Cancellation cannot promise rollback or acknowledgement.
            if self._writer is writer:
                await self.close()

    async def _receive(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        failure: PeerError = ConnectionClosed("The peer session ended.")
        try:
            while True:
                message = await asyncio.wait_for(
                    read_frame(reader), self._limits.io_timeout
                )
                request_id, result = self._reply(message)
                future = self._pending.get(request_id)
                if future is not None and not future.done():
                    if isinstance(result, RemoteError):
                        future.set_exception(result)
                    else:
                        future.set_result(result)
                # Replies to locally cancelled/timed-out requests are discarded.
        except PeerError as exc:
            failure = exc
        except (OSError, asyncio.IncompleteReadError, TimeoutError):
            pass
        finally:
            if self._writer is writer:
                self._writer = None
                self._capabilities = ()
                self._peer_info = None
                self._fail_pending(failure)
            await close_writer(writer)

    def _reply(self, message: Message) -> tuple[int, Message | RemoteError]:
        request_id = message.get("id")
        if type(request_id) is not int or request_id < 1 or request_id >= self._next_id:
            raise ProtocolError("The peer sent an invalid request identifier.")
        if message.get("type") == "result":
            if set(message) != {"type", "id", "result"} or not isinstance(
                message["result"], dict
            ):
                raise ProtocolError("The peer sent an invalid result envelope.")
            return request_id, message["result"]
        if (
            set(message) == {"type", "id", "code", "message"}
            and message["type"] == "error"
            and isinstance(message["code"], str)
            and message["code"] in _ERRORS
            and isinstance(message["message"], str)
            and len(message["message"]) <= 256
        ):
            # Never expose arbitrary peer-supplied error text in local logs.
            return request_id, RemoteError(message["code"], _ERRORS[message["code"]])
        raise ProtocolError("The peer sent an invalid reply envelope.")

    def _fail_pending(self, failure: PeerError) -> None:
        pending, self._pending = self._pending, {}
        for future in pending.values():
            if not future.done():
                future.set_exception(failure)

    async def _close_session(self) -> None:
        writer, self._writer = self._writer, None
        receiver, self._receiver = self._receiver, None
        self._capabilities = ()
        self._peer_info = None
        self._fail_pending(ConnectionClosed("The peer session was closed."))
        if receiver is not None:
            receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)
        if writer is not None:
            await close_writer(writer)

    async def close(self) -> None:
        """Cancel pending work, close the socket and await owned resources."""
        async with self._lifecycle_lock:
            await self._close_session()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()
