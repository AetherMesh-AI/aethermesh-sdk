"""Stable failures for authenticated, explicitly configured peer sessions."""


class PeerError(Exception):
    """Base class for peer connection and operation failures."""


class AuthenticationError(PeerError):
    """A TLS identity, trust chain, or pinned peer identity was rejected."""


class ProtocolError(PeerError):
    """A peer sent an invalid or unsupported protocol message."""


class ConnectionClosed(PeerError):
    """The peer stream closed before an operation completed."""


class RequestTimeout(PeerError):
    """A connection or operation exceeded its configured deadline."""


class EndpointUnavailable(ConnectionClosed):
    """Transport could not be established; no protocol negotiation was started."""


class EndpointTimeout(RequestTimeout):
    """Transport establishment timed out before protocol negotiation started."""


class RemoteError(PeerError):
    """An authenticated peer rejected a request with a stable error code."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
