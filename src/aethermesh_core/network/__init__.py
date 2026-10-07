"""Versioned peer SDK and optional service sharing one bounded protocol.

Nothing connects, listens, generates credentials or starts background work on
import. Existing ``aethermesh_core`` local prototype imports remain compatible.
"""

from .client import PeerClient
from .config import Limits, PeerEndpoint, TLSIdentity, certificate_fingerprint
from .errors import (
    AuthenticationError,
    ConnectionClosed,
    PeerError,
    ProtocolError,
    RemoteError,
    RequestTimeout,
)
from .protocol import PROTOCOL_VERSION
from .service import PeerService

__all__ = [
    "PROTOCOL_VERSION",
    "AuthenticationError",
    "ConnectionClosed",
    "Limits",
    "PeerClient",
    "PeerEndpoint",
    "PeerError",
    "PeerService",
    "ProtocolError",
    "RemoteError",
    "RequestTimeout",
    "TLSIdentity",
    "certificate_fingerprint",
]
