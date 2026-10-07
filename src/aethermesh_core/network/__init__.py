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
from .evaluation import (
    ROUTER_EVALUATION_OPERATION,
    EvaluationBudget,
    RouterEvaluator,
    evaluation_task,
)
from .profile import (
    HardwareProfile,
    NodeProfile,
    PeerInfo,
    hardware_profile,
    load_node_profile,
)
from .protocol import PROTOCOL_VERSION, SUPPORTED_PROTOCOL_VERSIONS
from .service import PeerService

__all__ = [
    "PROTOCOL_VERSION",
    "ROUTER_EVALUATION_OPERATION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "AuthenticationError",
    "ConnectionClosed",
    "EvaluationBudget",
    "HardwareProfile",
    "Limits",
    "NodeProfile",
    "PeerClient",
    "PeerEndpoint",
    "PeerError",
    "PeerInfo",
    "PeerService",
    "ProtocolError",
    "RemoteError",
    "RequestTimeout",
    "RouterEvaluator",
    "TLSIdentity",
    "certificate_fingerprint",
    "evaluation_task",
    "hardware_profile",
    "load_node_profile",
]
