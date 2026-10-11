"""Versioned peer SDK and optional service sharing one bounded protocol.

Nothing connects, listens, generates credentials or starts background work on
import. Existing ``aethermesh_core`` local prototype imports remain compatible.
"""

from .bundle import BundleInfo, inspect_bundle
from .bundle_export import export_bundle
from .client import PeerClient
from .config import Limits, PeerEndpoint, TLSIdentity, certificate_fingerprint
from .content import CONTENT_OPERATION, MAX_CONTENT_BYTES, ContentRef, ContentStore
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
from .transfer import MAX_TRANSFER_BYTES, TransferManifest, TransferStore

__all__ = [
    "CONTENT_OPERATION",
    "MAX_CONTENT_BYTES",
    "MAX_TRANSFER_BYTES",
    "PROTOCOL_VERSION",
    "ROUTER_EVALUATION_OPERATION",
    "SUPPORTED_PROTOCOL_VERSIONS",
    "AuthenticationError",
    "BundleInfo",
    "ConnectionClosed",
    "ContentRef",
    "ContentStore",
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
    "TransferManifest",
    "TransferStore",
    "certificate_fingerprint",
    "evaluation_task",
    "export_bundle",
    "hardware_profile",
    "inspect_bundle",
    "load_node_profile",
]
