"""Bounded, self-reported node metadata with explicit hardware disclosure.

TLS authenticates a certificate, not these facts. Matching an expected node ID
does not attest physical hardware, ownership, capacity, or peer honesty.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from aethermesh_core import identity as identity_module
from aethermesh_core.identity import HardwareIdentityInputs
from aethermesh_core.models import NodeIdentity

from .config import validate_fingerprint
from .errors import ProtocolError

_ARCHITECTURES = frozenset({"x86_64", "aarch64", "arm", "x86", "other", "unknown"})
_RAM_BUCKETS = frozenset(
    {"unknown", "under16", "16to31", "32to63", "64to127", "128plus"}
)
_VRAM_BUCKETS = frozenset({"unknown", "under8", "8to15", "16to31", "32plus"})
_HARDWARE_FIELDS = frozenset(
    {"cpu_architecture", "ram_gb_bucket", "gpu_available", "gpu_vram_gb_bucket"}
)
_PROFILE_FIELDS = frozenset({"node_id", "node_name", "hardware"})


def validate_node_id(value: str) -> None:
    """Require the local identity's reference-safe ASCII alphabet, bounded on wire."""
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9._:-]{1,128}", value) is None:
        raise ValueError(
            "node_id must contain 1 to 128 reference-safe ASCII characters"
        )


@dataclass(frozen=True)
class HardwareProfile:
    """Coarse optional facts; unknown GPU availability is distinct from absence."""

    cpu_architecture: str = "unknown"
    ram_gb_bucket: str = "unknown"
    gpu_available: bool | None = None
    gpu_vram_gb_bucket: str = "unknown"

    def __post_init__(self) -> None:
        for value, choices in (
            (self.cpu_architecture, _ARCHITECTURES),
            (self.ram_gb_bucket, _RAM_BUCKETS),
            (self.gpu_vram_gb_bucket, _VRAM_BUCKETS),
        ):
            if type(value) is not str or value not in choices:
                raise ValueError("Invalid hardware profile category")
        if self.gpu_available is not None and type(self.gpu_available) is not bool:
            raise ValueError("gpu_available must be a boolean or None")

    def to_dict(self) -> dict[str, Any]:
        """Return only the four explicitly shareable coarse fields."""
        return {
            "cpu_architecture": self.cpu_architecture,
            "ram_gb_bucket": self.ram_gb_bucket,
            "gpu_available": self.gpu_available,
            "gpu_vram_gb_bucket": self.gpu_vram_gb_bucket,
        }


@dataclass(frozen=True)
class NodeProfile:
    """Bounded local identity metadata, with hardware omitted unless opted in."""

    node_id: str
    node_name: str | None = None
    hardware: HardwareProfile | None = None

    def __post_init__(self) -> None:
        validate_node_id(self.node_id)
        if self.node_name is not None:
            validate_node_id(self.node_name)
        if self.hardware is not None and type(self.hardware) is not HardwareProfile:
            raise ValueError("hardware must be a HardwareProfile or None")

    def to_dict(self) -> dict[str, Any]:
        """Serialize the exact public shape, retaining explicit unknown values."""
        return {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "hardware": None if self.hardware is None else self.hardware.to_dict(),
        }

    @classmethod
    def from_dict(cls, value: object) -> NodeProfile:
        """Reject unexpected fields and invalid values without echoing peer data."""
        if type(value) is not dict or value.keys() != _PROFILE_FIELDS:
            raise ProtocolError("Invalid node profile")
        hardware_value = value["hardware"]
        hardware = None
        try:
            if hardware_value is not None:
                if (
                    type(hardware_value) is not dict
                    or hardware_value.keys() != _HARDWARE_FIELDS
                ):
                    raise ProtocolError("Invalid node profile")
                hardware = HardwareProfile(**hardware_value)
            return cls(value["node_id"], value["node_name"], hardware)
        except ValueError:
            raise ProtocolError("Invalid node profile") from None

    @classmethod
    def from_identity(
        cls, identity: NodeIdentity, *, hardware: HardwareProfile | None = None
    ) -> NodeProfile:
        """Preserve a local identity verbatim; do not derive or collect anything."""
        return cls(identity.node_id, identity.node_name, hardware)


@dataclass(frozen=True)
class PeerInfo:
    """Authenticated certificate plus self-reported metadata for one session.

    ``identity_pinned`` records a configured node-ID match, never hardware
    attestation. A version-1 peer has no negotiated node profile.
    """

    fingerprint: str
    protocol_version: int
    profile: NodeProfile | None
    identity_pinned: bool = False

    def __post_init__(self) -> None:
        validate_fingerprint(self.fingerprint)
        if type(self.protocol_version) is not int or self.protocol_version not in (
            1,
            2,
        ):
            raise ValueError("protocol_version must be a supported integer version")
        if self.profile is not None and type(self.profile) is not NodeProfile:
            raise ValueError("profile must be a NodeProfile or None")
        if self.protocol_version == 1 and self.profile is not None:
            raise ValueError("Version 1 does not exchange node profiles")
        if type(self.identity_pinned) is not bool:
            raise ValueError("identity_pinned must be a boolean")
        if self.identity_pinned and self.profile is None:
            raise ValueError("An identity pin requires a node profile")


def _architecture(value: str) -> str:
    normalized = value.strip().lower()
    if normalized in {"", "unknown", "unknown_cpu_architecture"}:
        return "unknown"
    if normalized in {"amd64", "x86_64", "x64"}:
        return "x86_64"
    if normalized in {"arm64", "aarch64"}:
        return "aarch64"
    if normalized in {"x86", "i386", "i486", "i586", "i686", "ia32"}:
        return "x86"
    if normalized == "arm" or re.fullmatch(r"armv[5-8][a-z]*", normalized):
        return "arm"
    return "other"


def _positive_number(value: object) -> float | None:
    if type(value) not in (str, int, float):
        return None
    try:
        number = float(cast(str | int | float, value))
    except (ValueError, OverflowError):
        return None
    return number if math.isfinite(number) and number > 0 else None


def _bucket(value: object, boundaries: tuple[int, ...], names: tuple[str, ...]) -> str:
    number = _positive_number(value)
    if number is None:
        return "unknown"
    for boundary, name in zip(boundaries, names, strict=False):
        if number < boundary:
            return name
    return names[-1]


def _gpu_evidence(value: str) -> bool:
    normalized = value.strip().upper()
    return bool(normalized) and normalized not in {
        "UNKNOWN",
        "UNKNOWN_GPU_VENDOR",
        "UNKNOWN_GPU_MODEL",
        "UNKNOWN_GPU_DEVICE_ID",
        "NO_GPU_DETECTED",
        "NONE",
        "N/A",
    }


def hardware_profile(inputs: HardwareIdentityInputs) -> HardwareProfile:
    """Project already-collected facts into coarse fields, without further probes.

    Failed or unsupported collection cannot establish that a GPU is absent.
    The projection therefore reports True when evidence exists and None otherwise.
    A count alone is insufficient: a legacy collector defaults it to one on failure.
    Raw addresses, chip/model names, device IDs, and core counts are never returned.
    """
    gpu_known = _positive_number(inputs.max_gpu_vram_gb) is not None or any(
        _gpu_evidence(value)
        for value in (
            inputs.gpu_vendor,
            inputs.gpu_model_or_chip_name,
            inputs.gpu_device_id_if_available,
        )
    )
    return HardwareProfile(
        cpu_architecture=_architecture(inputs.cpu_architecture),
        ram_gb_bucket=_bucket(
            inputs.total_installed_ram_gb,
            (16, 32, 64, 128),
            ("under16", "16to31", "32to63", "64to127", "128plus"),
        ),
        gpu_available=True if gpu_known else None,
        gpu_vram_gb_bucket=_bucket(
            inputs.max_gpu_vram_gb,
            (8, 16, 32),
            ("under8", "8to15", "16to31", "32plus"),
        ),
    )


def load_node_profile(
    path: Path, *, create: bool = False, include_hardware: bool = False
) -> NodeProfile:
    """Read an explicit identity path; creation and hardware collection are opt-in.

    Existing identity files are validated and never rewritten or re-derived.
    Explicit creation retains the existing hardware-derived identity behavior.
    Hardware facts are collected only for an explicitly requested projection.
    """
    if type(create) is not bool or type(include_hardware) is not bool:
        raise ValueError("create and include_hardware must be booleans")
    identity = (
        identity_module.load_or_create_identity(path)
        if create and not path.exists()
        else identity_module.read_identity(path)
    )
    hardware = None
    if include_hardware:
        hardware = hardware_profile(
            identity_module.collect_hardware_identity_inputs(
                goos=identity_module._default_goos(),
                read_file=identity_module._read_text_file,
                run_command=identity_module._run_command,
            )
        )
    return NodeProfile.from_identity(identity, hardware=hardware)
