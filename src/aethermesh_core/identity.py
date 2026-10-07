"""Local node identity persistence helpers."""

from __future__ import annotations

import csv
import json
import logging
import os
import platform
import re
import secrets
import shutil
import subprocess  # nosec B404 - fixed local hardware probe commands only; no user input.
import sys
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from hashlib import sha256
from io import StringIO
from pathlib import Path
from typing import TextIO

from aethermesh_core.json_io import atomic_create_json, atomic_write_json
from aethermesh_core.models import NodeIdentity
from aethermesh_core.version_metadata import (
    VersionMetadataError,
    capture_version_metadata,
    validate_version_metadata,
)

IDENTITY_SCHEMA_VERSION = 1
IDENTITY_CREATED_BY = "aethermesh_core.identity.load_or_create_identity"
IDENTITY_PROVENANCE_SOURCE = "local-first-initialization"
IDENTITY_AUTHORITY = "local-only-no-network-consensus"
LOCAL_NODE_IDENTITY_VERSION = 1
LOCAL_NODE_IDENTITY_REQUIRED_FIELDS = frozenset(
    {
        "node_id",
        "creator_node_id",
        "created_at",
        "identity_version",
        "public_key",
        "manifest_ref",
    }
)
LOCAL_NODE_IDENTITY_SECRET_FIELD_FRAGMENTS = (
    "private_key",
    "secret_key",
    "secret_seed",
    "seed",
    "mnemonic",
    "password",
)
LOCAL_NODE_IDENTITY_FORBIDDEN_MANIFEST_REF_PREFIXES = (
    "http://",
    "https://",
    "registry://",
    "token://",
    "dashboard://",
)
UNKNOWN_CPU_ARCHITECTURE = "UNKNOWN_CPU_ARCHITECTURE"
UNKNOWN_CPU_VENDOR = "UNKNOWN_CPU_VENDOR"
UNKNOWN_CPU_BRAND = "UNKNOWN_CPU_BRAND"
UNKNOWN_PHYSICAL_CORE_COUNT = "UNKNOWN_PHYSICAL_CORE_COUNT"
UNKNOWN_LOGICAL_THREAD_COUNT = "UNKNOWN_LOGICAL_THREAD_COUNT"
UNKNOWN_GPU_VENDOR = "UNKNOWN_GPU_VENDOR"
UNKNOWN_GPU_MODEL = "UNKNOWN_GPU_MODEL"
UNKNOWN_GPU_DEVICE_ID = "UNKNOWN_GPU_DEVICE_ID"
UNKNOWN_GPU_COUNT = "UNKNOWN_GPU_COUNT"
UNKNOWN_GPU_VRAM = "UNKNOWN_GPU_VRAM"
UNKNOWN_RAM_GB = "UNKNOWN_RAM_GB"
NO_GPU_DETECTED = "NO_GPU_DETECTED"
NODE_NAME_WORDLIST_SIZE = 16384
NODE_NAME_WORDLIST_DIR = Path(__file__).parents[2] / "wordlists" / "node-names"
NODE_NAME_WORDLIST_FILES = {
    "cpu": "cpu-traits.txt",
    "mac": "mac-signals.txt",
    "gpu": "gpu-compute.txt",
    "ram": "ram-memory.txt",
}
LOGGER = logging.getLogger(__name__)

FileReader = Callable[[str], str]
CommandRunner = Callable[..., str]
HostnameReader = Callable[[], str]
NodeIdFactory = Callable[[], str]


@dataclass(frozen=True)
class HardwareIdentityInputs:
    """Normalized-source hardware facts used to derive the public node id."""

    cpu_architecture: str
    cpu_vendor: str
    cpu_brand_or_chip_name: str
    physical_core_count: int | str
    logical_thread_count: int | str
    permanent_mac_addresses: tuple[str, ...]
    gpu_vendor: str
    gpu_model_or_chip_name: str
    gpu_device_id_if_available: str
    gpu_count: int | str
    max_gpu_vram_gb: float | int | str
    total_installed_ram_gb: float | int | str


@dataclass(frozen=True)
class HardwareComponentHashes:
    """Readable hardware hash derivation details for tests and debug output."""

    cpu_input: str
    cpu_hash: str
    mac_input: str
    mac_hash: str
    gpu_input: str
    gpu_hash: str
    ram_input: str
    ram_hash: str


class IdentityPersistenceError(ValueError):
    """Raised when local identity JSON cannot be safely loaded or saved."""


@dataclass(frozen=True)
class LocalNodeIdentity:
    """Validated version 1 public local node identity document."""

    node_id: str
    creator_node_id: str
    created_at: str
    identity_version: int
    public_key: str
    manifest_ref: str
    local_metadata: dict[str, object] | None = None

    def to_document(self) -> dict[str, object]:
        """Return the public identity document without private key material."""

        document: dict[str, object] = {
            "node_id": self.node_id,
            "creator_node_id": self.creator_node_id,
            "created_at": self.created_at,
            "identity_version": self.identity_version,
            "public_key": self.public_key,
            "manifest_ref": self.manifest_ref,
        }
        if self.local_metadata is not None:
            document["local_metadata"] = dict(self.local_metadata)
        return document


@dataclass(frozen=True)
class IdentityResetResult:
    """Local audit details for an explicit identity reset."""

    previous_node_id: str
    new_node_id: str
    backup_path: str
    audit_receipt_path: str
    warning: str

    def to_dict(self) -> dict[str, object]:
        """Return JSON-serializable reset details for CLI/API callers."""

        return {
            "previous_node_id": self.previous_node_id,
            "new_node_id": self.new_node_id,
            "backup_path": self.backup_path,
            "audit_receipt_path": self.audit_receipt_path,
            "warning": self.warning,
        }


def parse_local_node_identity_document(document: object) -> LocalNodeIdentity:
    """Parse and validate the Phase 1 public local node identity shape."""

    if not isinstance(document, dict):
        raise IdentityPersistenceError("local node identity must be a JSON object")
    _reject_secret_identity_fields(document)

    missing_fields = sorted(LOCAL_NODE_IDENTITY_REQUIRED_FIELDS - document.keys())
    if missing_fields:
        raise IdentityPersistenceError(
            "local node identity missing required fields: " + ", ".join(missing_fields)
        )

    node_id = _required_identity_string(document, "node_id")
    _require_reference_safe_identity_value("node_id", node_id)
    creator_node_id = _required_identity_string(document, "creator_node_id")
    _require_reference_safe_identity_value("creator_node_id", creator_node_id)
    created_at = _required_identity_string(document, "created_at")
    _require_identity_timestamp(created_at)
    identity_version = document["identity_version"]
    if (
        not isinstance(identity_version, int)
        or isinstance(identity_version, bool)
        or identity_version != LOCAL_NODE_IDENTITY_VERSION
    ):
        raise IdentityPersistenceError("local node identity_version must be integer 1")
    public_key = _required_identity_string(document, "public_key")
    manifest_ref = _required_identity_string(document, "manifest_ref")
    _require_local_manifest_ref(manifest_ref)

    local_metadata = document.get("local_metadata")
    if local_metadata is not None and not isinstance(local_metadata, dict):
        raise IdentityPersistenceError(
            "local node identity local_metadata must be an object when present"
        )

    return LocalNodeIdentity(
        node_id=node_id,
        creator_node_id=creator_node_id,
        created_at=created_at,
        identity_version=identity_version,
        public_key=public_key,
        manifest_ref=manifest_ref,
        local_metadata=dict(local_metadata) if local_metadata is not None else None,
    )


def _required_identity_string(document: dict[str, object], field_name: str) -> str:
    value = document[field_name]
    if not isinstance(value, str) or not value.strip():
        raise IdentityPersistenceError(
            f"local node identity field '{field_name}' must be a non-empty string"
        )
    return value


def _require_reference_safe_identity_value(field_name: str, value: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9._:-]+", value):
        raise IdentityPersistenceError(
            f"local node identity field '{field_name}' must be reference-safe"
        )


def _require_identity_timestamp(value: str) -> None:
    normalized = value.replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise IdentityPersistenceError(
            "local node identity created_at must be an ISO 8601 timestamp"
        ) from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise IdentityPersistenceError(
            "local node identity created_at must include a timezone"
        )
    if parsed.utcoffset() != timedelta(0):
        raise IdentityPersistenceError(
            "local node identity created_at must be a UTC timestamp"
        )


def _require_local_manifest_ref(value: str) -> None:
    lowered = value.lower()
    path_part, separator, fragment = value.partition("#")
    if (
        value != value.strip()
        or not path_part
        or not re.fullmatch(r"[A-Za-z0-9._/-]+", path_part)
        or "://" in lowered
        or lowered.startswith(LOCAL_NODE_IDENTITY_FORBIDDEN_MANIFEST_REF_PREFIXES)
        or path_part.startswith(("/", "~"))
        or re.match(r"^[A-Za-z]:[\\/]", path_part) is not None
        or ".." in Path(path_part).parts
        or (separator and not re.fullmatch(r"[A-Za-z0-9._:-]+", fragment))
    ):
        raise IdentityPersistenceError(
            "local node identity manifest_ref must be a local file or fixture reference"
        )
    if _contains_secret_identity_fragment(lowered):
        raise IdentityPersistenceError(
            "local node identity manifest_ref must not reference private key material"
        )


def _contains_secret_identity_fragment(value: str) -> bool:
    normalized = re.sub(r"[^a-z0-9]", "", value.lower())
    return any(
        fragment in value.lower() or re.sub(r"[^a-z0-9]", "", fragment) in normalized
        for fragment in LOCAL_NODE_IDENTITY_SECRET_FIELD_FRAGMENTS
    )


def _reject_secret_identity_fields(value: object) -> None:
    if isinstance(value, dict):
        for key, nested_value in value.items():
            if _contains_secret_identity_fragment(str(key)):
                raise IdentityPersistenceError(
                    "local node identity must not contain private key material"
                )
            _reject_secret_identity_fields(nested_value)
    elif isinstance(value, list):
        for nested_value in value:
            _reject_secret_identity_fields(nested_value)


def read_identity(path: str | Path) -> NodeIdentity:
    """Read and validate a saved identity without probing, creating, or updating it."""

    identity_path = Path(path)
    return _validated_identity_from_document(
        _load_identity_document(identity_path), identity_path=identity_path
    )


def load_or_create_identity(
    path: str | Path,
    *,
    goos: str | None = None,
    read_file: FileReader | None = None,
    run_command: CommandRunner | None = None,
    read_hostname: HostnameReader | None = None,
    hardware_inputs: HardwareIdentityInputs | None = None,
    account_id: str | None = None,
    node_id_factory: NodeIdFactory | None = None,
) -> NodeIdentity:
    """Load a versioned local node identity, creating one if the file is missing."""

    identity_path = Path(path)
    if identity_path.exists():
        return _load_identity(identity_path)

    node_id = (
        node_id_factory()
        if node_id_factory is not None
        else deterministic_machine_node_id(
            goos=goos,
            read_file=read_file,
            run_command=run_command,
            read_hostname=read_hostname,
            hardware_inputs=hardware_inputs,
            account_id=account_id,
        )
    )
    identity = NodeIdentity(
        node_id=node_id,
        node_name=deterministic_machine_node_name(
            goos=goos,
            read_file=read_file,
            run_command=run_command,
            read_hostname=read_hostname,
            hardware_inputs=hardware_inputs,
            account_id=account_id,
            node_id=node_id,
        ),
    )
    try:
        _save_identity(identity_path, identity)
    except IdentityPersistenceError as exc:
        if identity_path.exists() and "already exists" in str(exc):
            return _load_identity(identity_path)
        raise
    return identity


def reset_identity(
    path: str | Path,
    *,
    reason: str | None = None,
    quarantine_dir: str | Path | None = None,
    audit_receipt_path: str | Path | None = None,
    rotate_creator_identity: bool = False,
    hardware_inputs: HardwareIdentityInputs | None = None,
    node_id_factory: NodeIdFactory | None = None,
) -> IdentityResetResult:
    """Explicitly replace a persisted local identity after quarantining the old one.

    Normal initialization must call ``load_or_create_identity``. This reset flow is
    intentionally separate so identity replacement is local-first, auditable, and
    never hidden behind routine startup.
    """

    identity_path = Path(path)
    if not identity_path.exists():
        raise IdentityPersistenceError(
            "identity reset requires an existing identity file; use normal init first"
        )
    previous = _load_identity(identity_path)
    previous_document = _load_identity_document(identity_path)
    previous_creator_node_id = _identity_document_creator_node_id(previous_document)
    timestamp = datetime.now(UTC).replace(microsecond=0).isoformat()
    backup_root = (
        Path(quarantine_dir)
        if quarantine_dir is not None
        else identity_path.parent / "identity-quarantine"
    )
    backup_root.mkdir(parents=True, exist_ok=True)
    backup_path = _unique_reset_artifact_path(
        backup_root,
        f"{identity_path.stem}-{_timestamp_slug(timestamp)}-{previous.node_id[:12]}",
        identity_path.suffix or ".json",
    )
    try:
        shutil.copy2(identity_path, backup_path)
    except OSError as exc:
        raise IdentityPersistenceError(
            f"could not quarantine previous identity file: {exc}"
        ) from exc
    backed_up_refs = _backup_identity_referenced_artifacts(
        previous_document,
        identity_path=identity_path,
        backup_root=backup_root,
    )

    node_id = (
        node_id_factory()
        if node_id_factory is not None
        else deterministic_machine_node_id(hardware_inputs=hardware_inputs)
    )
    new_identity = NodeIdentity(
        node_id=node_id,
        node_name=deterministic_machine_node_name(
            hardware_inputs=hardware_inputs,
            node_id=node_id,
        ),
    )
    new_creator_node_id = (
        node_id if rotate_creator_identity else previous_creator_node_id
    )
    receipt_path = (
        Path(audit_receipt_path)
        if audit_receipt_path is not None
        else backup_root / "identity-reset-receipts.json"
    )
    _load_identity_reset_receipts(receipt_path)
    _save_identity(
        identity_path,
        new_identity,
        overwrite_existing=True,
        creator_node_id=new_creator_node_id,
    )
    try:
        _append_identity_reset_receipt(
            receipt_path,
            timestamp=timestamp,
            identity_path=identity_path,
            backup_path=backup_path,
            previous_node_id=previous.node_id,
            new_node_id=new_identity.node_id,
            previous_creator_node_id=previous_creator_node_id,
            new_creator_node_id=new_creator_node_id,
            rotate_creator_identity=rotate_creator_identity,
            files_backed_up=[backup_path, *backed_up_refs],
            reason=reason,
        )
    except IdentityPersistenceError:
        try:
            shutil.copy2(backup_path, identity_path)
        except OSError as exc:
            raise IdentityPersistenceError(
                "identity reset audit receipt failed after replacing identity, "
                f"and previous identity restoration failed: {exc}"
            ) from exc
        raise
    return IdentityResetResult(
        previous_node_id=previous.node_id,
        new_node_id=new_identity.node_id,
        backup_path=str(backup_path),
        audit_receipt_path=str(receipt_path),
        warning=_identity_reset_warning(),
    )


def _new_local_node_id() -> str:
    """Return a legacy collision-resistant local node id for explicit rotations."""

    return secrets.token_hex(32)


def deterministic_machine_node_id(
    *,
    debug: bool = False,
    output: TextIO | None = None,
    goos: str | None = None,
    read_file: Callable[[str], str] | None = None,
    run_command: Callable[..., str] | None = None,
    read_hostname: Callable[[], str] | None = None,
    hardware_inputs: HardwareIdentityInputs | None = None,
    account_id: str | None = None,
) -> str:
    """Return NODE_ID as the deterministic hardware-root SHA256 hex string.

    account_id is accepted only to keep account ownership concerns separate from
    node identity derivation. It is intentionally not used in the calculation.
    """

    del account_id, read_hostname
    hardware = _resolved_hardware_inputs(goos, read_file, run_command, hardware_inputs)
    hashes = _component_hashes(hardware)
    root_json = _canonical_root_json(hashes)
    node_id = _sha256_hex(root_json)
    if debug:
        print_hardware_node_id_debug(
            hashes=hashes,
            root_json=root_json,
            node_id=node_id,
            node_name=_node_name_from_hashes(hashes, node_id),
            output=output,
        )
    return node_id


def deterministic_machine_node_name(
    *,
    node_id: str | None = None,
    account_id: str | None = None,
    hardware_inputs: HardwareIdentityInputs | None = None,
    read_hostname: Callable[[], str] | None = None,
    run_command: Callable[..., str] | None = None,
    read_file: Callable[[str], str] | None = None,
    goos: str | None = None,
) -> str:
    """Return the deterministic display-only node name for hardware inputs."""

    del account_id, read_hostname
    hardware = _resolved_hardware_inputs(goos, read_file, run_command, hardware_inputs)
    hashes = _component_hashes(hardware)
    effective_node_id = node_id or _sha256_hex(_canonical_root_json(hashes))
    return _node_name_from_hashes(hashes, effective_node_id)


def _resolved_hardware_inputs(
    goos: str | None,
    read_file: Callable[[str], str] | None,
    run_command: Callable[..., str] | None,
    hardware_inputs: HardwareIdentityInputs | None,
) -> HardwareIdentityInputs:
    return hardware_inputs or collect_hardware_identity_inputs(
        goos=_default_goos() if goos is None else goos,
        read_file=_read_text_file if read_file is None else read_file,
        run_command=_run_command if run_command is None else run_command,
    )


def collect_hardware_identity_inputs(
    *,
    goos: str,
    read_file: FileReader,
    run_command: CommandRunner,
) -> HardwareIdentityInputs:
    """Collect hardware facts without account, install, hostname, or OS IDs."""

    if goos == "darwin":
        return _darwin_hardware_inputs(run_command)
    if goos == "linux":
        return _linux_hardware_inputs(read_file, run_command)
    if goos == "windows":
        return _windows_hardware_inputs(run_command)
    return HardwareIdentityInputs(
        cpu_architecture=platform.machine(),
        cpu_vendor="",
        cpu_brand_or_chip_name=platform.processor(),
        physical_core_count=_physical_core_count_fallback(),
        logical_thread_count=os.cpu_count() or 0,
        permanent_mac_addresses=(),
        gpu_vendor="",
        gpu_model_or_chip_name="",
        gpu_device_id_if_available="",
        gpu_count=0,
        max_gpu_vram_gb="",
        total_installed_ram_gb=0,
    )


def print_hardware_node_id_debug(
    *,
    hashes: HardwareComponentHashes,
    root_json: str,
    node_id: str,
    node_name: str,
    output: TextIO | None = None,
) -> None:
    """Print the full hardware-root derivation in a readable debug format."""

    destination = sys.stdout if output is None else output
    lines = [
        f"[DEBUG] CPU_INPUT = {hashes.cpu_input}",
        f"[DEBUG] CPU_HASH = {hashes.cpu_hash}",
        f"[DEBUG] MAC_INPUT = {hashes.mac_input}",
        f"[DEBUG] MAC_HASH = {hashes.mac_hash}",
        f"[DEBUG] GPU_INPUT = {hashes.gpu_input}",
        f"[DEBUG] GPU_HASH = {hashes.gpu_hash}",
        f"[DEBUG] RAM_INPUT = {hashes.ram_input}",
        f"[DEBUG] RAM_HASH = {hashes.ram_hash}",
        f"[DEBUG] ROOT_JSON = {root_json}",
        f"[DEBUG] NODE_ID = {node_id}",
        f"[DEBUG] NODE_NAME = {node_name}",
    ]
    print("\n".join(lines), file=destination)


def _component_hashes(hardware: HardwareIdentityInputs) -> HardwareComponentHashes:
    cpu_input = _join_normalized(
        (
            (hardware.cpu_architecture, UNKNOWN_CPU_ARCHITECTURE),
            (hardware.cpu_vendor, UNKNOWN_CPU_VENDOR),
            (hardware.cpu_brand_or_chip_name, UNKNOWN_CPU_BRAND),
            (hardware.physical_core_count, UNKNOWN_PHYSICAL_CORE_COUNT),
            (hardware.logical_thread_count, UNKNOWN_LOGICAL_THREAD_COUNT),
        )
    )
    mac_input = "|".join(
        sorted(
            {
                normalized
                for value in hardware.permanent_mac_addresses
                if (normalized := _normalize_mac_address(value))
            }
        )
    )
    gpu_input = _gpu_input(hardware)
    ram_input = _rounded_gb_input(hardware.total_installed_ram_gb, UNKNOWN_RAM_GB)
    return HardwareComponentHashes(
        cpu_input=cpu_input,
        cpu_hash=_sha256_hex(cpu_input),
        mac_input=mac_input,
        mac_hash=_sha256_hex(mac_input),
        gpu_input=gpu_input,
        gpu_hash=_sha256_hex(gpu_input),
        ram_input=ram_input,
        ram_hash=_sha256_hex(ram_input),
    )


def _canonical_root_json(hashes: HardwareComponentHashes) -> str:
    return json.dumps(
        {
            "cpu_hash": hashes.cpu_hash,
            "mac_hash": hashes.mac_hash,
            "gpu_hash": hashes.gpu_hash,
            "ram_hash": hashes.ram_hash,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _node_name_from_hashes(hashes: HardwareComponentHashes, node_id: str) -> str:
    wordlists = _node_name_wordlists()
    words = (
        wordlists["cpu"][_index_from_hash(hashes.cpu_hash)],
        wordlists["mac"][_index_from_hash(hashes.mac_hash)],
        wordlists["gpu"][_index_from_hash(hashes.gpu_hash)],
        wordlists["ram"][_index_from_hash(hashes.ram_hash)],
    )
    return f"{'-'.join(words)}_{node_id[:6]}"


def _index_from_hash(hash_hex: str) -> int:
    return int(hash_hex[:16], 16) % NODE_NAME_WORDLIST_SIZE


@lru_cache(maxsize=1)
def _node_name_wordlists() -> dict[str, tuple[str, ...]]:
    wordlist_dir = _node_name_wordlist_dir()
    return {
        key: tuple((wordlist_dir / filename).read_text(encoding="ascii").splitlines())
        for key, filename in NODE_NAME_WORDLIST_FILES.items()
    }


def _node_name_wordlist_dir() -> Path:
    if NODE_NAME_WORDLIST_DIR.exists():
        return NODE_NAME_WORDLIST_DIR
    return Path(sys.prefix) / "wordlists" / "node-names"


def _gpu_input(hardware: HardwareIdentityInputs) -> str:
    gpu_count = _safe_int(hardware.gpu_count)
    gpu_detected = gpu_count > 0 or any(
        _normalize_value(value)
        for value in (
            hardware.gpu_vendor,
            hardware.gpu_model_or_chip_name,
            hardware.gpu_device_id_if_available,
            hardware.max_gpu_vram_gb,
        )
    )
    if not gpu_detected:
        return NO_GPU_DETECTED
    return _join_normalized(
        (
            (hardware.gpu_vendor, UNKNOWN_GPU_VENDOR),
            (hardware.gpu_model_or_chip_name, UNKNOWN_GPU_MODEL),
            (hardware.gpu_device_id_if_available, UNKNOWN_GPU_DEVICE_ID),
            (hardware.gpu_count, UNKNOWN_GPU_COUNT),
            (
                _rounded_gb_input(hardware.max_gpu_vram_gb, UNKNOWN_GPU_VRAM),
                UNKNOWN_GPU_VRAM,
            ),
        )
    )


def _join_normalized(values: Iterable[tuple[object, str]]) -> str:
    return "|".join(
        _normalize_value(value, placeholder) for value, placeholder in values
    )


def _normalize_value(value: object, placeholder: str = "") -> str:
    normalized = re.sub(r"\s+", " ", str(value).strip()).upper()
    return normalized or placeholder


def _rounded_gb_input(value: object, placeholder: str) -> str:
    if str(value).strip() == "":
        return placeholder
    rounded = _round_installed_ram_gb(value)
    if rounded <= 0:
        return placeholder
    return str(rounded)


def _normalize_mac_address(value: object) -> str:
    normalized = re.sub(r"[^0-9A-Fa-f]", "", str(value).strip()).upper()
    if len(normalized) != 12 or normalized == "000000000000":
        return ""
    return normalized


def _round_installed_ram_gb(value: object) -> int:
    try:
        return int(float(str(value).strip()) + 0.5)
    except (TypeError, ValueError):
        return 0


def _sha256_hex(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _darwin_hardware_inputs(run_command: CommandRunner) -> HardwareIdentityInputs:
    displays = _run_or_empty(run_command, "system_profiler", "SPDisplaysDataType")
    return HardwareIdentityInputs(
        cpu_architecture=platform.machine(),
        cpu_vendor=_run_or_empty(run_command, "sysctl", "-n", "machdep.cpu.vendor"),
        cpu_brand_or_chip_name=_run_or_empty(
            run_command, "sysctl", "-n", "machdep.cpu.brand_string"
        ),
        physical_core_count=_run_or_empty(
            run_command, "sysctl", "-n", "hw.physicalcpu"
        ),
        logical_thread_count=_run_or_empty(
            run_command, "sysctl", "-n", "hw.logicalcpu"
        ),
        permanent_mac_addresses=tuple(
            _physical_mac_addresses(
                _run_or_empty(run_command, "networksetup", "-listallhardwareports"),
                source="darwin-networksetup",
            )
        ),
        gpu_vendor=_extract_labeled_value(displays, "Vendor"),
        gpu_model_or_chip_name=_extract_labeled_value(displays, "Chipset Model"),
        gpu_device_id_if_available=_extract_labeled_value(displays, "Device ID"),
        gpu_count=_count_display_chips(displays),
        max_gpu_vram_gb=_max_gpu_vram_gb(displays),
        total_installed_ram_gb=_bytes_to_gb(
            _safe_int(_run_or_empty(run_command, "sysctl", "-n", "hw.memsize"))
        ),
    )


def _linux_hardware_inputs(
    read_file: FileReader, run_command: CommandRunner
) -> HardwareIdentityInputs:
    cpuinfo = _read_or_empty(read_file, "/proc/cpuinfo")
    meminfo = _read_or_empty(read_file, "/proc/meminfo")
    lscpu = _run_or_empty(run_command, "lscpu")
    lspci = _run_or_empty(run_command, "lspci", "-nn")
    return HardwareIdentityInputs(
        cpu_architecture=platform.machine(),
        cpu_vendor=_linux_cpu_value(cpuinfo, lscpu, "vendor_id", "Vendor ID"),
        cpu_brand_or_chip_name=_linux_cpu_value(
            cpuinfo, lscpu, "model name", "Model name"
        ),
        physical_core_count=_linux_physical_core_count(cpuinfo, lscpu),
        logical_thread_count=os.cpu_count() or _linux_lscpu_int(lscpu, "CPU(s)") or 0,
        permanent_mac_addresses=tuple(
            _physical_mac_addresses(
                _run_or_empty(run_command, "ip", "-o", "link"),
                source="linux-ip-link",
            )
        ),
        gpu_vendor=_linux_gpu_vendor(lspci),
        gpu_model_or_chip_name=_linux_gpu_model(lspci),
        gpu_device_id_if_available=_linux_gpu_device_id(lspci),
        gpu_count=_linux_gpu_count(lspci),
        max_gpu_vram_gb=_max_gpu_vram_gb(lspci),
        total_installed_ram_gb=_linux_memtotal_gb(meminfo),
    )


def _windows_hardware_inputs(run_command: CommandRunner) -> HardwareIdentityInputs:
    cpu = _run_or_empty(
        run_command,
        "wmic",
        "cpu",
        "get",
        "Manufacturer,Name,NumberOfCores,NumberOfLogicalProcessors",
        "/format:csv",
    )
    gpu = _run_or_empty(
        run_command,
        "wmic",
        "path",
        "win32_VideoController",
        "get",
        "AdapterCompatibility,Name,PNPDeviceID,AdapterRAM",
        "/format:csv",
    )
    ram = _run_or_empty(run_command, "wmic", "memorychip", "get", "capacity")
    macs = _run_or_empty(run_command, "getmac", "/fo", "csv", "/v")
    return HardwareIdentityInputs(
        cpu_architecture=platform.machine(),
        cpu_vendor=_csv_first_value(cpu, "Manufacturer"),
        cpu_brand_or_chip_name=_csv_first_value(cpu, "Name"),
        physical_core_count=_csv_first_value(cpu, "NumberOfCores"),
        logical_thread_count=_csv_first_value(cpu, "NumberOfLogicalProcessors"),
        permanent_mac_addresses=tuple(
            _physical_mac_addresses(macs, source="windows-getmac")
        ),
        gpu_vendor=_csv_first_value(gpu, "AdapterCompatibility"),
        gpu_model_or_chip_name=_csv_first_value(gpu, "Name"),
        gpu_device_id_if_available=_csv_first_value(gpu, "PNPDeviceID"),
        gpu_count=max(1, len([line for line in gpu.splitlines() if line.strip()][1:])),
        max_gpu_vram_gb=_bytes_to_gb(_max_csv_int(gpu, "AdapterRAM")),
        total_installed_ram_gb=_bytes_to_gb(
            sum(_safe_int(value) for value in re.findall(r"\b\d{8,}\b", ram))
        ),
    )


def _default_goos() -> str:
    if sys.platform.startswith("linux"):
        return "linux"
    if sys.platform == "darwin":
        return "darwin"
    if sys.platform.startswith("win"):
        return "windows"
    return sys.platform


def _read_text_file(path: str) -> str:
    return Path(path).read_text(encoding="utf-8").strip()


def _run_command(name: str, *args: str) -> str:
    return subprocess.check_output(  # nosec B603 - fixed hardware probes, shell disabled.
        [name, *args], stderr=subprocess.STDOUT, text=True
    ).strip()


def _read_or_empty(read_file: FileReader | None, path: str) -> str:
    if read_file is None:
        return ""
    try:
        return read_file(path).strip()
    except OSError:
        return ""


def _run_or_empty(
    run_command: CommandRunner | None,
    name: str,
    *args: str,
) -> str:
    if run_command is None:
        return ""
    try:
        return run_command(name, *args).strip()
    except (OSError, subprocess.SubprocessError):
        return ""


def _physical_core_count_fallback() -> int:
    count = os.cpu_count()
    if count is None:
        return 0
    return max(1, count // 2)


def _physical_mac_addresses(text: str, *, source: str) -> list[str]:
    if source == "darwin-networksetup":
        return _darwin_physical_mac_addresses(text)
    if source == "linux-ip-link":
        return _linux_physical_mac_addresses(text)
    if source == "windows-getmac":
        return _windows_physical_mac_addresses(text)
    return []


def _darwin_physical_mac_addresses(text: str) -> list[str]:
    addresses: list[str] = []
    for block in re.split(r"\n\s*\n", text.strip()):
        port = _extract_labeled_value(block, "Hardware Port")
        if not _is_darwin_physical_ethernet_or_wifi_port(port):
            continue
        address = _extract_labeled_value(block, "Ethernet Address")
        if _is_usable_mac_address(address):
            addresses.append(address)
    return addresses


def _linux_physical_mac_addresses(text: str) -> list[str]:
    addresses: list[str] = []
    for line in text.splitlines():
        match = re.match(r"\s*\d+:\s*([^:@]+)(?:@[^:]+)?:.*?link/ether\s+(\S+)", line)
        if not match:
            continue
        interface_name, address = match.groups()
        if _is_physical_linux_network_interface(
            interface_name
        ) and _is_usable_mac_address(address):
            addresses.append(address)
    return addresses


def _windows_physical_mac_addresses(text: str) -> list[str]:
    addresses: list[str] = []
    for row in csv.DictReader(StringIO(text)):
        description = " ".join(
            str(row.get(field, ""))
            for field in ("Connection Name", "Network Adapter", "Description")
        )
        address = str(
            row.get("Physical Address") or row.get("PhysicalAddress") or ""
        ).strip()
        if _is_physical_ethernet_or_wifi_name(description) and _is_usable_mac_address(
            address
        ):
            addresses.append(address)
    return addresses


def _is_physical_linux_network_interface(name: str) -> bool:
    lowered = name.lower()
    if _is_virtual_or_non_hardware_network_name(lowered):
        return False
    return lowered.startswith(("eth", "en", "wlan", "wl"))


def _is_darwin_physical_ethernet_or_wifi_port(name: str) -> bool:
    lowered = _normalize_network_name(name)
    if lowered in {"ethernet", "wi-fi", "wifi"}:
        return True
    if _is_virtual_or_non_hardware_network_name(lowered):
        return False
    if "adapter" in lowered or "iphone" in lowered:
        return False
    return "usb" in lowered and bool(
        re.search(r"\b(ethernet|lan|wi-?fi|wireless)\b", lowered)
    )


def _is_physical_ethernet_or_wifi_name(name: str) -> bool:
    lowered = _normalize_network_name(name)
    if _is_virtual_or_non_hardware_network_name(lowered):
        return False
    return bool(re.search(r"\b(ethernet|wi-?fi|wireless|802\.11)\b", lowered))


def _normalize_network_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.strip()).lower()


def _is_virtual_or_non_hardware_network_name(name: str) -> bool:
    if name.startswith(
        (
            "bridge",
            "docker",
            "veth",
            "vmnet",
            "awdl",
            "llw",
            "p2p",
            "utun",
            "tun",
            "tap",
        )
    ):
        return True
    return bool(
        re.search(
            r"\b(virtual|bridge|bluetooth|thunderbolt|tunnel|tap|tun|docker|veth|vmnet|hyper-v|loopback|awdl|llw|p2p|utun|vpn)\b",
            name,
        )
    )


def _is_usable_mac_address(value: str) -> bool:
    normalized = _normalize_mac_address(value)
    return bool(normalized and normalized != "FFFFFFFFFFFF")


def _extract_mac_addresses(text: str) -> list[str]:
    return [
        value
        for value in re.findall(r"(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}", text)
        if _normalize_mac_address(value) != "FFFFFFFFFFFF"
    ]


def _extract_labeled_value(text: str, label: str) -> str:
    pattern = re.compile(rf"^\s*{re.escape(label)}\s*:\s*(.+?)\s*$", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1) if match else ""


def _count_display_chips(text: str) -> int:
    models = re.findall(r"^\s*Chipset Model\s*:", text, flags=re.MULTILINE)
    return len(models)


def _max_gpu_vram_gb(text: str) -> float:
    values = [
        _vram_value_to_gb(amount, unit)
        for amount, unit in re.findall(
            r"(?:VRAM|AdapterRAM|Video Memory|Dedicated Memory|size)\s*(?:\([^)]*\))?\s*[:=]\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGT]?B?|[KMGT])?",
            text,
            flags=re.IGNORECASE,
        )
    ]
    return max(values, default=0)


def _vram_value_to_gb(amount: str, unit: str | None) -> float:
    try:
        value = float(amount)
    except ValueError:
        return 0
    normalized_unit = (unit or "GB").strip().upper()
    if normalized_unit in {"", "G", "GB"}:
        return value
    if normalized_unit in {"M", "MB"}:
        return value / 1024
    if normalized_unit in {"K", "KB"}:
        return value / float(1024**2)
    if normalized_unit in {"T", "TB"}:
        return value * 1024
    return value


def _bytes_to_gb(value: int) -> float:
    if value <= 0:
        return 0
    return value / float(1024**3)


def _linux_memtotal_gb(meminfo: str) -> float:
    match = re.search(r"^\s*MemTotal\s*:\s*(\d+)\s+kB", meminfo, flags=re.MULTILINE)
    if not match:
        return 0
    kib = _safe_int(match.group(1))
    return kib / float(1024**2)


def _linux_cpu_value(cpuinfo: str, lscpu: str, cpuinfo_key: str, lscpu_key: str) -> str:
    value = _colon_value(cpuinfo, cpuinfo_key)
    return value or _colon_value(lscpu, lscpu_key)


def _colon_value(text: str, key: str) -> str:
    pattern = re.compile(rf"^\s*{re.escape(key)}\s*:\s*(.+?)\s*$", re.MULTILINE)
    match = pattern.search(text)
    return match.group(1) if match else ""


def _linux_physical_core_count(cpuinfo: str, lscpu: str) -> int | str:
    pairs = set(
        re.findall(r"physical id\s*:\s*(\S+).*?core id\s*:\s*(\S+)", cpuinfo, re.DOTALL)
    )
    if pairs:
        return len(pairs)
    cores_per_socket = _linux_lscpu_int(lscpu, "Core(s) per socket")
    sockets = _linux_lscpu_int(lscpu, "Socket(s)")
    if cores_per_socket and sockets:
        return cores_per_socket * sockets
    return _physical_core_count_fallback()


def _linux_lscpu_int(lscpu: str, key: str) -> int:
    return _safe_int(_colon_value(lscpu, key))


def _linux_gpu_lines(lspci: str) -> list[str]:
    return [
        line
        for line in lspci.splitlines()
        if re.search(r"\b(VGA|3D|Display)\b", line, flags=re.IGNORECASE)
    ]


def _linux_gpu_count(lspci: str) -> int:
    return len(_linux_gpu_lines(lspci))


def _linux_gpu_model(lspci: str) -> str:
    lines = _linux_gpu_lines(lspci)
    if not lines:
        return ""
    match = re.search(r"\]:\s*(.+)$", lines[0])
    if match:
        return match.group(1)
    return re.sub(r"^[0-9A-Fa-f:.]+\s+[^:]+:\s*", "", lines[0])


def _linux_gpu_vendor(lspci: str) -> str:
    model = _linux_gpu_model(lspci)
    return model.split(" ", 1)[0] if model else ""


def _linux_gpu_device_id(lspci: str) -> str:
    lines = _linux_gpu_lines(lspci)
    if not lines:
        return ""
    match = re.search(r"\[([0-9A-Fa-f]{4}:[0-9A-Fa-f]{4})\]", lines[0])
    return match.group(1) if match else ""


def _csv_first_value(text: str, key: str) -> str:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return ""
    headers = [part.strip() for part in lines[0].split(",")]
    try:
        index = headers.index(key)
    except ValueError:
        return ""
    values = [part.strip() for part in lines[1].split(",")]
    return values[index] if index < len(values) else ""


def _max_csv_int(text: str, key: str) -> int:
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    if len(lines) < 2:
        return 0
    headers = [part.strip() for part in lines[0].split(",")]
    try:
        index = headers.index(key)
    except ValueError:
        return 0
    return max(
        (
            _safe_int(values[index])
            for values in (
                [part.strip() for part in line.split(",")] for line in lines[1:]
            )
            if index < len(values)
        ),
        default=0,
    )


def _safe_int(value: object) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _load_identity(path: Path) -> NodeIdentity:
    try:
        identity = _validated_identity_from_document(
            _load_identity_document(path), identity_path=path
        )
    except IdentityPersistenceError as exc:
        LOGGER.warning("identity validation failed for %s", path.name)
        raise IdentityPersistenceError(f"identity file {path.name}: {exc}") from exc
    LOGGER.info("identity validation passed for %s", path.name)
    return identity


def _validated_identity_from_document(
    document: dict[str, object], *, identity_path: Path
) -> NodeIdentity:
    version = document.get("version")
    if version != IDENTITY_SCHEMA_VERSION:
        raise IdentityPersistenceError("identity JSON must contain version 1")
    node = document.get("node")
    if not isinstance(node, dict):
        raise IdentityPersistenceError("identity JSON field 'node' must be an object")
    node_id = _required_identity_document_id(node, "node.node_id")
    node_name = node.get("node_name")
    if node_name is not None:
        if not isinstance(node_name, str) or not node_name.strip():
            raise IdentityPersistenceError(
                "identity JSON field 'node.node_name' must be a non-empty string when present"
            )
        _require_reference_safe_identity_value("node.node_name", node_name)
    creator_node_id = _identity_document_creator_node_id(document)
    created_at = node.get("created_at")
    if not isinstance(created_at, str) or not created_at:
        raise IdentityPersistenceError(
            "identity JSON field 'node.created_at' must be a non-empty string"
        )
    _require_identity_timestamp(created_at)
    provenance = document.get("provenance")
    if not isinstance(provenance, dict):
        raise IdentityPersistenceError(
            "identity JSON field 'provenance' must be an object"
        )
    _require_provenance_string(provenance, "created_by")
    _require_provenance_string(provenance, "source")
    _require_provenance_string(provenance, "creation_event")
    _require_provenance_string(provenance, "load_behavior")
    _require_provenance_string(provenance, "authority")
    references = document.get("references")
    if not isinstance(references, dict):
        raise IdentityPersistenceError(
            "identity JSON field 'references' must be an object"
        )
    manifest_refs = _require_string_list(references, "manifest_refs")
    validation_receipt_refs = _require_string_list(
        references, "validation_receipt_refs"
    )
    try:
        validate_version_metadata(references.get("version_metadata"))
    except VersionMetadataError as exc:
        raise IdentityPersistenceError(f"identity JSON {exc}") from exc
    lineage = document.get("lineage")
    if not isinstance(lineage, dict):
        raise IdentityPersistenceError(
            "identity JSON field 'lineage' must be an object"
        )
    parent_node_ids = _require_string_list(lineage, "parent_node_ids")
    lineage_links = _require_string_list(lineage, "lineage_links")
    for parent_node_id in parent_node_ids:
        _require_reference_safe_identity_value(
            "lineage.parent_node_ids", parent_node_id
        )
    contribution_attribution = document.get("contribution_attribution")
    if not isinstance(contribution_attribution, dict):
        raise IdentityPersistenceError(
            "identity JSON field 'contribution_attribution' must be an object"
        )
    attribution_creator_node_id = contribution_attribution.get("creator_node_id")
    if attribution_creator_node_id != creator_node_id:
        raise IdentityPersistenceError(
            "identity JSON field 'contribution_attribution.creator_node_id' must match node.creator_node_id"
        )
    attribution_node_id = contribution_attribution.get("attribution_node_id")
    if attribution_node_id != node_id:
        raise IdentityPersistenceError(
            "identity JSON field 'contribution_attribution.attribution_node_id' must match node.node_id"
        )
    contribution_refs = _require_string_list(
        contribution_attribution, "contribution_refs"
    )
    _validate_identity_reference_values(
        manifest_refs=manifest_refs,
        validation_receipt_refs=validation_receipt_refs,
        lineage_links=lineage_links,
        contribution_refs=contribution_refs,
        identity_path=identity_path,
        node_id=node_id,
        creator_node_id=creator_node_id,
    )
    return NodeIdentity(node_id=node_id, node_name=node_name)


def _load_identity_document(path: Path) -> dict[str, object]:
    try:
        with path.open("r", encoding="utf-8") as handle:
            document: object = json.load(handle)
    except json.JSONDecodeError as exc:
        raise IdentityPersistenceError(
            f"identity JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise IdentityPersistenceError(f"could not read identity file: {exc}") from exc
    if not isinstance(document, dict):
        raise IdentityPersistenceError("identity JSON must be an object")
    return document


def _identity_document_creator_node_id(document: dict[str, object]) -> str:
    node = document.get("node")
    if not isinstance(node, dict):
        raise IdentityPersistenceError("identity JSON field 'node' must be an object")
    creator_node_id = node.get("creator_node_id")
    if not isinstance(creator_node_id, str) or not creator_node_id:
        raise IdentityPersistenceError(
            "identity JSON field 'node.creator_node_id' must be a non-empty string"
        )
    _require_reference_safe_identity_value("node.creator_node_id", creator_node_id)
    return creator_node_id


def _required_identity_document_id(document: dict[str, object], field_name: str) -> str:
    value = document.get(field_name.rsplit(".", 1)[-1])
    if not isinstance(value, str) or not value.strip():
        raise IdentityPersistenceError(
            f"identity JSON field '{field_name}' must be a non-empty string"
        )
    _require_reference_safe_identity_value(field_name, value)
    return value


def _require_string_list(document: dict[str, object], field_name: str) -> list[str]:
    value = document.get(field_name)
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise IdentityPersistenceError(
            f"identity JSON field '{field_name}' must be a list of non-empty strings"
        )
    return value


def _validate_identity_reference_values(
    *,
    manifest_refs: list[str],
    validation_receipt_refs: list[str],
    lineage_links: list[str],
    contribution_refs: list[str],
    identity_path: Path,
    node_id: str,
    creator_node_id: str,
) -> None:
    for ref in manifest_refs:
        _require_local_identity_ref(ref, "references.manifest_refs")
        artifact = _load_local_identity_ref_document_if_exists(
            ref, identity_path=identity_path
        )
        if artifact is not None and not _manifest_document_matches_node(
            artifact, node_id
        ):
            raise IdentityPersistenceError(
                "identity JSON references.manifest_refs does not match node.node_id"
            )
    for ref in validation_receipt_refs:
        _require_local_identity_ref(ref, "references.validation_receipt_refs")
        artifact = _load_local_identity_ref_document_if_exists(
            ref, identity_path=identity_path
        )
        if artifact is not None and not _receipt_document_matches_node(
            artifact, node_id
        ):
            raise IdentityPersistenceError(
                "identity JSON references.validation_receipt_refs does not match node.node_id"
            )
    for ref in lineage_links:
        _require_local_identity_ref(ref, "lineage.lineage_links")
        artifact = _load_local_identity_ref_document_if_exists(
            ref, identity_path=identity_path
        )
        if (
            artifact is not None
            and _identity_artifact_has_identity_metadata(artifact)
            and not _identity_artifact_mentions_node(artifact, node_id)
        ):
            raise IdentityPersistenceError(
                "identity JSON lineage.lineage_links does not match node.node_id"
            )
    for ref in contribution_refs:
        _require_local_identity_ref(ref, "contribution_attribution.contribution_refs")
        artifact = _load_local_identity_ref_document_if_exists(
            ref, identity_path=identity_path
        )
        if (
            artifact is not None
            and _identity_artifact_has_identity_metadata(artifact)
            and not (
                _identity_artifact_mentions_node(artifact, node_id)
                and _identity_artifact_mentions_node(artifact, creator_node_id)
            )
        ):
            raise IdentityPersistenceError(
                "identity JSON contribution_attribution.contribution_refs does not match attribution metadata"
            )


def _require_local_identity_ref(ref: str, field_name: str) -> None:
    try:
        _require_local_manifest_ref(ref)
    except IdentityPersistenceError as exc:
        raise IdentityPersistenceError(
            f"identity JSON field '{field_name}' contains malformed local reference"
        ) from exc


def _load_local_identity_ref_document_if_exists(
    ref: str, *, identity_path: Path
) -> object | None:
    path = _local_identity_ref_path(ref, identity_path=identity_path)
    if path is None or not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            document: object = json.load(handle)
            return document
    except json.JSONDecodeError as exc:
        raise IdentityPersistenceError(
            f"identity JSON referenced local artifact is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise IdentityPersistenceError(
            f"could not read identity referenced local artifact: {exc}"
        ) from exc


def _manifest_document_matches_node(document: object, node_id: str) -> bool:
    if not isinstance(document, dict):
        return False
    nodes = document.get("nodes")
    if isinstance(nodes, list):
        for node in nodes:
            if node == node_id:
                return True
            if isinstance(node, dict) and node.get("node_id") == node_id:
                return True
        return False
    node = document.get("node")
    if isinstance(node, dict):
        return node.get("node_id") == node_id
    return True


def _receipt_document_matches_node(document: object, node_id: str) -> bool:
    if not isinstance(document, dict):
        return False
    receipts = document.get("receipts")
    if isinstance(receipts, list):
        return any(
            isinstance(receipt, dict) and receipt.get("node_id") == node_id
            for receipt in receipts
        )
    if _identity_artifact_has_identity_metadata(document):
        return _identity_artifact_mentions_node(document, node_id)
    return True


def _identity_artifact_has_identity_metadata(value: object) -> bool:
    if isinstance(value, dict):
        if any(
            key in value
            for key in (
                "node_id",
                "creator_node_id",
                "attribution_node_id",
                "parent_node_ids",
            )
        ):
            return True
        return any(
            _identity_artifact_has_identity_metadata(nested_value)
            for nested_value in value.values()
        )
    if isinstance(value, list):
        return any(_identity_artifact_has_identity_metadata(item) for item in value)
    return False


def _identity_artifact_mentions_node(value: object, node_id: str) -> bool:
    if isinstance(value, dict):
        return any(
            nested_value == node_id
            if isinstance(nested_value, str)
            else _identity_artifact_mentions_node(nested_value, node_id)
            for nested_value in value.values()
        )
    if isinstance(value, list):
        return any(_identity_artifact_mentions_node(item, node_id) for item in value)
    return value == node_id


def _require_provenance_string(document: dict[str, object], field_name: str) -> None:
    value = document.get(field_name)
    if not isinstance(value, str) or not value:
        raise IdentityPersistenceError(
            f"identity JSON field 'provenance.{field_name}' must be a non-empty string"
        )


def _identity_reset_warning() -> str:
    return (
        "WARNING: resetting the local node identity may affect lineage and "
        "contribution attribution continuity; the previous identity was quarantined. "
        "This is a local-only prototype reset and does not claim network consensus."
    )


def _timestamp_slug(timestamp: str) -> str:
    return re.sub(r"[^0-9A-Za-z]+", "-", timestamp).strip("-")


def _unique_reset_artifact_path(root: Path, stem: str, suffix: str) -> Path:
    candidate = root / f"{stem}{suffix}"
    index = 1
    while candidate.exists():
        candidate = root / f"{stem}-{index}{suffix}"
        index += 1
    return candidate


def _identity_reset_artifact_ref(path: Path) -> str:
    """Return a local audit reference without leaking host-specific directories."""

    return path.name


def _backup_identity_referenced_artifacts(
    document: dict[str, object], *, identity_path: Path, backup_root: Path
) -> list[Path]:
    copied: list[Path] = []
    refs_by_section = (
        (
            "manifest_refs",
            _string_list_from_section(document, "references", "manifest_refs"),
        ),
        (
            "validation_receipt_refs",
            _string_list_from_section(
                document, "references", "validation_receipt_refs"
            ),
        ),
        (
            "lineage_links",
            _string_list_from_section(document, "lineage", "lineage_links"),
        ),
        (
            "contribution_refs",
            _string_list_from_section(
                document,
                "contribution_attribution",
                "contribution_refs",
            ),
        ),
    )
    for section, refs in refs_by_section:
        target_dir = backup_root / section
        for ref in refs:
            source = _local_identity_ref_path(ref, identity_path=identity_path)
            if source is None or not source.is_file():
                continue
            target_dir.mkdir(parents=True, exist_ok=True)
            destination = _unique_reset_artifact_path(
                target_dir, source.stem, source.suffix
            )
            try:
                shutil.copy2(source, destination)
            except OSError as exc:
                raise IdentityPersistenceError(
                    f"could not quarantine referenced identity artifact: {exc}"
                ) from exc
            copied.append(destination)
    return copied


def _string_list_from_section(
    document: dict[str, object], section_name: str, field_name: str
) -> list[str]:
    section = document.get(section_name)
    if not isinstance(section, dict):
        return []
    values = section.get(field_name)
    if not isinstance(values, list):
        return []
    return [value for value in values if isinstance(value, str) and value]


def _local_identity_ref_path(ref: str, *, identity_path: Path) -> Path | None:
    local_ref = ref.split("#", 1)[0]
    if not local_ref or "://" in local_ref:
        return None
    path = Path(local_ref)
    if path.is_absolute() or ".." in path.parts:
        return None
    return identity_path.parent / path


def _append_identity_reset_receipt(
    path: Path,
    *,
    timestamp: str,
    identity_path: Path,
    backup_path: Path,
    previous_node_id: str,
    new_node_id: str,
    previous_creator_node_id: str,
    new_creator_node_id: str,
    rotate_creator_identity: bool,
    files_backed_up: list[Path],
    reason: str | None,
) -> None:
    document = _load_identity_reset_receipts(path)
    reset_receipts = document["reset_receipts"]
    if not isinstance(reset_receipts, list):
        raise IdentityPersistenceError(
            "identity reset audit receipt reset_receipts must be a list"
        )
    receipt = {
        "event": "identity_reset",
        "timestamp": timestamp,
        "previous_node_id": previous_node_id,
        "new_node_id": new_node_id,
        "previous_creator_node_id": previous_creator_node_id,
        "new_creator_node_id": new_creator_node_id,
        "full_local_identity_rotation": rotate_creator_identity,
        "identity_path": _identity_reset_artifact_ref(identity_path),
        "quarantined_identity_path": _identity_reset_artifact_ref(backup_path),
        "files_backed_up": [
            _identity_reset_artifact_ref(file_path) for file_path in files_backed_up
        ],
        "backed_up_identity_sections": [
            "node",
            "references.manifest_refs",
            "references.validation_receipt_refs",
            "lineage",
            "contribution_attribution",
        ],
        "reason": reason or None,
        "warning": _identity_reset_warning(),
        "active_identity_binding": {
            "manifest_node_id": new_node_id,
            "validation_receipt_node_id": new_node_id,
            "lineage_node_id": new_node_id,
            "contribution_attribution_node_id": new_node_id,
            "creator_node_id": new_creator_node_id,
        },
    }
    reset_receipts.append(receipt)
    try:
        atomic_write_json(path, document)
    except OSError as exc:
        raise IdentityPersistenceError(
            f"could not write identity reset audit receipt: {exc}"
        ) from exc


def _load_identity_reset_receipts(path: Path) -> dict[str, object]:
    if not path.exists():
        return {
            "version": 1,
            "receipt_type": "identity_reset_audit",
            "reset_receipts": [],
        }
    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise IdentityPersistenceError(
            f"identity reset audit receipt JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise IdentityPersistenceError(
            f"could not read identity reset audit receipt: {exc}"
        ) from exc
    if not isinstance(document, dict):
        raise IdentityPersistenceError("identity reset audit receipt must be an object")
    if document.get("version") != 1:
        raise IdentityPersistenceError(
            "identity reset audit receipt must contain version 1"
        )
    if document.get("receipt_type") != "identity_reset_audit":
        raise IdentityPersistenceError(
            "identity reset audit receipt_type must be identity_reset_audit"
        )
    reset_receipts = document.get("reset_receipts")
    if not isinstance(reset_receipts, list):
        raise IdentityPersistenceError(
            "identity reset audit receipt reset_receipts must be a list"
        )
    return document


def _save_identity(
    path: Path,
    identity: NodeIdentity,
    *,
    overwrite_existing: bool = False,
    creator_node_id: str | None = None,
) -> None:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    document = _identity_document(identity, creator_node_id=creator_node_id)
    if not overwrite_existing:
        _create_identity_document_without_overwrite(path, document)
        return
    try:
        atomic_write_json(path, document)
    except OSError as exc:
        raise IdentityPersistenceError(f"could not write identity file: {exc}") from exc


def _create_identity_document_without_overwrite(
    path: Path, document: dict[str, object]
) -> None:
    """Create an identity JSON file atomically without replacing a winner."""

    try:
        atomic_create_json(path, document)
    except FileExistsError as exc:
        raise IdentityPersistenceError(
            "identity file already exists; use the explicit reset flow to replace it"
        ) from exc
    except (OSError, TypeError, ValueError) as exc:
        raise IdentityPersistenceError(f"could not write identity file: {exc}") from exc


def _identity_document(
    identity: NodeIdentity,
    *,
    created_at: str | None = None,
    creator_node_id: str | None = None,
) -> dict[str, object]:
    timestamp = created_at or datetime.now(UTC).replace(microsecond=0).isoformat()
    try:
        _require_identity_timestamp(timestamp)
        version_metadata = capture_version_metadata(captured_at=timestamp)
    except IdentityPersistenceError:
        version_metadata = capture_version_metadata()
    node: dict[str, object] = {"node_id": identity.node_id}
    if identity.node_name is not None:
        node["node_name"] = identity.node_name
    creator = creator_node_id or identity.node_id
    node["creator_node_id"] = creator
    node["created_at"] = timestamp
    return {
        "version": IDENTITY_SCHEMA_VERSION,
        "node": node,
        "provenance": {
            "created_by": IDENTITY_CREATED_BY,
            "source": IDENTITY_PROVENANCE_SOURCE,
            "creation_event": "identity_manifest_created",
            "load_behavior": "reuse_existing_identity_without_overwrite",
            "authority": IDENTITY_AUTHORITY,
        },
        "references": {
            "manifest_refs": [],
            "validation_receipt_refs": [],
            "version_metadata": version_metadata,
        },
        "lineage": {
            "parent_node_ids": [],
            "lineage_links": [],
        },
        "contribution_attribution": {
            "creator_node_id": creator,
            "attribution_node_id": identity.node_id,
            "contribution_refs": [],
        },
    }
