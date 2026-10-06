"""Validated Phase 1 local runtime configuration."""

from __future__ import annotations

import json
import re
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from itertools import combinations
from pathlib import Path

from aethermesh_core.json_io import atomic_create_json, atomic_write_json

LOCAL_RUNTIME_CONFIG_VERSION = 1
LOCAL_RUNTIME_CONFIG_PATH = "runtime-config.json"
CREATOR_NODE_ID_STABILITY = (
    "stable local identity; do not regenerate during normal local runs"
)
DEFAULT_RUNTIME_PATHS = {
    "identity": "identity/creator-node.json",
    "manifest": "manifests/local-node-manifest.json",
    "validation_receipts": "receipts",
    "lineage": "lineage",
    "contribution_attribution": "contributions",
    "log": "logs/startup.log",
    "work_inputs": "work/inputs",
    "work_outputs": "work/outputs",
}
_ALLOWED_TOP_LEVEL_FIELDS = frozenset({"version", "node", "paths", "network_mode"})
_ALLOWED_NODE_FIELDS = frozenset(
    {"node_id", "creator_node_id", "creator_node_id_stability"}
)


class LocalRuntimeConfigError(ValueError):
    """Raised when local runtime config is missing required safe fields."""


@dataclass(frozen=True)
class LocalRuntimeConfig:
    """One explicit local-only runtime config source for startup artifacts."""

    node_id: str
    creator_node_id: str
    paths: dict[str, str]

    def to_document(self) -> dict[str, object]:
        return {
            "version": LOCAL_RUNTIME_CONFIG_VERSION,
            "node": {
                "node_id": self.node_id,
                "creator_node_id": self.creator_node_id,
                "creator_node_id_stability": CREATOR_NODE_ID_STABILITY,
            },
            "paths": dict(self.paths),
            "network_mode": "local-only-no-p2p",
        }

    def resolve_path(self, runtime_root: Path, path_key: str) -> Path:
        return runtime_root / self.paths[path_key]


def default_local_runtime_config(
    node_id: str, creator_node_id: str
) -> LocalRuntimeConfig:
    """Build the minimal Phase 1 local-only config defaults."""

    _require_node_id(node_id, "node.node_id")
    _require_node_id(creator_node_id, "node.creator_node_id")
    return LocalRuntimeConfig(
        node_id=node_id,
        creator_node_id=creator_node_id,
        paths=dict(DEFAULT_RUNTIME_PATHS),
    )


def load_local_runtime_config(path: Path) -> LocalRuntimeConfig:
    """Load and validate one local runtime config file."""

    try:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
    except json.JSONDecodeError as exc:
        raise LocalRuntimeConfigError(
            f"local runtime config JSON is malformed: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise LocalRuntimeConfigError(
            f"could not read local runtime config: {exc}"
        ) from exc
    return parse_local_runtime_config(document)


def parse_local_runtime_config(document: object) -> LocalRuntimeConfig:
    """Validate a local runtime config document and return its config object."""

    if not isinstance(document, dict):
        raise LocalRuntimeConfigError("local runtime config must be a JSON object")
    _reject_unknown_fields(
        document.keys(), _ALLOWED_TOP_LEVEL_FIELDS, "local runtime config"
    )
    if (
        type(document.get("version")) is not int
        or document["version"] != LOCAL_RUNTIME_CONFIG_VERSION
    ):
        raise LocalRuntimeConfigError("local runtime config must contain version 1")
    if document.get("network_mode") != "local-only-no-p2p":
        raise LocalRuntimeConfigError(
            "local runtime config network_mode must be local-only-no-p2p"
        )
    node = document.get("node")
    if not isinstance(node, dict):
        raise LocalRuntimeConfigError("local runtime config node must be an object")
    _reject_unknown_fields(
        node.keys(), _ALLOWED_NODE_FIELDS, "local runtime config node"
    )
    node_id = _require_node_id(node.get("node_id"), "node.node_id")
    creator_node_id = _require_node_id(
        node.get("creator_node_id"), "node.creator_node_id"
    )
    if node.get("creator_node_id_stability") != CREATOR_NODE_ID_STABILITY:
        raise LocalRuntimeConfigError(
            "local runtime config node.creator_node_id_stability must preserve local identity"
        )
    paths = document.get("paths")
    if not isinstance(paths, dict):
        raise LocalRuntimeConfigError("local runtime config paths must be an object")
    _reject_unknown_fields(
        paths.keys(), DEFAULT_RUNTIME_PATHS.keys(), "local runtime config paths"
    )
    resolved_paths: dict[str, str] = {}
    for key in DEFAULT_RUNTIME_PATHS:
        resolved_paths[key] = _require_local_ref(paths.get(key), f"paths.{key}")
    _validate_safe_runtime_layout(resolved_paths)
    return LocalRuntimeConfig(
        node_id=node_id,
        creator_node_id=creator_node_id,
        paths=resolved_paths,
    )


def load_or_create_local_runtime_config(
    path: Path, *, node_id: str, creator_node_id: str
) -> LocalRuntimeConfig:
    """Load existing config or create the default config for a new local runtime."""

    if path.exists():
        config = load_local_runtime_config(path)
        if config.node_id != node_id:
            raise LocalRuntimeConfigError(
                "local runtime config node.node_id does not match identity"
            )
        if config.creator_node_id != creator_node_id:
            raise LocalRuntimeConfigError(
                "local runtime config node.creator_node_id does not match identity"
            )
        return config
    config = default_local_runtime_config(node_id, creator_node_id)
    try:
        atomic_create_json(path, config.to_document())
    except OSError as exc:
        raise LocalRuntimeConfigError(
            f"could not create local runtime config: {exc}"
        ) from exc
    return config


def write_local_runtime_config(path: Path, config: LocalRuntimeConfig) -> None:
    """Persist a known-good config, replacing any previous config."""

    try:
        atomic_write_json(path, config.to_document())
    except OSError as exc:
        raise LocalRuntimeConfigError(
            f"could not write local runtime config: {exc}"
        ) from exc


def load_optional_local_runtime_config(
    root: Path, error_type: Callable[[str], Exception]
) -> LocalRuntimeConfig | None:
    """Load ``root`` config when present, translating validation errors."""

    config_path = root / LOCAL_RUNTIME_CONFIG_PATH
    if not config_path.exists():
        return None
    try:
        return load_local_runtime_config(config_path)
    except LocalRuntimeConfigError as exc:
        raise error_type(str(exc)) from exc


def configured_runtime_ref(config: LocalRuntimeConfig | None, path_key: str) -> str:
    """Return configured local path reference or the Phase 1 default."""

    if config is None:
        return DEFAULT_RUNTIME_PATHS[path_key]
    return config.paths[path_key]


def configured_runtime_path(
    root: Path, config: LocalRuntimeConfig | None, path_key: str
) -> Path:
    """Resolve a configured local runtime path under ``root``."""

    return root / configured_runtime_ref(config, path_key)


def validate_runtime_path_boundaries(
    root: Path, config: LocalRuntimeConfig | None
) -> None:
    """Reject configured paths that escape ``root`` through filesystem links."""

    resolved_root = root.resolve()
    paths = DEFAULT_RUNTIME_PATHS if config is None else config.paths
    resolved_paths: dict[str, str] = {}
    for key, relative_ref in paths.items():
        resolved_path = (root / relative_ref).resolve()
        if not resolved_path.is_relative_to(resolved_root):
            raise LocalRuntimeConfigError(
                f"local runtime config paths.{key} must stay within the runtime directory"
            )
        resolved_paths[key] = resolved_path.relative_to(resolved_root).as_posix()
    resolved_config_path = (root / LOCAL_RUNTIME_CONFIG_PATH).resolve()
    if not resolved_config_path.is_relative_to(resolved_root):
        raise LocalRuntimeConfigError(
            "local runtime config must stay within the runtime directory"
        )
    _validate_safe_runtime_layout(
        resolved_paths,
        runtime_config_ref=resolved_config_path.relative_to(resolved_root).as_posix(),
    )


def _require_node_id(value: object, label: str) -> str:
    if not isinstance(value, str) or not value:
        raise LocalRuntimeConfigError(
            f"local runtime config {label} must be a non-empty string"
        )
    return value


def _require_local_ref(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise LocalRuntimeConfigError(
            f"local runtime config {label} must be a non-empty local path"
        )
    if value != value.strip() or "://" in value or value.startswith("~"):
        raise LocalRuntimeConfigError(
            f"local runtime config {label} must be a relative local path"
        )
    if re.match(r"^[A-Za-z]:[\\/]", value) is not None:
        raise LocalRuntimeConfigError(
            f"local runtime config {label} must be a relative local path"
        )
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise LocalRuntimeConfigError(
            f"local runtime config {label} must be a relative local path"
        )
    return path.as_posix()


def _validate_safe_runtime_layout(
    paths: dict[str, str], *, runtime_config_ref: str = LOCAL_RUNTIME_CONFIG_PATH
) -> None:
    """Reject artifact layouts that could mix or overwrite preserved records."""

    file_refs = {
        **{f"paths.{key}": Path(paths[key]) for key in ("identity", "manifest", "log")},
        "runtime config": Path(runtime_config_ref),
    }
    for (key, file_ref), (other_key, other_file_ref) in combinations(
        file_refs.items(), 2
    ):
        if _paths_overlap(file_ref, other_file_ref):
            raise LocalRuntimeConfigError(
                "local runtime config file paths must be separate: "
                f"{key} and {other_key}"
            )

    artifact_dirs = {
        key: Path(paths[key])
        for key in (
            "validation_receipts",
            "lineage",
            "contribution_attribution",
            "work_inputs",
            "work_outputs",
        )
    }
    for key, directory in artifact_dirs.items():
        for file_key, file_ref in file_refs.items():
            if _paths_overlap(directory, file_ref):
                raise LocalRuntimeConfigError(
                    f"local runtime config paths.{key} must not overlap {file_key}"
                )
    for (key, directory), (other_key, other_directory) in combinations(
        artifact_dirs.items(), 2
    ):
        if _paths_overlap(directory, other_directory):
            raise LocalRuntimeConfigError(
                "local runtime config artifact directories must be separate: "
                f"paths.{key} and paths.{other_key}"
            )


def _paths_overlap(left: Path, right: Path) -> bool:
    return left == right or left in right.parents or right in left.parents


def _reject_unknown_fields(
    field_names: Iterable[str], allowed_fields: Iterable[str], label: str
) -> None:
    field_set = set(field_names)
    unknown = sorted(field_set - set(allowed_fields))
    if unknown:
        raise LocalRuntimeConfigError(
            f"{label} contains unsupported fields: {', '.join(unknown)}"
        )


__all__ = [
    "DEFAULT_RUNTIME_PATHS",
    "LOCAL_RUNTIME_CONFIG_PATH",
    "LOCAL_RUNTIME_CONFIG_VERSION",
    "LocalRuntimeConfig",
    "LocalRuntimeConfigError",
    "configured_runtime_path",
    "configured_runtime_ref",
    "default_local_runtime_config",
    "load_local_runtime_config",
    "load_optional_local_runtime_config",
    "load_or_create_local_runtime_config",
    "parse_local_runtime_config",
    "validate_runtime_path_boundaries",
    "write_local_runtime_config",
]
