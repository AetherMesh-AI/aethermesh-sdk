"""Strict, privacy-preserving bridges from saved local identity to peer metadata."""

import json
import os
import subprocess
import sys
import tempfile
import unittest
from dataclasses import FrozenInstanceError, replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core import identity as identity_module
from aethermesh_core.identity import (
    HardwareIdentityInputs,
    IdentityPersistenceError,
    _save_identity,
    load_or_create_identity,
    read_identity,
)
from aethermesh_core.models import NodeIdentity
from aethermesh_core.network.errors import ProtocolError
from aethermesh_core.network.profile import (
    HardwareProfile,
    NodeProfile,
    PeerInfo,
    hardware_profile,
    load_node_profile,
    validate_node_id,
)


def _hardware(**overrides: Any) -> HardwareIdentityInputs:
    fields: dict[str, Any] = {
        "cpu_architecture": "AMD64",
        "cpu_vendor": "private-cpu-vendor",
        "cpu_brand_or_chip_name": "private-cpu-chip-serial",
        "physical_core_count": 23,
        "logical_thread_count": 46,
        "permanent_mac_addresses": ("a2:bb:cc:dd:ee:ff",),
        "gpu_vendor": "private-gpu-vendor",
        "gpu_model_or_chip_name": "private-gpu-chip-serial",
        "gpu_device_id_if_available": "private-device-id",
        "gpu_count": 1,
        "max_gpu_vram_gb": 24,
        "total_installed_ram_gb": 64,
    }
    return HardwareIdentityInputs(**(fields | overrides))


def _unknown_hardware(**overrides: Any) -> HardwareIdentityInputs:
    return _hardware(
        **(
            {
                "cpu_architecture": "",
                "gpu_vendor": "",
                "gpu_model_or_chip_name": "",
                "gpu_device_id_if_available": "",
                "gpu_count": 0,
                "max_gpu_vram_gb": 0,
                "total_installed_ram_gb": 0,
            }
            | overrides
        )
    )


class HardwareProfileTests(unittest.TestCase):
    def test_default_unknown_values_and_frozen_shape(self) -> None:
        profile = HardwareProfile()
        self.assertEqual(
            profile.to_dict(),
            {
                "cpu_architecture": "unknown",
                "ram_gb_bucket": "unknown",
                "gpu_available": None,
                "gpu_vram_gb_bucket": "unknown",
            },
        )
        with self.assertRaises(FrozenInstanceError):
            profile.cpu_architecture = "x86"
        changed = profile.to_dict()
        changed["gpu_available"] = True
        self.assertIsNone(profile.gpu_available)

    def test_all_canonical_values_are_accepted(self) -> None:
        for value in ("x86_64", "aarch64", "arm", "x86", "other", "unknown"):
            self.assertEqual(
                HardwareProfile(cpu_architecture=value).cpu_architecture, value
            )
        for value in ("unknown", "under16", "16to31", "32to63", "64to127", "128plus"):
            self.assertEqual(HardwareProfile(ram_gb_bucket=value).ram_gb_bucket, value)
        for value in ("unknown", "under8", "8to15", "16to31", "32plus"):
            self.assertEqual(
                HardwareProfile(gpu_vram_gb_bucket=value).gpu_vram_gb_bucket, value
            )
        for value in (True, False, None):
            self.assertIs(HardwareProfile(gpu_available=value).gpu_available, value)

    def test_invalid_categories_and_exact_types_rejected(self) -> None:
        class StringSubclass(str):
            pass

        for field in ("cpu_architecture", "ram_gb_bucket", "gpu_vram_gb_bucket"):
            for value in (
                None,
                True,
                1,
                [],
                {},
                "",
                "UNKNOWN",
                "private-secret",
                StringSubclass("unknown"),
            ):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    HardwareProfile(**{field: value})
        for value in (0, 1, "true", "false", [], {}):
            with self.subTest(value=value), self.assertRaises(ValueError):
                HardwareProfile(gpu_available=value)

    def test_projection_exposes_only_coarse_fields_without_logs(self) -> None:
        with (
            self.assertNoLogs(identity_module.LOGGER),
            patch.object(
                identity_module,
                "collect_hardware_identity_inputs",
                side_effect=AssertionError("unexpected probe"),
            ),
        ):
            profile = hardware_profile(_hardware())
        self.assertEqual(profile, HardwareProfile("x86_64", "64to127", True, "16to31"))
        serialized = json.dumps(profile.to_dict(), sort_keys=True)
        for private in (
            "private-",
            "a2:bb:cc:dd:ee:ff",
            "physical_core",
            "logical_thread",
            "gpu_count",
            "23",
            "46",
        ):
            self.assertNotIn(private, serialized)
        self.assertEqual(
            profile,
            hardware_profile(
                _hardware(
                    cpu_vendor="changed-vendor",
                    cpu_brand_or_chip_name="changed-cpu",
                    permanent_mac_addresses=("00:11:22:33:44:55",),
                    physical_core_count=96,
                    logical_thread_count=192,
                    gpu_vendor="changed-vendor",
                    gpu_model_or_chip_name="changed-gpu",
                    gpu_device_id_if_available="changed-id",
                    gpu_count=8,
                )
            ),
        )

    def test_architecture_normalization_is_bounded_and_does_not_leak(self) -> None:
        for expected, aliases in (
            ("x86_64", ("AMD64", "x86_64", " X64 ")),
            ("aarch64", ("arm64", " AARCH64 ")),
            ("x86", ("x86", "i386", "i486", "i586", "i686", "ia32")),
            ("arm", ("arm", "armv5", "armv6l", "armv7l", "armv8l")),
            ("unknown", ("", " ", "unknown", "UNKNOWN_CPU_ARCHITECTURE")),
            (
                "other",
                (
                    "s390x",
                    "riscv64",
                    "private-architecture-model",
                    "arm64-private-model",
                ),
            ),
        ):
            for value in aliases:
                with self.subTest(value=value):
                    self.assertEqual(
                        hardware_profile(
                            _hardware(cpu_architecture=value)
                        ).cpu_architecture,
                        expected,
                    )

    def test_ram_bucket_boundaries(self) -> None:
        for value, expected in (
            (0.1, "under16"),
            (15.99, "under16"),
            (16, "16to31"),
            (31.99, "16to31"),
            (32, "32to63"),
            (63.99, "32to63"),
            (64, "64to127"),
            (127.99, "64to127"),
            (128, "128plus"),
            (1024, "128plus"),
            (" 32 ", "32to63"),
        ):
            with self.subTest(value=value):
                self.assertEqual(
                    hardware_profile(
                        _hardware(total_installed_ram_gb=value)
                    ).ram_gb_bucket,
                    expected,
                )

    def test_vram_bucket_boundaries(self) -> None:
        for value, expected in (
            (0.1, "under8"),
            (7.99, "under8"),
            (8, "8to15"),
            (15.99, "8to15"),
            (16, "16to31"),
            (31.99, "16to31"),
            (32, "32plus"),
            (1024, "32plus"),
            (" 8 ", "8to15"),
        ):
            with self.subTest(value=value):
                profile = hardware_profile(_unknown_hardware(max_gpu_vram_gb=value))
                self.assertEqual(profile.gpu_vram_gb_bucket, expected)
                self.assertIs(profile.gpu_available, True)

    def test_unavailable_invalid_nonfinite_and_zero_memory_are_unknown(self) -> None:
        for value in (
            "",
            " ",
            "UNKNOWN_RAM_GB",
            "invalid",
            "NaN",
            "inf",
            "-inf",
            0,
            -1,
            float("nan"),
            float("inf"),
            None,
            True,
            False,
            [],
            {},
            10**400,
        ):
            with self.subTest(value=value):
                profile = hardware_profile(
                    _unknown_hardware(
                        total_installed_ram_gb=value,
                        max_gpu_vram_gb=value,
                        gpu_count=value,
                    )
                )
                self.assertEqual(profile.ram_gb_bucket, "unknown")
                self.assertEqual(profile.gpu_vram_gb_bucket, "unknown")
                self.assertIsNone(profile.gpu_available)

    def test_gpu_probe_failure_or_placeholder_is_not_reported_as_absence(self) -> None:
        for placeholder in (
            "",
            " ",
            "UNKNOWN",
            "UNKNOWN_GPU_VENDOR",
            "UNKNOWN_GPU_MODEL",
            "UNKNOWN_GPU_DEVICE_ID",
            "NO_GPU_DETECTED",
            "NONE",
            "N/A",
        ):
            with self.subTest(placeholder=placeholder):
                profile = hardware_profile(
                    _unknown_hardware(
                        gpu_vendor=placeholder,
                        gpu_model_or_chip_name=placeholder,
                        gpu_device_id_if_available=placeholder,
                    )
                )
                self.assertIsNone(profile.gpu_available)
                self.assertEqual(profile.gpu_vram_gb_bucket, "unknown")
        self.assertEqual(hardware_profile(_unknown_hardware()), HardwareProfile())

    def test_each_positive_gpu_evidence_source_independently_marks_present(
        self,
    ) -> None:
        for field, value in (
            ("gpu_vendor", "vendor"),
            ("gpu_model_or_chip_name", "chip"),
            ("gpu_device_id_if_available", "device-id"),
            ("max_gpu_vram_gb", 8),
        ):
            with self.subTest(field=field, value=value):
                self.assertIs(
                    hardware_profile(_unknown_hardware(**{field: value})).gpu_available,
                    True,
                )

    def test_count_alone_and_failed_windows_probe_remain_unknown(self) -> None:
        for count in (1, "1", 2, "4"):
            self.assertIsNone(
                hardware_profile(_unknown_hardware(gpu_count=count)).gpu_available
            )
        failed = identity_module.collect_hardware_identity_inputs(
            goos="windows", read_file=lambda path: "", run_command=lambda *args: ""
        )
        self.assertEqual(failed.gpu_count, 1)
        self.assertIsNone(hardware_profile(failed).gpu_available)


class NodeProfileTests(unittest.TestCase):
    def test_identity_conversion_preserves_legacy_ids_and_missing_names(self) -> None:
        for node_name in (None, "Legacy.Node_Name:1-2"):
            identity = NodeIdentity("legacy:Node.ID_1-2", node_name)
            profile = NodeProfile.from_identity(identity)
            self.assertEqual(
                profile.to_dict(),
                {"node_id": identity.node_id, "node_name": node_name, "hardware": None},
            )
            self.assertEqual(NodeProfile.from_dict(profile.to_dict()), profile)
            self.assertIsNone(profile.hardware)
        hardware = HardwareProfile("aarch64", "32to63", False, "unknown")
        profile = NodeProfile.from_identity(identity, hardware=hardware)
        self.assertEqual(NodeProfile.from_dict(profile.to_dict()), profile)
        self.assertIs(profile.hardware, hardware)
        with self.assertRaises(FrozenInstanceError):
            profile.node_id = "changed"

    def test_reference_safe_ascii_and_exact_length_limits(self) -> None:
        class StringSubclass(str):
            pass

        for value in ("a", "A0._:-", "a" * 128):
            validate_node_id(value)
            self.assertEqual(NodeProfile(value, value).node_id, value)
        for value in (
            None,
            1,
            True,
            [],
            {},
            "",
            "a" * 129,
            "line\nbreak",
            "\x00",
            "a b",
            "a/b",
            "a\\b",
            "汉字",
            "é",
            "\ud800",
            StringSubclass("valid"),
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    validate_node_id(value)
                with self.assertRaises(ValueError):
                    NodeProfile(value)
                if value is not None:
                    with self.assertRaises(ValueError):
                        NodeProfile("valid", value)

    def test_hardware_type_is_strict(self) -> None:
        for value in ({}, HardwareProfile().to_dict(), True, "unknown"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                NodeProfile("valid", hardware=value)

    def test_serialization_returns_fresh_root_and_nested_dicts(self) -> None:
        profile = NodeProfile("node:1", "name:1", HardwareProfile())
        document = profile.to_dict()
        document["node_id"] = "changed"
        document["hardware"]["cpu_architecture"] = "x86"
        self.assertEqual(profile.node_id, "node:1")
        self.assertEqual(profile.hardware, HardwareProfile())

    def test_wire_parser_rejects_missing_extra_and_nonobject_fields(self) -> None:
        class DictSubclass(dict):
            pass

        base = NodeProfile("node:1", "name:1", HardwareProfile()).to_dict()
        cases = [
            None,
            True,
            1,
            "secret",
            [],
            DictSubclass(base),
            base | {"private_secret": "secret"},
        ]
        for field in base:
            cases.append({key: value for key, value in base.items() if key != field})
        for hardware in (
            False,
            1,
            [],
            "secret",
            DictSubclass(base["hardware"]),
            base["hardware"] | {"serial": "private-secret"},
        ):
            cases.append(base | {"hardware": hardware})
        for field in base["hardware"]:
            cases.append(
                base
                | {
                    "hardware": {
                        key: value
                        for key, value in base["hardware"].items()
                        if key != field
                    }
                }
            )
        for case in cases:
            with (
                self.subTest(case=case),
                self.assertRaisesRegex(ProtocolError, "^Invalid node profile$"),
            ):
                NodeProfile.from_dict(case)

    def test_wire_parser_rejects_nested_values_and_sanitizes_failures(self) -> None:
        base = NodeProfile("valid", "valid", HardwareProfile()).to_dict()
        for field in ("node_id", "node_name"):
            for value in (
                "private/secret",
                "x" * 129,
                ["secret"],
                {"secret": "value"},
                True,
            ):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaisesRegex(ProtocolError, "^Invalid node profile$"),
                ):
                    NodeProfile.from_dict(base | {field: value})
        for field in base["hardware"]:
            for value in ("private/secret", ["secret"], {"secret": "value"}, 1):
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaisesRegex(ProtocolError, "^Invalid node profile$"),
                ):
                    NodeProfile.from_dict(
                        base | {"hardware": base["hardware"] | {field: value}}
                    )

    def test_import_and_construction_do_not_probe_or_create_files(self) -> None:
        script = """
from pathlib import Path
from unittest.mock import patch
from aethermesh_core import identity
with patch.object(identity, 'collect_hardware_identity_inputs', side_effect=AssertionError('probe')), patch.object(identity, '_resolved_hardware_inputs', side_effect=AssertionError('probe')), patch.object(Path, 'mkdir', side_effect=AssertionError('mkdir')), patch.object(Path, 'write_text', side_effect=AssertionError('write')), patch.object(Path, 'write_bytes', side_effect=AssertionError('write')):
    from aethermesh_core.network.profile import HardwareProfile, NodeProfile, PeerInfo
    profile = NodeProfile('local-id', hardware=HardwareProfile())
    PeerInfo('a' * 64, 2, profile)
"""
        result = subprocess.run(
            [sys.executable, "-c", script],
            capture_output=True,
            check=False,
            timeout=10,
            env=os.environ
            | {"PYTHONPATH": str(Path(__file__).resolve().parents[1] / "src")},
        )
        self.assertEqual(result.returncode, 0, result.stderr.decode())
        self.assertEqual(result.stdout, b"")
        self.assertEqual(result.stderr, b"")


class PeerInfoTests(unittest.TestCase):
    def test_peer_information_is_frozen_and_retains_separate_trust_inputs(self) -> None:
        profile = NodeProfile(
            "self-reported-id", hardware=HardwareProfile(gpu_available=True)
        )
        peer = PeerInfo("a" * 64, 2, profile, True)
        self.assertEqual(peer.fingerprint, "a" * 64)
        self.assertEqual(peer.protocol_version, 2)
        self.assertIs(peer.profile, profile)
        self.assertTrue(peer.identity_pinned)
        self.assertEqual(PeerInfo("b" * 64, 1, None).profile, None)
        self.assertFalse(PeerInfo("b" * 64, 2, profile).identity_pinned)
        with self.assertRaises(FrozenInstanceError):
            peer.profile = None

    def test_peer_information_rejects_invalid_types_versions_and_pins(self) -> None:
        valid = PeerInfo("a" * 64, 2, NodeProfile("valid"))
        for field, values in (
            ("fingerprint", ("", "A" * 64, "a" * 63, None, True, {})),
            ("protocol_version", (0, -1, 3, True, 1.0, "2", None)),
            ("profile", ({}, "node", True)),
            ("identity_pinned", (0, 1, "true", None)),
        ):
            for value in values:
                with (
                    self.subTest(field=field, value=value),
                    self.assertRaises(ValueError),
                ):
                    replace(valid, **{field: value})
        with self.assertRaises(ValueError):
            PeerInfo("a" * 64, 2, None, True)
        with self.assertRaises(ValueError):
            PeerInfo("a" * 64, 1, NodeProfile("valid"))


class LoadNodeProfileTests(unittest.TestCase):
    def test_missing_default_identity_fails_without_probes_or_directory_creation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "missing" / "identity.json"
            with patch.object(
                identity_module, "collect_hardware_identity_inputs"
            ) as collect:
                with self.assertRaises(IdentityPersistenceError):
                    load_node_profile(path)
                with self.assertRaises(IdentityPersistenceError):
                    read_identity(str(path))
            collect.assert_not_called()
            self.assertFalse(path.parent.exists())

    def test_read_only_saved_identity_reuse_preserves_bytes_and_none_name(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity.json"
            for name in (None, "Legacy.Node_Name:2"):
                _save_identity(
                    path, NodeIdentity("Legacy.Node:1", name), overwrite_existing=True
                )
                content = path.read_bytes()
                with patch.object(
                    identity_module,
                    "collect_hardware_identity_inputs",
                    side_effect=AssertionError("probe"),
                ):
                    self.assertEqual(
                        read_identity(path), NodeIdentity("Legacy.Node:1", name)
                    )
                    for create in (False, True):
                        self.assertEqual(
                            load_node_profile(path, create=create),
                            NodeProfile("Legacy.Node:1", name),
                        )
                self.assertEqual(path.read_bytes(), content)

    def test_explicit_creation_uses_existing_identity_generator_and_persistence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "explicit" / "identity.json"
            with (
                patch.object(
                    identity_module,
                    "collect_hardware_identity_inputs",
                    return_value=_hardware(),
                ) as collect,
                patch.object(
                    identity_module,
                    "load_or_create_identity",
                    wraps=load_or_create_identity,
                ) as create,
            ):
                profile = load_node_profile(path, create=True)
            create.assert_called_once_with(path)
            self.assertEqual(collect.call_count, 2)
            self.assertEqual(profile, NodeProfile.from_identity(read_identity(path)))
            self.assertIsNone(profile.hardware)
            content = path.read_bytes()
            with patch.object(
                identity_module,
                "collect_hardware_identity_inputs",
                return_value=_hardware(total_installed_ram_gb=128),
            ) as collect:
                self.assertEqual(load_node_profile(path, create=True), profile)
            collect.assert_not_called()
            self.assertEqual(path.read_bytes(), content)

    def test_hardware_collection_requires_opt_in_and_never_changes_saved_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity.json"
            identity = load_or_create_identity(path, hardware_inputs=_hardware())
            content = path.read_bytes()
            with (
                patch.object(identity_module, "_default_goos", return_value="linux"),
                patch.object(
                    identity_module,
                    "collect_hardware_identity_inputs",
                    return_value=_hardware(),
                ) as collect,
            ):
                profile = load_node_profile(path, include_hardware=True)
            collect.assert_called_once_with(
                goos="linux",
                read_file=identity_module._read_text_file,
                run_command=identity_module._run_command,
            )
            self.assertEqual(
                profile,
                NodeProfile.from_identity(
                    identity, hardware=hardware_profile(_hardware())
                ),
            )
            with patch.object(
                identity_module,
                "collect_hardware_identity_inputs",
                return_value=_unknown_hardware(total_installed_ram_gb=128),
            ):
                changed = load_node_profile(path, create=True, include_hardware=True)
            self.assertEqual(changed.node_id, identity.node_id)
            self.assertEqual(changed.node_name, identity.node_name)
            self.assertEqual(
                changed.hardware, HardwareProfile("unknown", "128plus", None, "unknown")
            )
            self.assertEqual(path.read_bytes(), content)

    def test_invalid_saved_identity_is_never_overwritten_or_hardware_probed(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity.json"
            for content in (b"not-json", b"{}", b'{"version":2}', b"[]"):
                path.write_bytes(content)
                with (
                    self.assertNoLogs(identity_module.LOGGER),
                    patch.object(
                        identity_module, "collect_hardware_identity_inputs"
                    ) as collect,
                ):
                    for create in (False, True):
                        with self.assertRaises(IdentityPersistenceError):
                            load_node_profile(
                                path, create=create, include_hardware=True
                            )
                collect.assert_not_called()
                self.assertEqual(path.read_bytes(), content)

    def test_out_of_wire_bounds_saved_identity_is_rejected_without_migration(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "identity.json"
            for identity in (NodeIdentity("x" * 129), NodeIdentity("valid", "x" * 129)):
                _save_identity(path, identity, overwrite_existing=True)
                content = path.read_bytes()
                self.assertEqual(read_identity(path), identity)
                with self.assertRaises(ValueError):
                    load_node_profile(path)
                self.assertEqual(path.read_bytes(), content)

    def test_opt_in_flags_require_explicit_booleans_before_io(self) -> None:
        for field in ("create", "include_hardware"):
            for value in (None, 0, 1, "true", [], {}):
                with (
                    self.subTest(field=field, value=value),
                    patch.object(identity_module, "read_identity") as read,
                    patch.object(identity_module, "load_or_create_identity") as create,
                ):
                    with self.assertRaises(ValueError):
                        load_node_profile(Path("not-read.json"), **{field: value})
                    read.assert_not_called()
                    create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
