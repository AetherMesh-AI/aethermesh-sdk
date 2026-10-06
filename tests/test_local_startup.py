import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.cli import main
from aethermesh_core.local_runtime_config import LOCAL_RUNTIME_CONFIG_PATH
from aethermesh_core.local_startup import LocalStartupError, start_local_node
from aethermesh_core.version_metadata import capture_version_metadata


class LocalNodeStartupTests(unittest.TestCase):
    def test_runtime_path_file_fails_with_clear_startup_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir) / "runtime"
            runtime.write_text("not a directory", encoding="utf-8")

            with self.assertRaisesRegex(
                LocalStartupError, "local runtime path must be a directory"
            ):
                start_local_node(runtime)

            self.assertEqual(runtime.read_text(encoding="utf-8"), "not a directory")

    def test_clean_startup_creates_auditable_artifacts_and_preserves_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)

            first = start_local_node(runtime).to_dict()
            second = start_local_node(runtime).to_dict()

            self.assertEqual(first["node_id"], second["node_id"])
            self.assertEqual(first["creator_node_id"], second["creator_node_id"])
            self.assertEqual(first["network_mode"], "local-only-no-p2p")
            audit_events = [
                json.loads(line)
                for line in (runtime / "logs" / "startup.log")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(audit_events), 2)
            for event, startup in zip(audit_events, (first, second), strict=True):
                self.assertEqual(event["event"], "local_node_startup")
                self.assertEqual(event["event_type"], "node_startup")
                self.assertRegex(event["timestamp"], r"^\d{4}-\d{2}-\d{2}T")
                self.assertEqual(event["node_id"], startup["node_id"])
                self.assertEqual(event["creator_node_id"], startup["creator_node_id"])
                self.assertEqual(event["manifest_ref"], startup["manifest_path"])
                self.assertEqual(event["manifest_hash"], startup["manifest_hash"])
                self.assertEqual(event["lineage_ref"], startup["lineage_path"])
                self.assertEqual(event["validation_status"], "passed")
                self.assertEqual(
                    event["contribution_attribution"],
                    {
                        "creator_node_id": startup["creator_node_id"],
                        "attribution_node_id": startup["node_id"],
                        "event": "local_node_startup",
                        "scoring_applied": False,
                    },
                )
                self.assertNotIn("token", json.dumps(event).lower())
                self.assertNotIn("reward", json.dumps(event).lower())
                self.assertNotIn("peer", json.dumps(event).lower())
                self.assertNotIn("consensus", json.dumps(event).lower())
            self.assertNotEqual(
                audit_events[0]["local_run_id"], audit_events[1]["local_run_id"]
            )
            for relative_path in (
                "identity/creator-node.json",
                "manifests/local-node-manifest.json",
                "contributions",
                "receipts",
                "lineage",
                "logs/startup.log",
                "work/inputs",
                "work/outputs",
            ):
                self.assertTrue((runtime / relative_path).exists(), relative_path)
            config = self._load(runtime / LOCAL_RUNTIME_CONFIG_PATH)
            self.assertEqual(config["node"]["node_id"], first["node_id"])
            self.assertEqual(
                config["node"]["creator_node_id"], first["creator_node_id"]
            )
            self.assertIn(
                "do not regenerate",
                config["node"]["creator_node_id_stability"],
            )
            self.assertEqual(
                config["paths"],
                {
                    "identity": "identity/creator-node.json",
                    "manifest": "manifests/local-node-manifest.json",
                    "validation_receipts": "receipts",
                    "lineage": "lineage",
                    "contribution_attribution": "contributions",
                    "log": "logs/startup.log",
                    "work_inputs": "work/inputs",
                    "work_outputs": "work/outputs",
                },
            )
            self.assertNotIn("token", json.dumps(config).lower())
            self.assertNotIn("reward", json.dumps(config).lower())
            self.assertNotIn("dashboard", json.dumps(config).lower())
            self.assertNotIn(
                "p2p", config["network_mode"].lower().replace("no-p2p", "")
            )
            first_dirs = first["runtime_directories"]
            self.assertIsInstance(first_dirs, dict)
            assert isinstance(first_dirs, dict)
            self.assertEqual(first_dirs["receipts"], "receipts")
            self.assertEqual(first_dirs["lineage"], "lineage")
            self.assertEqual(
                config["paths"]["contribution_attribution"], "contributions"
            )
            receipt = self._load(runtime / str(first["validation_receipt_path"]))
            second_receipt = self._load(
                runtime / str(second["validation_receipt_path"])
            )
            lineage = self._load(runtime / str(first["lineage_path"]))
            manifest = self._load(runtime / str(first["manifest_path"]))
            self.assertEqual(receipt["receipt_type"], "startup_validation")
            self.assertEqual(receipt["command_invoked"], "start-local-node")
            self.assertEqual(second_receipt["command_invoked"], "start-local-node")
            self.assertNotEqual(
                first["validation_receipt_path"], second["validation_receipt_path"]
            )
            self.assertEqual(receipt["node_id"], first["node_id"])
            self.assertEqual(receipt["creator_node_id"], first["creator_node_id"])
            self.assertEqual(receipt["manifest_hash"], first["manifest_hash"])
            self.assertEqual(receipt["validation_result"]["status"], "passed")
            self.assertFalse(receipt["contribution_attribution"]["scoring_applied"])
            self.assertNotIn("private", json.dumps(receipt).lower())
            self.assertEqual(lineage["lineage_type"], "local_node_startup")
            self.assertEqual(lineage["inputs"]["manifest_hash"], first["manifest_hash"])
            self.assertFalse(lineage["contribution_attribution"]["scoring_applied"])
            advertisements = manifest["capability_advertisements"]
            self.assertEqual(len(advertisements), 1)
            advertisement = advertisements[0]
            self.assertEqual(
                advertisement["capability_id"], "local.manifest-validation"
            )
            self.assertEqual(advertisement["version"], "1.0.0")
            self.assertEqual(advertisement["creator_node_id"], first["creator_node_id"])
            self.assertEqual(
                advertisement["validation"]["required_receipt_ref"],
                first["validation_receipt_path"],
            )
            self.assertEqual(
                advertisement["lineage"]["source_manifest_ref"], first["manifest_path"]
            )
            self.assertEqual(
                advertisement["contribution_attribution"]["creator_node_id"],
                first["creator_node_id"],
            )
            self.assertEqual(
                advertisement["contribution_attribution"]["work_record_ref"],
                first["validation_receipt_path"],
            )
            self.assertEqual(advertisement["network_mode"], "local-only-no-p2p")
            advertisement_audit_events = [
                json.loads(line)
                for line in (runtime / "logs" / "local-audit-events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(advertisement_audit_events), 2)
            for event, startup, action in zip(
                advertisement_audit_events,
                (first, second),
                ("created", "refreshed"),
                strict=True,
            ):
                self.assertEqual(event["event_type"], "capability_advertised")
                self.assertEqual(event["capability_advertisement_action"], action)
                self.assertEqual(event["creator_node_id"], startup["creator_node_id"])
                self.assertEqual(event["node_id"], startup["node_id"])
                self.assertEqual(event["capability_id"], advertisement["capability_id"])
                self.assertEqual(event["manifest_digest"], startup["manifest_hash"])
                self.assertEqual(
                    event["validation_receipt_refs"],
                    [advertisement["validation"]["required_receipt_ref"]],
                )
                self.assertEqual(event["lineage_refs"], [startup["lineage_path"]])
                self.assertEqual(
                    event["contribution_attribution"]["creator_node_id"],
                    startup["creator_node_id"],
                )
                self.assertNotIn("description", event)
                self.assertRegex(
                    event["advertisement_payload_digest"], r"^sha256:[0-9a-f]{64}$"
                )

    def test_reset_startup_appends_a_replaced_capability_advertisement_event(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            reset = start_local_node(runtime, reset_creator_identity=True)

            events = [
                json.loads(line)
                for line in (runtime / "logs" / "local-audit-events.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]

            self.assertEqual(
                [event["capability_advertisement_action"] for event in events],
                ["created", "replaced"],
            )
            self.assertEqual(events[-1]["creator_node_id"], reset.creator_node_id)

    def test_rejected_manifest_does_not_append_capability_advertisement_event(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            audit_path = runtime / "logs" / "local-audit-events.jsonl"
            before = audit_path.read_text(encoding="utf-8")
            (runtime / "manifests" / "local-node-manifest.json").write_text(
                "{not json", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                LocalStartupError, "startup manifest JSON is malformed"
            ):
                start_local_node(runtime)

            self.assertEqual(audit_path.read_text(encoding="utf-8"), before)

    def test_existing_identity_missing_manifest_fails_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            (runtime / "manifests" / "local-node-manifest.json").unlink()

            with self.assertRaisesRegex(LocalStartupError, "required startup manifest"):
                start_local_node(runtime)

    def test_existing_version_one_manifest_is_upgraded_with_advertisement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            manifest_path = runtime / first.manifest_path
            manifest = self._load(manifest_path)
            manifest.pop("capability_advertisements")
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            second = start_local_node(runtime)

            upgraded = self._load(manifest_path)
            advertisement = upgraded["capability_advertisements"][0]
            self.assertEqual(second.creator_node_id, first.creator_node_id)
            self.assertEqual(advertisement["creator_node_id"], first.creator_node_id)
            self.assertEqual(
                advertisement["validation"]["required_receipt_ref"],
                second.validation_receipt_path,
            )

    def test_existing_config_missing_identity_fails_before_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            identity_path = runtime / first.identity_path
            identity_path.unlink()
            receipt_count = len(tuple((runtime / "receipts").iterdir()))
            lineage_count = len(tuple((runtime / "lineage").iterdir()))

            with self.assertRaisesRegex(
                LocalStartupError, "required creator node identity is missing"
            ):
                start_local_node(runtime)

            self.assertFalse(identity_path.exists())
            self.assertEqual(
                len(tuple((runtime / "receipts").iterdir())), receipt_count
            )
            self.assertEqual(len(tuple((runtime / "lineage").iterdir())), lineage_count)

    def test_missing_config_rejects_nondefault_preserved_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            preserved_identity = runtime / "configured" / "creator-node.json"
            preserved_identity.parent.mkdir()
            preserved_identity.write_bytes(b"preserved identity")

            with self.assertRaisesRegex(
                LocalStartupError, "required local runtime config is missing"
            ):
                start_local_node(runtime)

            self.assertEqual(preserved_identity.read_bytes(), b"preserved identity")
            self.assertFalse((runtime / "identity" / "creator-node.json").exists())
            self.assertFalse((runtime / LOCAL_RUNTIME_CONFIG_PATH).exists())

    def test_missing_config_allows_precreated_empty_runtime_directories(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            for relative_path in ("receipts", "lineage", "work/inputs", "work/outputs"):
                (runtime / relative_path).mkdir(parents=True, exist_ok=True)

            result = start_local_node(runtime)

            self.assertEqual(result.validation_result, "passed")
            self.assertTrue((runtime / LOCAL_RUNTIME_CONFIG_PATH).is_file())
            self.assertTrue((runtime / result.identity_path).is_file())

    def test_missing_required_runtime_config_fields_fail_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            for field in (
                "identity",
                "manifest",
                "validation_receipts",
                "lineage",
                "contribution_attribution",
                "log",
                "work_inputs",
                "work_outputs",
            ):
                with self.subTest(field=field):
                    candidate = json.loads(json.dumps(config))
                    paths = candidate["paths"]
                    self.assertIsInstance(paths, dict)
                    assert isinstance(paths, dict)
                    paths.pop(field)
                    config_path.write_text(json.dumps(candidate), encoding="utf-8")

                    with self.assertRaisesRegex(
                        LocalStartupError,
                        f"local runtime config paths.{field} must be a non-empty local path",
                    ) as caught:
                        start_local_node(runtime)
                    self.assertEqual(caught.exception.code, "STARTUP_CONFIG_INVALID")
                    self.assertEqual(caught.exception.phase, "config_load")

    def test_runtime_config_rejects_malformed_version_and_identity_stability(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            cases = (
                ({"version": True}, "must contain version 1"),
                (
                    {"node.creator_node_id_stability": None},
                    "must preserve local identity",
                ),
            )
            for updates, message in cases:
                with self.subTest(updates=updates):
                    candidate = json.loads(json.dumps(config))
                    for key, value in updates.items():
                        if key.startswith("node."):
                            candidate["node"][key.removeprefix("node.")] = value
                        else:
                            candidate[key] = value
                    config_path.write_text(json.dumps(candidate), encoding="utf-8")

                    with self.assertRaisesRegex(LocalStartupError, message):
                        start_local_node(runtime)

    def test_runtime_uses_configured_local_artifact_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            manifest = self._load(runtime / first.manifest_path)
            config_paths = config["paths"]
            self.assertIsInstance(config_paths, dict)
            assert isinstance(config_paths, dict)
            config_paths.update(
                {
                    "manifest": "configured/manifests/node-manifest.json",
                    "validation_receipts": "configured/receipts",
                    "lineage": "configured/lineage",
                    "contribution_attribution": "configured/contributions",
                    "log": "configured/logs/startup.log",
                    "work_inputs": "configured/work/inputs",
                    "work_outputs": "configured/work/outputs",
                }
            )
            manifest["work_directories"] = {
                "manifests": "configured/manifests",
                "receipts": "configured/receipts",
                "logs": "configured/logs",
                "work_inputs": "configured/work/inputs",
                "work_outputs": "configured/work/outputs",
                "lineage": "configured/lineage",
                "contribution_attribution": "configured/contributions",
            }
            custom_manifest = runtime / str(config_paths["manifest"])
            custom_manifest.parent.mkdir(parents=True)
            custom_manifest.write_text(json.dumps(manifest), encoding="utf-8")
            config_path.write_text(json.dumps(config), encoding="utf-8")

            second = start_local_node(runtime)

            self.assertEqual(second.creator_node_id, first.creator_node_id)
            self.assertEqual(
                second.manifest_path, "configured/manifests/node-manifest.json"
            )
            self.assertTrue(
                second.validation_receipt_path.startswith("configured/receipts/")
            )
            self.assertTrue(second.lineage_path.startswith("configured/lineage/"))
            self.assertEqual(second.log_path, "configured/logs/startup.log")
            self.assertEqual(
                config_paths["contribution_attribution"], "configured/contributions"
            )

    def test_runtime_config_rejects_unsupported_fields_and_nonlocal_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)

            for field_name in ("tokenomics", "reward_policy", "dashboard"):
                with self.subTest(field=field_name):
                    candidate = dict(config)
                    candidate[field_name] = {"enabled": True}
                    config_path.write_text(json.dumps(candidate), encoding="utf-8")
                    with self.assertRaisesRegex(
                        LocalStartupError, "unsupported fields"
                    ):
                        start_local_node(runtime)

            config_path.write_text(json.dumps(config), encoding="utf-8")
            for bad_path in (
                "~/aethermesh",
                "C:/Users/trevoroler/aethermesh",
                "https://example.invalid/config.json",
            ):
                with self.subTest(path=bad_path):
                    candidate = json.loads(json.dumps(config))
                    candidate["paths"]["lineage"] = bad_path
                    config_path.write_text(json.dumps(candidate), encoding="utf-8")
                    with self.assertRaisesRegex(
                        LocalStartupError, "relative local path"
                    ):
                        start_local_node(runtime)

    def test_corrupt_manifest_fails_before_new_receipt_is_accepted(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            receipt_count = len(
                tuple((runtime / "receipts").glob("startup-validation-*.json"))
            )
            (runtime / "manifests" / "local-node-manifest.json").write_text(
                "{not json", encoding="utf-8"
            )

            with self.assertRaisesRegex(
                LocalStartupError, "startup manifest JSON is malformed"
            ):
                start_local_node(runtime)
            self.assertEqual(
                len(tuple((runtime / "receipts").glob("startup-validation-*.json"))),
                receipt_count,
            )

    def test_overlapping_runtime_paths_fail_before_identity_or_receipt_writes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            identity_path = runtime / "identity" / "creator-node.json"
            identity_before = identity_path.read_bytes()
            receipt_count = len(
                tuple((runtime / "receipts").glob("startup-validation-*.json"))
            )
            lineage_count = len(
                tuple((runtime / "lineage").glob("startup-lineage-*.json"))
            )
            cases = (
                ({"lineage": "receipts/lineage"}, "artifact directories"),
                ({"log": "runtime-config.json"}, "file paths"),
                (
                    {"identity": "records", "manifest": "records/manifest.json"},
                    "file paths",
                ),
                ({"log": "work"}, "must not overlap"),
                ({"work_outputs": "logs"}, "must not overlap"),
            )
            for updates, message in cases:
                with self.subTest(updates=updates):
                    candidate = json.loads(json.dumps(config))
                    candidate["paths"].update(updates)
                    config_path.write_text(json.dumps(candidate), encoding="utf-8")

                    with self.assertRaisesRegex(LocalStartupError, message):
                        start_local_node(runtime)

                    self.assertEqual(identity_path.read_bytes(), identity_before)
                    self.assertEqual(
                        len(
                            tuple(
                                (runtime / "receipts").glob("startup-validation-*.json")
                            )
                        ),
                        receipt_count,
                    )
                    self.assertEqual(
                        len(
                            tuple((runtime / "lineage").glob("startup-lineage-*.json"))
                        ),
                        lineage_count,
                    )

    def test_runtime_paths_cannot_escape_root_through_symlinks(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir)
            runtime = parent / "runtime"
            outside = parent / "outside"
            runtime.mkdir()
            outside.mkdir()
            (runtime / "work").symlink_to(outside, target_is_directory=True)

            with self.assertRaisesRegex(
                LocalStartupError,
                "paths.work_inputs must stay within the runtime directory",
            ):
                start_local_node(runtime)

            self.assertFalse((runtime / "identity" / "creator-node.json").exists())
            self.assertEqual(tuple(outside.iterdir()), ())

    def test_configured_symlink_escape_fails_before_preserved_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir)
            runtime = parent / "runtime"
            outside = parent / "outside"
            first = start_local_node(runtime)
            outside.mkdir()
            (runtime / "escape").symlink_to(outside, target_is_directory=True)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            config["paths"]["lineage"] = "escape/lineage"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            identity_path = runtime / first.identity_path
            identity_before = identity_path.read_bytes()
            receipt_count = len(tuple((runtime / "receipts").iterdir()))

            with self.assertRaisesRegex(
                LocalStartupError,
                "paths.lineage must stay within the runtime directory",
            ):
                start_local_node(runtime)

            self.assertEqual(identity_path.read_bytes(), identity_before)
            self.assertEqual(
                len(tuple((runtime / "receipts").iterdir())), receipt_count
            )
            self.assertEqual(tuple(outside.iterdir()), ())

    def test_runtime_config_symlink_cannot_escape_root(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            parent = Path(temp_dir)
            runtime = parent / "runtime"
            outside_config = parent / "outside-config.json"
            runtime.mkdir()
            outside_config.write_text("not read", encoding="utf-8")
            (runtime / LOCAL_RUNTIME_CONFIG_PATH).symlink_to(outside_config)

            with self.assertRaisesRegex(
                LocalStartupError,
                "local runtime config must stay within the runtime directory",
            ):
                start_local_node(runtime)

            self.assertFalse((runtime / "identity").exists())

    def test_dangling_runtime_config_symlink_fails_before_startup_writes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config_path.symlink_to(runtime / "missing-config.json")

            with self.assertRaisesRegex(LocalStartupError, "dangling filesystem link"):
                start_local_node(runtime)

            self.assertTrue(config_path.is_symlink())
            self.assertFalse((runtime / "missing-config.json").exists())
            self.assertFalse((runtime / "identity").exists())
            self.assertFalse((runtime / "receipts").exists())

    def test_resolved_runtime_paths_cannot_alias_preserved_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            identity_path = runtime / first.identity_path
            identity_before = identity_path.read_bytes()
            (runtime / "alias").symlink_to(
                runtime / "receipts", target_is_directory=True
            )
            config["paths"]["lineage"] = "alias"
            config_path.write_text(json.dumps(config), encoding="utf-8")

            with self.assertRaisesRegex(
                LocalStartupError,
                "artifact directories must be separate",
            ):
                start_local_node(runtime)

            self.assertEqual(identity_path.read_bytes(), identity_before)

    def test_invalid_manifest_fields_fail_closed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            manifest_path = runtime / "manifests" / "local-node-manifest.json"
            manifest = self._load(manifest_path)
            manifest["validation"]["external_services_required"] = True
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(
                LocalStartupError, "validation.external_services_required"
            ):
                start_local_node(runtime)

    def test_reset_creator_identity_requires_explicit_flag_and_rotates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            second = start_local_node(runtime)
            self.assertEqual(first.node_id, second.node_id)
            reset = start_local_node(runtime, reset_creator_identity=True)
            self.assertEqual(first.node_id, reset.node_id)
            self.assertEqual(reset.node_id, reset.creator_node_id)
            self.assertTrue((runtime / "identity" / "identity-quarantine").exists())

            (runtime / LOCAL_RUNTIME_CONFIG_PATH).unlink()
            identity_path = runtime / reset.identity_path
            identity_before = identity_path.read_bytes()
            receipt_count = len(tuple((runtime / "receipts").iterdir()))

            with self.assertRaisesRegex(
                LocalStartupError, "required local runtime config is missing"
            ):
                start_local_node(runtime, reset_creator_identity=True)

            self.assertEqual(identity_path.read_bytes(), identity_before)
            self.assertEqual(
                len(tuple((runtime / "receipts").iterdir())), receipt_count
            )

    def test_reset_uses_configured_identity_and_receipt_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            first = start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            manifest = self._load(runtime / first.manifest_path)
            custom_identity = runtime / "configured" / "identity" / "creator.json"
            custom_identity.parent.mkdir(parents=True)
            (runtime / first.identity_path).replace(custom_identity)
            config["paths"]["identity"] = "configured/identity/creator.json"
            config["paths"]["validation_receipts"] = "configured/receipts"
            manifest["work_directories"]["receipts"] = "configured/receipts"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            (runtime / first.manifest_path).write_text(
                json.dumps(manifest), encoding="utf-8"
            )

            reset = start_local_node(runtime, reset_creator_identity=True)

            self.assertEqual(reset.identity_path, "configured/identity/creator.json")
            self.assertTrue(
                (runtime / "configured" / "identity" / "identity-quarantine").is_dir()
            )
            self.assertTrue(
                (
                    runtime / "configured" / "receipts" / "identity-reset-receipts.json"
                ).is_file()
            )
            self.assertFalse(
                (runtime / "receipts" / "identity-reset-receipts.json").exists()
            )

    def test_reset_rejects_config_identity_mismatch_before_rotation(self) -> None:
        for field in ("node_id", "creator_node_id"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as temp_dir:
                runtime = Path(temp_dir)
                first = start_local_node(runtime)
                config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
                config = self._load(config_path)
                config["node"][field] = "mismatched-node-id"
                config_path.write_text(json.dumps(config), encoding="utf-8")
                identity_path = runtime / first.identity_path
                identity_before = identity_path.read_bytes()

                with self.assertRaisesRegex(
                    LocalStartupError, f"node.{field} does not match identity"
                ):
                    start_local_node(runtime, reset_creator_identity=True)

                self.assertEqual(identity_path.read_bytes(), identity_before)
                self.assertFalse(
                    (identity_path.parent / "identity-quarantine").exists()
                )
                self.assertFalse(
                    (runtime / "receipts" / "identity-reset-receipts.json").exists()
                )

    def test_cli_starts_local_node_and_reports_startup_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["start-local-node", "--runtime-dir", temp_dir])
            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["validation_result"], "passed")

            stderr = io.StringIO()
            with (
                patch(
                    "aethermesh_core.cli.start_local_node",
                    side_effect=LocalStartupError("manifest bad"),
                ),
                contextlib.redirect_stderr(stderr),
            ):
                exit_code = main(["start-local-node", "--runtime-dir", temp_dir])
            self.assertEqual(exit_code, 1)
            self.assertIn("manifest bad", stderr.getvalue())
            self.assertIn("STARTUP_MANIFEST_INVALID", stderr.getvalue())
            self.assertIn("manifest_validation", stderr.getvalue())

    def test_fatal_startup_errors_are_stable_nonsecret_and_do_not_attribute(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            result = start_local_node(runtime)
            identity_path = runtime / result.identity_path
            identity = self._load(identity_path)
            identity["node"].pop("creator_node_id")
            identity_path.write_text(json.dumps(identity), encoding="utf-8")

            with self.assertRaises(LocalStartupError) as caught:
                start_local_node(runtime)

            error = caught.exception
            self.assertEqual(error.code, "STARTUP_IDENTITY_INVALID")
            self.assertEqual(error.phase, "identity_load")
            self.assertIn("creator_node_id", str(error))
            self.assertEqual(tuple((runtime / "contributions").iterdir()), ())

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            receipts = runtime / "receipts"
            for child in receipts.iterdir():
                child.unlink()
            receipts.rmdir()
            receipts.write_text(
                "private-key-material-must-not-appear", encoding="utf-8"
            )

            with self.assertRaises(LocalStartupError) as caught:
                start_local_node(runtime)

            error = caught.exception
            self.assertEqual(error.code, "STARTUP_RECEIPT_STORAGE_UNAVAILABLE")
            self.assertEqual(error.phase, "storage_check")
            self.assertIn("paths.validation_receipts", str(error))
            self.assertNotIn("private-key-material-must-not-appear", str(error))
            self.assertEqual(tuple((runtime / "contributions").iterdir()), ())

        classifications = (
            (
                "lineage store unavailable",
                "STARTUP_LINEAGE_STORAGE_UNAVAILABLE",
                "paths.lineage",
            ),
            (
                "contribution attribution store unavailable",
                "STARTUP_ATTRIBUTION_STORAGE_UNAVAILABLE",
                "paths.contribution_attribution",
            ),
        )
        for detail, code, field in classifications:
            with self.subTest(code=code):
                error = LocalStartupError(detail)
                self.assertEqual(error.code, code)
                self.assertEqual(error.phase, "storage_check")
                self.assertIn(field, str(error))

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            config_path = runtime / LOCAL_RUNTIME_CONFIG_PATH
            config = self._load(config_path)
            config["paths"]["validation_receipts"] = "custom/storage"
            config_path.write_text(json.dumps(config), encoding="utf-8")
            (runtime / "custom").mkdir()
            (runtime / "custom" / "storage").write_text("unavailable", encoding="utf-8")

            with self.assertRaises(LocalStartupError) as caught:
                start_local_node(runtime)

            error = caught.exception
            self.assertEqual(error.code, "STARTUP_RECEIPT_STORAGE_UNAVAILABLE")
            self.assertEqual(error.phase, "storage_check")
            self.assertIn("paths.validation_receipts", str(error))

        for leaked_path in (
            "/Users/example/private/runtime/receipts",
            "/Users/example/Private Runtime/receipts",
        ):
            with self.subTest(leaked_path=leaked_path):
                error = LocalStartupError(
                    f"could not create startup validation receipt at '{leaked_path}'"
                )
                self.assertNotIn(leaked_path, str(error))
                self.assertIn("<local-path>", str(error))

    def test_manifest_validation_rejects_bad_shapes(self) -> None:
        cases = (
            ("version", 2, "version 1"),
            ("version", True, "version 1"),
            ("version", 1.0, "version 1"),
            ("manifest_type", "other", "manifest_type"),
            ("node", "bad", "node must be an object"),
            ("capabilities", [], "capabilities"),
            ("runtime_version", {"bad": True}, "version metadata"),
            ("validation", "bad", "validation must be an object"),
            ("work_directories", [], "work_directories must be an object"),
        )
        for key, value, message in cases:
            with (
                self.subTest(key=key),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                runtime = Path(temp_dir)
                start_local_node(runtime)
                manifest_path = runtime / "manifests" / "local-node-manifest.json"
                manifest = self._load(manifest_path)
                manifest[key] = value
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaisesRegex(LocalStartupError, message):
                    start_local_node(runtime)

    def test_manifest_validation_rejects_identity_and_directory_mismatches(
        self,
    ) -> None:
        cases = (
            (("node", "node_id"), "other", "node_id does not match"),
            (("node", "creator_node_id"), "other", "creator_node_id does not match"),
            (("work_directories", "logs"), "elsewhere", "work_directories.logs"),
        )
        for path, value, message in cases:
            with (
                self.subTest(path=path),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                runtime = Path(temp_dir)
                start_local_node(runtime)
                manifest_path = runtime / "manifests" / "local-node-manifest.json"
                manifest = self._load(manifest_path)
                section, field = path
                manifest[section][field] = value
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                with self.assertRaisesRegex(LocalStartupError, message):
                    start_local_node(runtime)

    def test_low_level_startup_errors_are_reported_clearly(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            start_local_node(runtime)
            identity_path = runtime / "identity" / "creator-node.json"
            identity = self._load(identity_path)
            identity["node"].pop("creator_node_id")
            identity_path.write_text(json.dumps(identity), encoding="utf-8")
            with self.assertRaisesRegex(LocalStartupError, "creator_node_id"):
                start_local_node(runtime)

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            (runtime / "logs").parent.mkdir(parents=True, exist_ok=True)
            (runtime / "logs").write_text("not a directory", encoding="utf-8")
            with self.assertRaisesRegex(
                LocalStartupError, "required local runtime config"
            ):
                start_local_node(runtime)

        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            manifest_dir = runtime / "manifests"
            manifest_dir.mkdir(parents=True)
            manifest_path = manifest_dir / "local-node-manifest.json"
            manifest_path.write_text(json.dumps({"version": 1}), encoding="utf-8")
            with (
                patch(
                    "aethermesh_core.local_startup.load_or_create_identity",
                    side_effect=ValueError("identity backend failed"),
                ),
                self.assertRaises(ValueError),
            ):
                start_local_node(runtime)

    def test_default_manifest_uses_current_runtime_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runtime = Path(temp_dir)
            metadata = capture_version_metadata(captured_at="2026-01-01T00:00:00+00:00")
            with patch(
                "aethermesh_core.local_startup.capture_version_metadata",
                return_value=metadata,
            ):
                result = start_local_node(runtime)
            manifest = self._load(runtime / result.manifest_path)
            self.assertEqual(manifest["runtime_version"], metadata)

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        if not isinstance(document, dict):
            raise AssertionError("expected object")  # noqa: TRY004 - justification: Fail the test, including under python -O.
        return document


if __name__ == "__main__":
    unittest.main()
