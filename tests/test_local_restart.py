import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.cli import main
from aethermesh_core.local_json_helpers import load_json_mapping, require_text_field
from aethermesh_core.local_restart import LocalRestartError, restart_local_node
from aethermesh_core.local_startup import start_local_node


class LocalRestartTests(unittest.TestCase):
    def test_restart_preserves_identity_and_restores_auditable_local_state(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start = start_local_node(root).to_dict()
            original_node_id = str(start["node_id"])
            original_creator_node_id = str(start["creator_node_id"])
            original_manifest = self._load(root / str(start["manifest_path"]))
            original_receipt = self._load(root / str(start["validation_receipt_path"]))
            original_lineage = self._load(root / str(start["lineage_path"]))
            contribution_dir = root / "contributions"
            contribution_dir.mkdir(exist_ok=True)
            contribution = {
                "node_id": original_node_id,
                "creator_node_id": original_creator_node_id,
                "attribution": "local-only",
            }
            (contribution_dir / "contribution-0001.json").write_text(
                json.dumps(contribution), encoding="utf-8"
            )
            (root / "node.pid").write_text("12345", encoding="utf-8")
            (root / "runtime.lock").write_text("locked", encoding="utf-8")
            (root / "work" / "in-progress").mkdir(parents=True)
            (root / "work" / "in-progress" / "active.json").write_text(
                json.dumps({"job_id": "job-1"}), encoding="utf-8"
            )

            result = restart_local_node(root).to_dict()
            restart_receipt = self._load(root / str(result["restart_receipt_path"]))
            shutdown_state = self._load(root / str(result["shutdown_state_path"]))
            restored_manifest = self._load(root / str(result["manifest_path"]))
            restored_original_receipt = self._load(
                root / str(start["validation_receipt_path"])
            )
            restored_original_lineage = self._load(root / str(start["lineage_path"]))
            restored_contribution = self._load(
                root / "contributions" / "contribution-0001.json"
            )

        self.assertEqual(result["node_id"], original_node_id)
        self.assertEqual(result["creator_node_id"], original_creator_node_id)
        self.assertEqual(result["final_status"], "restarted")
        self.assertEqual(result["network_mode"], "local-only-no-p2p")
        self.assertEqual(restored_manifest, original_manifest)
        self.assertEqual(restored_original_receipt, original_receipt)
        self.assertEqual(restored_original_lineage, original_lineage)
        self.assertEqual(restored_contribution, contribution)
        self.assertEqual(
            result["restored_manifest_refs"], [str(start["manifest_path"])]
        )
        self.assertEqual(
            result["restored_validation_receipt_refs"],
            [str(start["validation_receipt_path"])],
        )
        self.assertEqual(result["restored_lineage_refs"], [str(start["lineage_path"])])
        self.assertEqual(
            result["restored_contribution_refs"],
            ["contributions/contribution-0001.json"],
        )
        self.assertEqual(shutdown_state["status"], "stopped")
        self.assertEqual(shutdown_state["interrupted_work_count"], 1)
        self.assertEqual(
            result["recovery_decisions"],
            [
                {
                    "work_ref": "work/in-progress/active.json",
                    "previous_status": "stopped_retryable",
                    "restart_status": "pending_retry",
                    "decision": "left pending for local retry; not marked completed or validated",
                }
            ],
        )
        self.assertEqual(restart_receipt["receipt_type"], "local_node_restart")
        self.assertEqual(
            restart_receipt["restored_identity"]["node_id"], original_node_id
        )
        self.assertEqual(
            restart_receipt["restored_identity"]["creator_node_id"],
            original_creator_node_id,
        )
        self.assertEqual(
            restart_receipt["recovery_decisions"], result["recovery_decisions"]
        )
        self.assertEqual(restart_receipt["network_mode"], "local-only-no-p2p")
        restart_json = json.dumps(restart_receipt).lower()
        self.assertNotIn("decentralized", restart_json)
        self.assertNotIn("remote coordination", restart_json)

    def test_restart_refuses_identity_rotation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_local_node(root)
            with (
                patch(
                    "aethermesh_core.local_restart.start_local_node",
                    side_effect=LocalRestartError("restart changed node_id"),
                ),
                self.assertRaisesRegex(LocalRestartError, "restart changed node_id"),
            ):
                restart_local_node(root)

    def test_restart_cli_reports_json_and_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            start_local_node(root)
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(["restart-local-node", "--runtime-dir", str(root)])
            payload = json.loads(stdout.getvalue())

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["final_status"], "restarted")

        with tempfile.TemporaryDirectory() as temp_dir:
            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = main(["restart-local-node", "--runtime-dir", temp_dir])

        self.assertEqual(exit_code, 1)
        self.assertIn("identity file is missing", stderr.getvalue())

    def test_restart_shared_json_helpers_preserve_local_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "not-object.json"
            path.write_text("[]", encoding="utf-8")

            with self.assertRaisesRegex(
                LocalRestartError, "sample JSON must be an object"
            ):
                load_json_mapping(path, "sample", LocalRestartError)

        with self.assertRaisesRegex(
            LocalRestartError, "sample field 'node_id' must be a string"
        ):
            require_text_field({"node_id": ""}, "node_id", "sample", LocalRestartError)

    @staticmethod
    def _load(path: Path) -> dict[str, Any]:
        with path.open("r", encoding="utf-8") as handle:
            document = json.load(handle)
        if not isinstance(document, dict):
            raise AssertionError("expected object")  # noqa: TRY004 - justification: Fail the test, including under python -O.
        return document


if __name__ == "__main__":
    unittest.main()
