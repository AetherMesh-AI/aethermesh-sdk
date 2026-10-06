import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from aethermesh_core.node_state import (
    LocalNodeProcessingState,
    NodeStatePersistenceError,
    load_node_processing_state,
    save_node_processing_state,
)


class NodeStateTests(unittest.TestCase):
    def test_missing_state_loads_empty_for_expected_node(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"

            state = load_node_processing_state(state_path, expected_node_id="node-a")

        self.assertEqual(state.node_id, "node-a")
        self.assertEqual(state.processed_message_ids, [])
        self.assertEqual(state.processed_assignment_count, 0)

    def test_save_and_load_state_preserves_order_and_unknown_fields(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "deep" / "nested" / "node-state.json"
            state = LocalNodeProcessingState(
                node_id="node-a",
                processed_message_ids=["msg-0001", "msg-0003"],
                extra_fields={"note": "future metadata"},
            )

            save_node_processing_state(state_path, state)
            loaded = load_node_processing_state(state_path, expected_node_id="node-a")
            persisted = json.loads(state_path.read_text(encoding="utf-8"))

        self.assertEqual(loaded.node_id, "node-a")
        self.assertEqual(loaded.processed_message_ids, ["msg-0001", "msg-0003"])
        self.assertEqual(loaded.extra_fields, {"note": "future metadata"})
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(persisted["processed_assignment_count"], 2)
        self.assertEqual(persisted["processed_message_ids"], ["msg-0001", "msg-0003"])

    def test_malformed_json_is_rejected_without_rewrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text("not-json", encoding="utf-8")

            with self.assertRaisesRegex(NodeStatePersistenceError, "malformed"):
                load_node_processing_state(state_path, expected_node_id="node-a")

            self.assertEqual(state_path.read_text(encoding="utf-8"), "not-json")

    def test_missing_required_field_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text(json.dumps({"version": 1}), encoding="utf-8")

            with self.assertRaisesRegex(
                NodeStatePersistenceError, "missing required field"
            ):
                load_node_processing_state(state_path, expected_node_id="node-a")

    def test_wrong_field_types_are_rejected(self) -> None:
        cases = [
            {
                "version": 1,
                "node_id": 7,
                "processed_message_ids": [],
                "processed_assignment_count": 0,
            },
            {
                "version": 1,
                "node_id": "node-a",
                "processed_message_ids": "msg-1",
                "processed_assignment_count": 0,
            },
            {
                "version": 1,
                "node_id": "node-a",
                "processed_message_ids": ["msg-1"],
                "processed_assignment_count": "1",
            },
        ]
        for document in cases:
            with (
                self.subTest(document=document),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                state_path = Path(temp_dir) / "node-state.json"
                state_path.write_text(json.dumps(document), encoding="utf-8")

                with self.assertRaises(NodeStatePersistenceError):
                    load_node_processing_state(state_path, expected_node_id="node-a")

    def test_duplicate_processed_ids_are_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "node_id": "node-a",
                        "processed_message_ids": ["msg-0001", "msg-0001"],
                        "processed_assignment_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(NodeStatePersistenceError, "duplicate"):
                load_node_processing_state(state_path, expected_node_id="node-a")

    def test_unsupported_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 2,
                        "node_id": "node-a",
                        "processed_message_ids": [],
                        "processed_assignment_count": 0,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(NodeStatePersistenceError, "version 1"):
                load_node_processing_state(state_path, expected_node_id="node-a")

    def test_wrong_node_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "node_id": "node-b",
                        "processed_message_ids": [],
                        "processed_assignment_count": 0,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(NodeStatePersistenceError, "node-b"):
                load_node_processing_state(state_path, expected_node_id="node-a")

    def test_count_must_match_unique_processed_ids(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "node_id": "node-a",
                        "processed_message_ids": ["msg-0001"],
                        "processed_assignment_count": 2,
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(NodeStatePersistenceError, "must equal"):
                load_node_processing_state(state_path, expected_node_id="node-a")

    def test_save_node_processing_state_uses_stable_json_and_cleans_temp_files(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "deep" / "nested" / "node-state.json"
            state = LocalNodeProcessingState(
                node_id="node-a",
                processed_message_ids=["msg-0001", "msg-0003"],
                extra_fields={"z_future": True, "a_note": "kept"},
            )

            save_node_processing_state(state_path, state)
            raw = state_path.read_text(encoding="utf-8")

            self.assertEqual(
                raw,
                '{\n  "a_note": "kept",\n  "node_id": "node-a",\n  "processed_assignment_count": 2,\n  "processed_message_ids": [\n    "msg-0001",\n    "msg-0003"\n  ],\n  "version": 1,\n  "z_future": true\n}\n',
            )
            self.assertEqual(
                sorted(child.name for child in state_path.parent.iterdir()),
                ["node-state.json"],
            )

    def test_save_node_processing_state_preserves_existing_file_on_atomic_replace_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            state_path.write_text('{"original": true}\n', encoding="utf-8")
            state = LocalNodeProcessingState(
                node_id="node-a",
                processed_message_ids=["msg-0001"],
                extra_fields={},
            )

            with (
                mock.patch(
                    "aethermesh_core.node_state.os.replace",
                    side_effect=OSError("replace failed"),
                ),
                self.assertRaisesRegex(NodeStatePersistenceError, "replace failed"),
            ):
                save_node_processing_state(state_path, state)

            self.assertEqual(
                state_path.read_text(encoding="utf-8"), '{"original": true}\n'
            )
            self.assertEqual(
                sorted(child.name for child in state_path.parent.iterdir()),
                ["node-state.json"],
            )

    def test_bool_values_are_rejected_from_node_state_structural_fields(self) -> None:
        cases = [
            {
                "version": True,
                "node_id": "node-a",
                "processed_message_ids": [],
                "processed_assignment_count": 0,
            },
            {
                "version": 1,
                "node_id": "node-a",
                "processed_message_ids": [True],
                "processed_assignment_count": 1,
            },
            {
                "version": 1,
                "node_id": "node-a",
                "processed_message_ids": [],
                "processed_assignment_count": False,
            },
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            for document in cases:
                with self.subTest(document=document):
                    state_path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(NodeStatePersistenceError):
                        load_node_processing_state(
                            state_path, expected_node_id="node-a"
                        )

    def test_save_node_processing_state_uses_target_parent_temp_file_contract(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "deep" / "nested" / "node-state.json"
            state = LocalNodeProcessingState("node-a", [], {})
            calls: list[dict[str, object]] = []
            real_named_temporary_file = tempfile.NamedTemporaryFile

            def capture_named_temporary_file(*args, **kwargs):
                calls.append({"args": args, "kwargs": dict(kwargs)})
                return real_named_temporary_file(*args, **kwargs)

            with mock.patch(
                "aethermesh_core.node_state.tempfile.NamedTemporaryFile",
                side_effect=capture_named_temporary_file,
            ):
                save_node_processing_state(path, state)

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["args"], ("w",))
            self.assertEqual(calls[0]["kwargs"]["encoding"], "utf-8")
            self.assertEqual(calls[0]["kwargs"]["dir"], path.parent)
            self.assertEqual(calls[0]["kwargs"]["prefix"], ".node-state.json.")
            self.assertEqual(calls[0]["kwargs"]["suffix"], ".tmp")
            self.assertEqual(calls[0]["kwargs"]["delete"], False)

    def test_load_node_processing_state_error_messages_are_stable(self) -> None:
        cases = [
            (
                {
                    "version": True,
                    "node_id": "node-a",
                    "processed_message_ids": [],
                    "processed_assignment_count": 0,
                },
                "node state JSON must contain version 1",
            ),
            (
                {
                    "version": 1,
                    "processed_message_ids": [],
                    "processed_assignment_count": 0,
                },
                "node state JSON is missing required field(s): node_id",
            ),
            (
                {
                    "version": 1,
                    "node_id": "",
                    "processed_message_ids": [],
                    "processed_assignment_count": 0,
                },
                "node state field 'node_id' must be a non-empty string",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-b",
                    "processed_message_ids": [],
                    "processed_assignment_count": 0,
                },
                "node state belongs to node 'node-b', not 'node-a'",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "processed_message_ids": {},
                    "processed_assignment_count": 0,
                },
                "node state field 'processed_message_ids' must be a list",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "processed_message_ids": [""],
                    "processed_assignment_count": 1,
                },
                "node state processed_message_ids[0] must be a non-empty string",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "processed_message_ids": ["msg-1", "msg-1"],
                    "processed_assignment_count": 2,
                },
                "node state contains duplicate processed message id: msg-1",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "processed_message_ids": [],
                    "processed_assignment_count": False,
                },
                "node state field 'processed_assignment_count' must be an integer",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "processed_message_ids": ["msg-1"],
                    "processed_assignment_count": 2,
                },
                "node state processed_assignment_count must equal the number of unique processed message ids",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            state_path = Path(temp_dir) / "node-state.json"
            for document, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    state_path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(NodeStatePersistenceError) as cm:
                        load_node_processing_state(
                            state_path, expected_node_id="node-a"
                        )
                    self.assertEqual(str(cm.exception), expected_message)


if __name__ == "__main__":
    unittest.main()
