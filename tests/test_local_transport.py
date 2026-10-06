import json
import tempfile
import unittest
from pathlib import Path

from aethermesh_core import local_transport
from aethermesh_core.local_transport import (
    LocalTransportError,
    collect_local_outboxes,
    load_local_inbox,
    local_inbox_path,
    local_outbox_path,
    materialize_local_inboxes,
    write_local_inbox,
    write_local_outbox,
)
from aethermesh_core.messages import MeshMessage


class LocalTransportTests(unittest.TestCase):
    def test_materializes_two_recipient_inboxes_deterministically(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "dispatch.json"
            transport_dir = Path(temp_dir) / "transport"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            _message_entry(
                                "msg-0001", "node-a", None, "node_heartbeat"
                            ),
                            _message_entry("msg-0002", "scheduler", "node-b"),
                            _message_entry("msg-0003", "scheduler", "node-a"),
                            _message_entry("msg-0004", "scheduler", "node-b"),
                        ],
                    }
                ),
                encoding="utf-8",
            )

            first = materialize_local_inboxes(
                message_log_path=message_log_path, transport_dir=transport_dir
            )
            node_a_path = local_inbox_path(transport_dir, "node-a")
            node_b_path = local_inbox_path(transport_dir, "node-b")
            first_a = node_a_path.read_text(encoding="utf-8")
            first_b = node_b_path.read_text(encoding="utf-8")
            second = materialize_local_inboxes(
                message_log_path=message_log_path, transport_dir=transport_dir
            )
            second_a = node_a_path.read_text(encoding="utf-8")
            second_b = node_b_path.read_text(encoding="utf-8")
            node_a = json.loads(first_a)
            node_b = json.loads(first_b)

        self.assertEqual(
            first,
            {
                "command": "materialize-local-inboxes",
                "message_log_path": str(message_log_path),
                "transport_dir": str(transport_dir),
                "inbox_count": 2,
                "message_count": 3,
                "node_ids": ["node-a", "node-b"],
                "inbox_paths": {
                    "node-a": str(node_a_path),
                    "node-b": str(node_b_path),
                },
            },
        )
        self.assertEqual(first, second)
        self.assertEqual(first_a, second_a)
        self.assertEqual(first_b, second_b)
        self.assertEqual(node_a["version"], 1)
        self.assertEqual(node_a["node_id"], "node-a")
        self.assertEqual(
            [message["message_id"] for message in node_a["messages"]], ["msg-0003"]
        )
        self.assertEqual(
            [message["message_id"] for message in node_b["messages"]],
            ["msg-0002", "msg-0004"],
        )

    def test_load_rejects_wrong_node_inbox(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = local_inbox_path(temp_dir, "node-a")
            path.parent.mkdir(parents=True)
            path.write_text(
                json.dumps({"version": 1, "node_id": "node-b", "messages": []}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(LocalTransportError, "node_id mismatch"):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

    def test_load_rejects_malformed_and_unsupported_inboxes(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = local_inbox_path(temp_dir, "node-a")
            path.parent.mkdir(parents=True)
            path.write_text("not-json", encoding="utf-8")
            with self.assertRaisesRegex(LocalTransportError, "malformed"):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

            path.write_text(
                json.dumps({"version": 2, "node_id": "node-a", "messages": []}),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(LocalTransportError, "version 1"):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

    def test_load_rejects_duplicate_missing_and_wrong_recipient_entries(self) -> None:
        duplicate = _message("msg-0001", "node-a")
        with tempfile.TemporaryDirectory() as temp_dir:
            write_local_inbox(
                transport_dir=temp_dir,
                node_id="node-a",
                messages=[duplicate],
            )
            path = local_inbox_path(temp_dir, "node-a")
            document = json.loads(path.read_text(encoding="utf-8"))
            document["messages"].append(document["messages"][0])
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(LocalTransportError, "duplicate message_id"):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

            document["messages"] = [{"message_type": "job_assigned"}]
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(LocalTransportError, "message_id"):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

            document["messages"] = [_message("msg-0002", "node-b").to_dict()]
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(
                LocalTransportError, "recipient_node_id must be node-a"
            ):
                load_local_inbox(transport_dir=temp_dir, node_id="node-a")

    def test_materialize_ignores_broadcast_messages_without_creating_inboxes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "broadcast-only.json"
            transport_dir = Path(temp_dir) / "transport"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            _message_entry(
                                "msg-0001", "node-a", None, "node_heartbeat"
                            ),
                            _message_entry(
                                "msg-0002", "node-b", None, "node_heartbeat"
                            ),
                        ],
                    }
                ),
                encoding="utf-8",
            )

            result = materialize_local_inboxes(
                message_log_path=message_log_path, transport_dir=transport_dir
            )

            self.assertEqual(result["inbox_count"], 0)
            self.assertEqual(result["message_count"], 0)
            self.assertEqual(result["node_ids"], [])
            self.assertEqual(result["inbox_paths"], {})
            self.assertFalse((transport_dir / "inboxes").exists())

    def test_materialize_rejects_duplicate_source_message_ids_before_writing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "duplicates.json"
            transport_dir = Path(temp_dir) / "transport"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            _message_entry("msg-0001", "scheduler", "node-a"),
                            _message_entry("msg-0001", "scheduler", "node-b"),
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                LocalTransportError, "duplicate message_id in source log"
            ):
                materialize_local_inboxes(
                    message_log_path=message_log_path, transport_dir=transport_dir
                )

            self.assertFalse((transport_dir / "inboxes").exists())

    def test_inbox_path_quotes_node_ids_without_changing_loaded_node_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            node_id = "node/a b?c"
            source_path = Path(temp_dir) / "source log.json"
            path = write_local_inbox(
                transport_dir=temp_dir,
                node_id=node_id,
                messages=[_message("msg-quoted", node_id)],
                source_message_log_path=source_path,
            )
            document = json.loads(path.read_text(encoding="utf-8"))
            messages = load_local_inbox(transport_dir=temp_dir, node_id=node_id)

        self.assertEqual(path.name, "node%2Fa%20b%3Fc.json")
        self.assertEqual(path.parent.name, "inboxes")
        self.assertEqual(
            str(local_inbox_path(temp_dir, "node-._~")),
            str(Path(temp_dir) / "inboxes" / "node-._~.json"),
        )
        self.assertEqual(document["node_id"], node_id)
        self.assertEqual(document["source_message_log_path"], str(source_path))
        self.assertEqual([message.message_id for message in messages], ["msg-quoted"])

    def test_load_local_inbox_error_messages_are_stable(self) -> None:
        valid_message = _message("msg-0001", "node-a").to_dict()
        cases = [
            (
                {"version": True, "node_id": "node-a", "messages": []},
                "local transport inbox JSON must contain version 1",
            ),
            (
                {"version": 1, "node_id": "node-b", "messages": []},
                "local transport inbox node_id mismatch: expected node-a, found node-b",
            ),
            (
                {"version": 1, "node_id": "node-a", "messages": {}},
                "local transport inbox field 'messages' must be a list",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "messages": [valid_message, valid_message],
                },
                "duplicate message_id in local transport inbox: msg-0001",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "messages": [_message("msg-0002", "node-b").to_dict()],
                },
                "local transport inbox entry 0 recipient_node_id must be node-a",
            ),
            (
                {
                    "version": 1,
                    "node_id": "node-a",
                    "messages": [{"message_type": "job_assigned"}],
                },
                "local transport inbox entry 0 is invalid: message_id must be a non-empty string",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = local_inbox_path(temp_dir, "node-a")
            path.parent.mkdir(parents=True)
            for document, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(LocalTransportError) as cm:
                        load_local_inbox(transport_dir=temp_dir, node_id="node-a")
                    self.assertEqual(str(cm.exception), expected_message)

    def test_write_local_inbox_error_messages_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            duplicate = _message("msg-0001", "node-a")
            wrong_recipient = _message("msg-0002", "node-b")
            cases = [
                ({"node_id": "", "messages": []}, "node_id must be a non-empty string"),
                (
                    {"node_id": "node-a", "messages": [duplicate, duplicate]},
                    "duplicate message_id in local transport inbox: msg-0001",
                ),
                (
                    {"node_id": "node-a", "messages": [wrong_recipient]},
                    "local transport inbox entry 0 recipient_node_id must be node-a",
                ),
            ]
            for kwargs, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    with self.assertRaises(LocalTransportError) as cm:
                        write_local_inbox(transport_dir=temp_dir, **kwargs)
                    self.assertEqual(str(cm.exception), expected_message)

    def test_outbox_path_quotes_node_ids_and_writes_document_shape(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            node_id = "node/a b?c"
            source_inbox_path = local_inbox_path(temp_dir, node_id)
            message = _emitted_message("msg-result", node_id)

            path = write_local_outbox(
                transport_dir=temp_dir,
                node_id=node_id,
                source_inbox_path=source_inbox_path,
                processed_assignment_count=1,
                messages=[message],
            )
            document = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(path.name, "node%2Fa%20b%3Fc.json")
        self.assertEqual(path.parent.name, "outboxes")
        self.assertEqual(
            str(local_outbox_path(temp_dir, "node-._~")),
            str(Path(temp_dir) / "outboxes" / "node-._~.json"),
        )
        self.assertEqual(
            document,
            {
                "version": 1,
                "node_id": node_id,
                "source_inbox_path": str(source_inbox_path),
                "processed_assignment_count": 1,
                "messages": [message.to_dict()],
            },
        )

    def test_write_local_outbox_rejects_malformed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            duplicate = _emitted_message("msg-0001", "node-a")
            wrong_sender = _emitted_message("msg-0002", "node-b")
            cases = [
                ({"node_id": "", "messages": []}, "node_id must be a non-empty string"),
                (
                    {"node_id": "node-a", "messages": [duplicate, duplicate]},
                    "duplicate message_id in local transport outbox: msg-0001",
                ),
                (
                    {"node_id": "node-a", "messages": [wrong_sender]},
                    "local transport outbox entry 0 sender_node_id must be node-a",
                ),
            ]
            for kwargs, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    with self.assertRaises(LocalTransportError) as cm:
                        write_local_outbox(
                            transport_dir=temp_dir,
                            source_inbox_path="inbox.json",
                            processed_assignment_count=0,
                            **kwargs,
                        )
                    self.assertEqual(str(cm.exception), expected_message)

    def test_write_local_outbox_failed_write_preserves_existing_file(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            node_id = "node-a"
            path = local_outbox_path(temp_dir, node_id)
            path.parent.mkdir(parents=True)
            original = json.dumps({"version": 1, "keep": True})
            path.write_text(original, encoding="utf-8")
            message = _emitted_message("msg-0001", node_id)
            original_writer = local_transport.atomic_write_json
            local_transport.atomic_write_json = _failing_atomic_write_json
            try:
                with self.assertRaisesRegex(LocalTransportError, "could not write"):
                    write_local_outbox(
                        transport_dir=temp_dir,
                        node_id=node_id,
                        source_inbox_path="inbox.json",
                        processed_assignment_count=1,
                        messages=[message],
                    )
            finally:
                local_transport.atomic_write_json = original_writer
            preserved = path.read_text(encoding="utf-8")

        self.assertEqual(preserved, original)

    def test_collect_local_outboxes_writes_deterministic_message_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport_dir = Path(temp_dir) / "transport"
            output_path = Path(temp_dir) / "collected.json"
            node_b_path = write_local_outbox(
                transport_dir=transport_dir,
                node_id="node-b",
                source_inbox_path="inbox-b.json",
                processed_assignment_count=1,
                messages=[_emitted_message("msg-b-1", "node-b")],
            )
            node_a_path = write_local_outbox(
                transport_dir=transport_dir,
                node_id="node-a",
                source_inbox_path="inbox-a.json",
                processed_assignment_count=2,
                messages=[
                    _emitted_message("msg-a-1", "node-a"),
                    _emitted_message("msg-a-2", "node-a"),
                ],
            )
            before_a = node_a_path.read_text(encoding="utf-8")
            before_b = node_b_path.read_text(encoding="utf-8")

            result = collect_local_outboxes(
                transport_dir=transport_dir, message_log_path=output_path
            )
            collected = json.loads(output_path.read_text(encoding="utf-8"))
            after_a = node_a_path.read_text(encoding="utf-8")
            after_b = node_b_path.read_text(encoding="utf-8")

        self.assertEqual(
            result,
            {
                "source": "collect-local-outboxes",
                "transport_dir": str(transport_dir),
                "output_path": str(output_path),
                "outbox_count": 2,
                "node_ids": ["node-a", "node-b"],
                "message_count": 3,
            },
        )
        self.assertEqual(collected["version"], 1)
        self.assertEqual(collected["metadata"], result)
        self.assertEqual(
            [message["message_id"] for message in collected["messages"]],
            ["msg-a-1", "msg-a-2", "msg-b-1"],
        )
        self.assertEqual(after_a, before_a)
        self.assertEqual(after_b, before_b)

    def test_collect_local_outboxes_empty_cases_write_empty_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            cases = [
                Path(temp_dir) / "missing-transport",
                Path(temp_dir) / "empty-transport",
                Path(temp_dir) / "transport-without-outboxes",
                Path(temp_dir) / "transport-with-empty-outboxes",
            ]
            cases[1].mkdir()
            (cases[2] / "inboxes").mkdir(parents=True)
            (cases[3] / "outboxes").mkdir(parents=True)
            for index, transport_dir in enumerate(cases):
                output_path = Path(temp_dir) / f"empty-{index}.json"
                result = collect_local_outboxes(
                    transport_dir=transport_dir, message_log_path=output_path
                )
                collected = json.loads(output_path.read_text(encoding="utf-8"))

                self.assertEqual(result["outbox_count"], 0)
                self.assertEqual(result["node_ids"], [])
                self.assertEqual(result["message_count"], 0)
                self.assertEqual(collected["version"], 1)
                self.assertEqual(collected["messages"], [])

    def test_collect_local_outboxes_rejects_malformed_outboxes_without_writing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport_dir = Path(temp_dir) / "transport"
            outbox_dir = transport_dir / "outboxes"
            outbox_dir.mkdir(parents=True)
            output_path = Path(temp_dir) / "collected.json"
            output_path.write_text("original", encoding="utf-8")
            cases = [
                ("node-a.json", "not-json", "malformed"),
                (
                    "node-a.json",
                    json.dumps({"version": 2, "node_id": "node-a", "messages": []}),
                    "version 1",
                ),
                (
                    "node-a.json",
                    json.dumps({"version": 1, "node_id": "node-a", "messages": {}}),
                    "messages' must be a list",
                ),
                (
                    "node-a.json",
                    json.dumps(
                        {
                            "version": 1,
                            "node_id": "node-a",
                            "messages": [{"message_type": "job_result_reported"}],
                        }
                    ),
                    "entry 0 is invalid",
                ),
                (
                    "node-a.json",
                    json.dumps(
                        {
                            "version": 1,
                            "node_id": "node-a",
                            "messages": [_emitted_message("msg-1", "node-b").to_dict()],
                        }
                    ),
                    "sender_node_id must be node-a",
                ),
                (
                    "node%2Da.json",
                    json.dumps({"version": 1, "node_id": "node-a", "messages": []}),
                    "filename is not a safe node id",
                ),
            ]
            for filename, contents, expected in cases:
                with self.subTest(expected=expected):
                    for path in outbox_dir.glob("*.json"):
                        path.unlink()
                    (outbox_dir / filename).write_text(contents, encoding="utf-8")
                    with self.assertRaisesRegex(LocalTransportError, expected):
                        collect_local_outboxes(
                            transport_dir=transport_dir, message_log_path=output_path
                        )
                    self.assertEqual(
                        output_path.read_text(encoding="utf-8"), "original"
                    )


def _message(message_id: str, recipient: str) -> MeshMessage:
    return MeshMessage(
        message_id=message_id,
        message_type="job_assigned",
        sender_node_id="scheduler",
        recipient_node_id=recipient,
        payload={
            "job_id": message_id,
            "job_type": "echo",
            "payload": {"message": message_id},
        },
        correlation_id=message_id,
    )


def _emitted_message(message_id: str, sender: str) -> MeshMessage:
    return MeshMessage(
        message_id=message_id,
        message_type="job_result_reported",
        sender_node_id=sender,
        recipient_node_id="scheduler",
        payload={"job_id": message_id, "status": "completed"},
        correlation_id=message_id,
    )


def _failing_atomic_write_json(path: Path, document: dict[str, object]) -> None:
    raise OSError("simulated write failure")


def _message_entry(
    message_id: str,
    sender: str,
    recipient: str | None,
    message_type: str = "job_assigned",
) -> dict[str, object]:
    payload = (
        {"node_id": sender}
        if message_type == "node_heartbeat"
        else {
            "job_id": message_id,
            "job_type": "echo",
            "payload": {"message": message_id},
        }
    )
    return {
        "message_id": message_id,
        "message_type": message_type,
        "sender_node_id": sender,
        "recipient_node_id": recipient,
        "payload": payload,
        "correlation_id": None if message_type == "node_heartbeat" else message_id,
    }


if __name__ == "__main__":
    unittest.main()
