import unittest
from typing import Any, cast

from aethermesh_core.messages import (
    SUPPORTED_MESSAGE_TYPES,
    MeshMessage,
    message_from_mapping,
)


class MeshMessageTests(unittest.TestCase):
    def test_message_serializes_to_deterministic_json_compatible_dict(self) -> None:
        message = MeshMessage(
            message_id="msg-0001",
            message_type="job_assigned",
            sender_node_id="local-scheduler",
            recipient_node_id="node-a",
            payload={"job_id": "echo-1", "job_type": "echo"},
            correlation_id="echo-1",
        )

        self.assertEqual(
            message.to_dict(),
            {
                "message_id": "msg-0001",
                "message_type": "job_assigned",
                "sender_node_id": "local-scheduler",
                "recipient_node_id": "node-a",
                "payload": {"job_id": "echo-1", "job_type": "echo"},
                "correlation_id": "echo-1",
            },
        )

    def test_supported_message_types_are_limited_to_initial_local_mesh_types(
        self,
    ) -> None:
        self.assertEqual(
            SUPPORTED_MESSAGE_TYPES,
            frozenset(
                {
                    "node_heartbeat",
                    "job_assigned",
                    "job_result_reported",
                    "job_validated",
                    "contribution_recorded",
                }
            ),
        )

    def test_node_heartbeat_message_type_is_accepted(self) -> None:
        message = MeshMessage(
            message_id="msg-0001",
            message_type="node_heartbeat",
            sender_node_id="node-a",
            recipient_node_id=None,
            payload={
                "node_id": "node-a",
                "status": "available",
                "heartbeat_sequence": 1,
                "heartbeat_count": 1,
            },
            correlation_id=None,
        )

        self.assertEqual(message.message_type, "node_heartbeat")

    def test_unsupported_message_type_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "message_type must be one of"):
            MeshMessage(
                message_id="msg-0001",
                message_type="peer_discovered",
                sender_node_id="node-a",
                recipient_node_id=None,
                payload={},
                correlation_id=None,
            )

    def test_required_string_fields_must_be_non_empty(self) -> None:
        invalid_messages = [
            {
                "message_id": "",
                "message_type": "job_assigned",
                "sender_node_id": "node-a",
            },
            {"message_id": "msg-0001", "message_type": "", "sender_node_id": "node-a"},
            {
                "message_id": "msg-0001",
                "message_type": "job_assigned",
                "sender_node_id": "",
            },
            {
                "message_id": "msg-0001",
                "message_type": "job_assigned",
                "sender_node_id": "node-a",
                "recipient_node_id": "",
            },
            {
                "message_id": "msg-0001",
                "message_type": "job_assigned",
                "sender_node_id": "node-a",
                "correlation_id": "",
            },
        ]

        for invalid_kwargs in invalid_messages:
            with self.subTest(kwargs=invalid_kwargs):
                kwargs = dict(invalid_kwargs)
                with self.assertRaises(ValueError):
                    MeshMessage(
                        recipient_node_id=kwargs.pop("recipient_node_id", None),
                        payload={},
                        correlation_id=kwargs.pop("correlation_id", None),
                        **kwargs,
                    )

    def test_payload_must_be_a_dictionary(self) -> None:
        with self.assertRaisesRegex(ValueError, "payload must be a dictionary"):
            MeshMessage(
                message_id="msg-0001",
                message_type="job_assigned",
                sender_node_id="node-a",
                recipient_node_id=None,
                payload=cast(Any, []),
                correlation_id=None,
            )

    def test_message_from_mapping_defaults_optional_fields_and_preserves_payload(
        self,
    ) -> None:
        message = message_from_mapping(
            {
                "message_id": "msg-0001",
                "message_type": "job_assigned",
                "sender_node_id": "scheduler",
                "recipient_node_id": None,
                "payload": {"job_id": "echo-1", "nested": [1, True, None]},
            }
        )

        self.assertEqual(message.message_id, "msg-0001")
        self.assertEqual(message.message_type, "job_assigned")
        self.assertEqual(message.sender_node_id, "scheduler")
        self.assertIsNone(message.recipient_node_id)
        self.assertIsNone(message.correlation_id)
        self.assertEqual(
            message.payload, {"job_id": "echo-1", "nested": [1, True, None]}
        )

    def test_message_from_mapping_rejects_malformed_entries(self) -> None:
        cases: list[tuple[object, str]] = [
            ([], "must be an object"),
            (
                {"message_type": "job_assigned", "sender_node_id": "node-a"},
                "message_id",
            ),
            (
                {
                    "message_id": "",
                    "message_type": "job_assigned",
                    "sender_node_id": "node-a",
                },
                "message_id",
            ),
            ({"message_id": "msg-1", "sender_node_id": "node-a"}, "message_type"),
            (
                {"message_id": "msg-1", "message_type": "", "sender_node_id": "node-a"},
                "message_type",
            ),
            ({"message_id": "msg-1", "message_type": "job_assigned"}, "sender_node_id"),
            (
                {
                    "message_id": "msg-1",
                    "message_type": "job_assigned",
                    "sender_node_id": "",
                },
                "sender_node_id",
            ),
            (
                {
                    "message_id": "msg-1",
                    "message_type": "job_assigned",
                    "sender_node_id": "node-a",
                    "recipient_node_id": "",
                },
                "recipient_node_id",
            ),
            (
                {
                    "message_id": "msg-1",
                    "message_type": "job_assigned",
                    "sender_node_id": "node-a",
                    "payload": [],
                },
                "payload must be a dictionary",
            ),
            (
                {
                    "message_id": "msg-1",
                    "message_type": "job_assigned",
                    "sender_node_id": "node-a",
                    "payload": {1: "not-json-key"},
                },
                "payload keys must be strings",
            ),
            (
                {
                    "message_id": "msg-1",
                    "message_type": "job_assigned",
                    "sender_node_id": "node-a",
                    "correlation_id": "",
                },
                "correlation_id",
            ),
        ]
        for entry, message in cases:
            with self.subTest(entry=entry), self.assertRaisesRegex(ValueError, message):
                message_from_mapping(entry)


if __name__ == "__main__":
    unittest.main()
