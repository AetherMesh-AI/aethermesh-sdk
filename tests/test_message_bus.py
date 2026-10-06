import unittest
from typing import Any, cast

from aethermesh_core.message_bus import LocalMessageBus
from aethermesh_core.messages import MeshMessage


class LocalMessageBusTests(unittest.TestCase):
    def test_successful_send_to_registered_recipient_preserves_message(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-a")
        bus.register_node("node-b")
        message = _message(
            message_id="msg-0001",
            sender_node_id="node-a",
            recipient_node_id="node-b",
        )

        sent = bus.send(message)

        self.assertIs(sent, message)
        self.assertEqual(bus.log(), [message])
        self.assertEqual(bus.inbox_for("node-b"), [message])

    def test_log_preserves_send_order_over_multiple_messages(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-a")
        bus.register_node("node-b")
        first = _message(
            message_id="msg-0001",
            sender_node_id="node-a",
            recipient_node_id="node-b",
        )
        second = _message(
            message_id="msg-0002",
            sender_node_id="node-b",
            recipient_node_id="node-a",
        )

        bus.send(first)
        bus.send(second)

        self.assertEqual(
            [message.message_id for message in bus.log()], ["msg-0001", "msg-0002"]
        )

    def test_inbox_filters_by_recipient_deterministically(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-a")
        bus.register_node("node-b")
        bus.register_node("node-c")
        to_b = _message(
            message_id="msg-0001",
            sender_node_id="node-a",
            recipient_node_id="node-b",
        )
        to_c = _message(
            message_id="msg-0002",
            sender_node_id="node-a",
            recipient_node_id="node-c",
        )
        broadcast = _message(
            message_id="msg-0003",
            sender_node_id="node-a",
            recipient_node_id=None,
        )

        bus.send(to_b)
        bus.send(to_c)
        bus.send(broadcast)

        self.assertEqual(bus.log(), [to_b, to_c, broadcast])
        self.assertEqual(bus.inbox_for("node-b"), [to_b])
        self.assertEqual(bus.inbox_for("node-c"), [to_c])

    def test_register_node_rejects_empty_non_string_and_duplicate_ids(self) -> None:
        bus = LocalMessageBus()

        for node_id in ["", None, 123]:
            with (
                self.subTest(node_id=node_id),
                self.assertRaisesRegex(
                    ValueError, "node_id must be a non-empty string"
                ),
            ):
                bus.register_node(cast(Any, node_id))

        bus.register_node("node-a")
        with self.assertRaisesRegex(ValueError, "already registered: node-a"):
            bus.register_node("node-a")

    def test_register_node_empty_id_error_message_is_exact(self) -> None:
        bus = LocalMessageBus()

        with self.assertRaises(ValueError) as cm:
            bus.register_node("")

        self.assertEqual(str(cm.exception), "node_id must be a non-empty string")

    def test_send_rejects_unregistered_sender(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-b")

        with self.assertRaisesRegex(
            ValueError, "sender_node_id is not registered: node-a"
        ):
            bus.send(
                _message(
                    message_id="msg-0001",
                    sender_node_id="node-a",
                    recipient_node_id="node-b",
                )
            )

    def test_send_rejects_unregistered_recipient(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-a")

        with self.assertRaisesRegex(
            ValueError, "recipient_node_id is not registered: node-b"
        ):
            bus.send(
                _message(
                    message_id="msg-0001",
                    sender_node_id="node-a",
                    recipient_node_id="node-b",
                )
            )

    def test_log_and_inbox_return_copies(self) -> None:
        bus = LocalMessageBus()
        bus.register_node("node-a")
        bus.register_node("node-b")
        message = _message(
            message_id="msg-0001",
            sender_node_id="node-a",
            recipient_node_id="node-b",
        )
        bus.send(message)

        bus.log().clear()
        bus.inbox_for("node-b").clear()

        self.assertEqual(bus.log(), [message])
        self.assertEqual(bus.inbox_for("node-b"), [message])

    def test_inbox_rejects_unregistered_node(self) -> None:
        bus = LocalMessageBus()

        with self.assertRaisesRegex(ValueError, "node_id is not registered: node-a"):
            bus.inbox_for("node-a")


def _message(
    *,
    message_id: str,
    sender_node_id: str,
    recipient_node_id: str | None,
) -> MeshMessage:
    return MeshMessage(
        message_id=message_id,
        message_type="job_assigned",
        sender_node_id=sender_node_id,
        recipient_node_id=recipient_node_id,
        payload={"job_id": "echo-1", "job_type": "echo", "node_id": "node-b"},
        correlation_id="echo-1",
    )


if __name__ == "__main__":
    unittest.main()
