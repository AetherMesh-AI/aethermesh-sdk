import unittest

from aethermesh_core.node_registry import NodeRegistry
from aethermesh_core.scheduler import NodeStatus, ScheduledNode


class NodeRegistryTests(unittest.TestCase):
    def test_registers_string_node_ids_in_order(self) -> None:
        registry = NodeRegistry()

        registry.register("node-a")
        registry.register("node-b")

        self.assertEqual(
            registry.scheduled_nodes(),
            [
                ScheduledNode("node-a", NodeStatus.AVAILABLE),
                ScheduledNode("node-b", NodeStatus.AVAILABLE),
            ],
        )
        self.assertEqual(
            registry.to_roster(),
            [
                {
                    "node_id": "node-a",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 0,
                    "heartbeat_count": 0,
                },
                {
                    "node_id": "node-b",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 0,
                    "heartbeat_count": 0,
                },
            ],
        )

    def test_registers_scheduled_nodes_preserving_status(self) -> None:
        registry = NodeRegistry()

        registry.register(ScheduledNode("node-a", NodeStatus.OFFLINE, ("echo",)))
        registry.register(
            ScheduledNode("node-b", NodeStatus.AVAILABLE, ("text_stats",))
        )

        self.assertEqual(
            registry.scheduled_nodes(),
            [
                ScheduledNode("node-a", NodeStatus.OFFLINE, ("echo",)),
                ScheduledNode("node-b", NodeStatus.AVAILABLE, ("text_stats",)),
            ],
        )
        self.assertEqual(
            [entry["status"] for entry in registry.to_roster()],
            ["offline", "available"],
        )
        self.assertEqual(
            [entry["capabilities"] for entry in registry.to_roster()],
            [["echo"], ["text_stats"]],
        )

    def test_rejects_empty_or_whitespace_node_ids(self) -> None:
        registry = NodeRegistry()

        for node_id in ("", "   "):
            with (
                self.subTest(node_id=repr(node_id)),
                self.assertRaisesRegex(
                    ValueError, "node_id must be a non-empty string"
                ),
            ):
                registry.register(node_id)

    def test_rejects_duplicate_node_ids(self) -> None:
        registry = NodeRegistry()
        registry.register("node-a")

        with self.assertRaisesRegex(ValueError, "duplicate node_id: node-a"):
            registry.register("node-a")

    def test_marks_known_nodes_available_and_offline(self) -> None:
        registry = NodeRegistry()
        registry.register("node-a")

        registry.mark_offline("node-a")
        self.assertEqual(registry.scheduled_nodes()[0].status, NodeStatus.OFFLINE)
        self.assertEqual(registry.to_roster()[0]["status"], "offline")

        registry.mark_available("node-a")
        self.assertEqual(registry.scheduled_nodes()[0].status, NodeStatus.AVAILABLE)
        self.assertEqual(registry.to_roster()[0]["status"], "available")

    def test_rejects_status_or_heartbeat_updates_for_unknown_nodes(self) -> None:
        registry = NodeRegistry()

        with self.assertRaisesRegex(KeyError, "unknown node_id: missing-node"):
            registry.mark_available("missing-node")
        with self.assertRaisesRegex(KeyError, "unknown node_id: missing-node"):
            registry.mark_offline("missing-node")
        with self.assertRaisesRegex(KeyError, "unknown node_id: missing-node"):
            registry.record_heartbeat("missing-node")

    def test_records_deterministic_heartbeat_sequence_and_count(self) -> None:
        registry = NodeRegistry()
        registry.register("node-a")
        registry.register("node-b")

        self.assertEqual(registry.record_heartbeat("node-a"), 1)
        self.assertEqual(registry.record_heartbeat("node-b"), 2)
        self.assertEqual(registry.record_heartbeat("node-a"), 3)

        self.assertEqual(
            registry.to_roster(),
            [
                {
                    "node_id": "node-a",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 3,
                    "heartbeat_count": 2,
                },
                {
                    "node_id": "node-b",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 2,
                    "heartbeat_count": 1,
                },
            ],
        )


if __name__ == "__main__":
    unittest.main()
