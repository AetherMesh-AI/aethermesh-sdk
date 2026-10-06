import unittest
from unittest.mock import patch

from aethermesh_core.ledger import ContributionLedger
from aethermesh_core.message_bus import LocalMessageBus
from aethermesh_core.messages import MeshMessage
from aethermesh_core.models import JobResult, NodeIdentity
from aethermesh_core.node_service import LocalNodeService
from aethermesh_core.result_hash import result_hash
from aethermesh_core.runner import LocalRunner


class LocalNodeServiceTests(unittest.TestCase):
    def test_successful_echo_assignment_is_processed_from_inbox(self) -> None:
        service, bus, ledger = _service("node-a")
        assignment = _assignment(
            message_id="msg-0001",
            sender_node_id="local-scheduler",
            recipient_node_id="node-a",
            payload={
                "job_id": "echo-1",
                "job_type": "echo",
                "payload": {"message": "hello mesh"},
                "manifest_ref": "manifests/local-batch.json",
            },
        )
        bus.send(assignment)

        result = service.process_inbox()

        self.assertEqual(result.node_id, "node-a")
        self.assertEqual(result.ignored_message_ids, [])
        self.assertEqual(len(result.processed), 1)
        processed = result.processed[0]
        self.assertEqual(processed.message_id, "msg-0001")
        self.assertEqual(processed.job.job_id, "echo-1")
        self.assertEqual(processed.result.status, "completed")
        self.assertEqual(processed.result.output, "hello mesh")
        self.assertTrue(processed.validation.valid)
        self.assertEqual(processed.contribution_record.contribution_units, 1)
        self.assertEqual(
            processed.contribution_record.manifest_ref,
            "manifests/local-batch.json",
        )
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 1)
        self.assertEqual(
            [message.message_type for message in processed.emitted_messages],
            ["job_result_reported", "job_validated", "contribution_recorded"],
        )
        self.assertEqual(
            [message.correlation_id for message in processed.emitted_messages],
            ["echo-1", "echo-1", "echo-1"],
        )
        self.assertEqual(
            processed.emitted_messages[0].payload,
            {
                "job_id": "echo-1",
                "status": "completed",
                "success": True,
                "output": "hello mesh",
                "error": None,
                "contribution_units": 1,
                "result_hash": result_hash(processed.result),
            },
        )
        self.assertEqual(
            processed.contribution_record.result_hash, result_hash(processed.result)
        )
        self.assertEqual(
            processed.contribution_record.version_metadata_ref,
            service.version_metadata_ref,
        )
        self.assertEqual(
            processed.emitted_messages[1].payload,
            {
                "job_id": "echo-1",
                "node_id": "node-a",
                "assignment_message_id": "msg-0001",
                "result_message_id": "msg-0002",
                "validator_id": "node-a",
                "valid": True,
                "reason": "ok",
                "contribution_units_after_validation": 1,
                "version_metadata_ref": service.version_metadata_ref,
            },
        )
        self.assertEqual(
            processed.emitted_messages[2].payload,
            {
                "job_id": "echo-1",
                "node_id": "node-a",
                "status": "completed",
                "valid": True,
                "validation": "ok",
                "contribution_units": 1,
                "version_metadata_ref": service.version_metadata_ref,
            },
        )
        self.assertEqual(processed.contribution_record.validation_valid, True)
        self.assertEqual(processed.contribution_record.validation_reason, "ok")
        self.assertEqual(processed.contribution_record.job_type, "echo")

    def test_successful_text_stats_assignment_is_processed_from_inbox(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "text-stats-1",
                    "job_type": "text_stats",
                    "payload": {"text": "hello mesh\nhello node"},
                },
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.result.status, "completed")
        self.assertEqual(
            processed.result.output,
            {
                "character_count": len("hello mesh\nhello node"),
                "word_count": 4,
                "line_count": 2,
                "normalized_preview": "hello mesh hello node",
            },
        )
        self.assertTrue(processed.validation.valid)
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 2)

    def test_successful_text_embed_assignment_is_processed_from_inbox(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "text-embed-1",
                    "job_type": "text_embed",
                    "payload": {"text": "alpha beta beta", "dimensions": 4},
                },
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.result.status, "completed")
        self.assertEqual(
            processed.result.output,
            {
                "dimensions": 4,
                "token_count": 3,
                "unique_terms": 2,
                "vector": [0, 2, 1, 0],
            },
        )
        self.assertTrue(processed.validation.valid)
        self.assertEqual(processed.contribution_record.contribution_units, 3)
        self.assertEqual(processed.contribution_record.job_type, "text_embed")
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 3)

    def test_successful_text_retrieve_assignment_is_processed_from_inbox(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "text-retrieve-1",
                    "job_type": "text_retrieve",
                    "payload": {
                        "query": "alpha beta",
                        "documents": [
                            {"id": "doc-b", "text": "alpha only"},
                            {"id": "doc-a", "text": "alpha beta"},
                        ],
                    },
                },
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.result.status, "completed")
        self.assertEqual(
            processed.result.output,
            {
                "query_terms": ["alpha", "beta"],
                "matches": [
                    {
                        "id": "doc-a",
                        "score": 1.0,
                        "matched_term_count": 2,
                        "matched_terms": ["alpha", "beta"],
                    },
                    {
                        "id": "doc-b",
                        "score": 0.5,
                        "matched_term_count": 1,
                        "matched_terms": ["alpha"],
                    },
                ],
            },
        )
        self.assertTrue(processed.validation.valid)
        self.assertEqual(processed.contribution_record.contribution_units, 1)
        self.assertEqual(processed.contribution_record.job_type, "text_retrieve")
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 1)

    def test_malformed_assignment_is_failed_invalid_and_zero_contribution(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={"payload": {"message": "missing metadata"}},
                correlation_id="assignment-1",
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.job.job_id, "assignment-1")
        self.assertEqual(processed.job.job_type, "__malformed_assignment__")
        self.assertEqual(processed.result.status, "failed")
        self.assertFalse(processed.validation.valid)
        self.assertEqual(processed.validation.reason, "unsupported_job_type")
        self.assertEqual(processed.contribution_record.contribution_units, 0)
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 0)
        self.assertEqual(
            processed.emitted_messages[2].payload["contribution_units"],
            0,
        )

    def test_unsupported_job_type_is_reported_with_zero_contribution(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "job-unsupported",
                    "job_type": "render_frame",
                    "payload": {"message": "not supported locally"},
                },
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.result.status, "failed")
        self.assertEqual(processed.result.error, "Unsupported job type: render_frame")
        self.assertFalse(processed.validation.valid)
        self.assertEqual(processed.validation.reason, "unsupported_job_type")
        self.assertEqual(processed.contribution_record.contribution_units, 0)
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 0)

    def test_assignment_for_another_node_is_not_executed_by_this_node(self) -> None:
        service, bus, ledger = _service("node-a", extra_node_ids=["node-b"])
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-b",
                payload={
                    "job_id": "echo-1",
                    "job_type": "echo",
                    "payload": {"message": "hello other node"},
                },
            )
        )
        bus.send(
            _assignment(
                message_id="msg-0002",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "echo-2",
                    "job_type": "echo",
                    "payload": {"message": "hello node a"},
                },
            )
        )

        result = service.process_inbox()

        self.assertEqual(
            [processed.message_id for processed in result.processed], ["msg-0002"]
        )
        self.assertEqual(result.ignored_message_ids, [])
        self.assertEqual(ledger.summary_for_node("node-a").total_result_count, 1)
        self.assertEqual(
            [message.message_id for message in bus.log()],
            ["msg-0001", "msg-0002", "msg-0003", "msg-0004", "msg-0005"],
        )

    def test_repeated_process_inbox_does_not_double_record_contribution(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "echo-1",
                    "job_type": "echo",
                    "payload": {"message": "hello once"},
                },
            )
        )

        first = service.process_inbox()
        second = service.process_inbox()

        self.assertEqual(len(first.processed), 1)
        self.assertEqual(second.processed, [])
        self.assertIn("msg-0001", second.ignored_message_ids)
        summary = ledger.summary_for_node("node-a")
        self.assertEqual(summary.total_result_count, 1)
        self.assertEqual(summary.total_contribution_units, 1)
        self.assertEqual(
            [message.message_id for message in bus.log()],
            ["msg-0001", "msg-0002", "msg-0003", "msg-0004"],
        )

    def test_seeded_processed_message_ids_skip_assignment_without_contribution(
        self,
    ) -> None:
        service, bus, ledger = _service("node-a", processed_message_ids=["msg-0001"])
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "echo-1",
                    "job_type": "echo",
                    "payload": {"message": "already done"},
                },
            )
        )
        bus.send(
            _assignment(
                message_id="msg-0002",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "echo-2",
                    "job_type": "echo",
                    "payload": {"message": "new work"},
                },
            )
        )

        result = service.process_inbox()

        self.assertEqual(
            [assignment.message_id for assignment in result.processed], ["msg-0002"]
        )
        self.assertEqual(result.ignored_message_ids, ["msg-0001"])
        self.assertEqual(result.skipped_processed_message_ids, ["msg-0001"])
        self.assertEqual(result.processed_message_ids, ["msg-0001", "msg-0002"])
        summary = ledger.summary_for_node("node-a")
        self.assertEqual(summary.total_result_count, 1)
        self.assertEqual(summary.total_contribution_units, 1)

    def test_assignment_payload_fallbacks_are_explicit(self) -> None:
        service, bus, _ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={"payload": "not-object"},
                correlation_id=None,
            )
        )

        result = service.process_inbox()
        processed = result.processed[0]

        self.assertEqual(processed.job.job_id, "malformed-msg-0001")
        self.assertEqual(processed.job.job_type, "__malformed_assignment__")
        self.assertEqual(processed.job.payload, {})
        self.assertEqual(
            [message.correlation_id for message in processed.emitted_messages],
            ["malformed-msg-0001", "malformed-msg-0001", "malformed-msg-0001"],
        )

    def test_contribution_is_recorded_only_after_validation(self) -> None:
        service, bus, ledger = _service("node-a")
        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "echo-1",
                    "job_type": "echo",
                    "payload": {},
                },
            )
        )

        result = service.process_inbox()

        processed = result.processed[0]
        self.assertEqual(processed.result.status, "completed")
        self.assertFalse(processed.validation.valid)
        self.assertEqual(processed.validation.reason, "missing_payload_message")
        self.assertEqual(processed.contribution_record.status, "completed")
        self.assertEqual(processed.contribution_record.contribution_units, 0)
        self.assertEqual(processed.contribution_record.validation_valid, False)
        self.assertEqual(
            processed.contribution_record.validation_reason, "missing_payload_message"
        )
        self.assertEqual(processed.contribution_record.job_type, "echo")
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 0)
        self.assertEqual(processed.emitted_messages[2].payload["valid"], False)
        self.assertEqual(
            processed.emitted_messages[2].payload["validation"],
            "missing_payload_message",
        )

    def test_schema_rejection_has_no_accepted_contribution(self) -> None:
        service, bus, ledger = _service("node-a")

        def malformed_result(job: object) -> JobResult:
            job_id = job.job_id
            return JobResult(
                job_id=job_id,
                node_id="node-a",
                status="completed",
                output={"character_count": "wrong"},
                error=None,
                contribution_units=1,
            )

        bus.send(
            _assignment(
                message_id="msg-0001",
                sender_node_id="local-scheduler",
                recipient_node_id="node-a",
                payload={
                    "job_id": "text-stats-1",
                    "job_type": "text_stats",
                    "payload": {"text": "hello"},
                },
            )
        )
        with patch.object(service.runner, "run", side_effect=malformed_result):
            processed = service.process_inbox().processed[0]

        self.assertFalse(processed.validation.valid)
        self.assertEqual(
            processed.validation.reason,
            "output_schema.v1.text_stats.output: missing required field line_count",
        )
        self.assertEqual(processed.contribution_record.contribution_units, 0)
        self.assertFalse(processed.contribution_record.validation_valid)
        self.assertEqual(ledger.summary_for_node("node-a").total_contribution_units, 0)
        self.assertFalse(processed.emitted_messages[1].payload["valid"])
        self.assertEqual(processed.emitted_messages[2].payload["contribution_units"], 0)


def _service(
    node_id: str,
    *,
    extra_node_ids: list[str] | None = None,
    processed_message_ids: list[str] | None = None,
) -> tuple[LocalNodeService, LocalMessageBus, ContributionLedger]:
    identity = NodeIdentity(node_id=node_id)
    bus = LocalMessageBus()
    bus.register_node("local-scheduler")
    bus.register_node("local-ledger")
    bus.register_node(node_id)
    for extra_node_id in extra_node_ids or []:
        bus.register_node(extra_node_id)
    ledger = ContributionLedger()
    service = LocalNodeService(
        identity=identity,
        message_bus=bus,
        runner=LocalRunner(identity),
        ledger=ledger,
        processed_message_ids=processed_message_ids,
    )
    return service, bus, ledger


def _assignment(
    *,
    message_id: str,
    sender_node_id: str,
    recipient_node_id: str,
    payload: dict[str, object],
    correlation_id: str | None = None,
) -> MeshMessage:
    job_id = payload.get("job_id")
    return MeshMessage(
        message_id=message_id,
        message_type="job_assigned",
        sender_node_id=sender_node_id,
        recipient_node_id=recipient_node_id,
        payload=payload,
        correlation_id=correlation_id or (job_id if isinstance(job_id, str) else None),
    )


if __name__ == "__main__":
    unittest.main()
