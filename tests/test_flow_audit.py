import json
import tempfile
import unittest
from collections.abc import Callable
from pathlib import Path

from aethermesh_core.cli import run_local_flow
from aethermesh_core.flow_audit import (
    FlowAuditError,
    _assert_assignment_matches_receipt,
    _assert_contribution_message_matches_receipt,
    _assert_result_message_matches_receipt,
    _assert_validation_message_matches_receipt,
    audit_local_flow,
)
from aethermesh_core.messages import MeshMessage


class FlowAuditTests(unittest.TestCase):
    def test_audit_local_flow_reports_deterministic_artifact_counts(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))

            report = audit_local_flow(output_dir)

        self.assertEqual(
            report,
            {
                "ok": True,
                "output_dir": str(output_dir),
                "artifacts": {
                    "dispatch_message_log": str(
                        output_dir / "dispatch-message-log.json"
                    ),
                    "flow_message_log": str(output_dir / "flow-message-log.json"),
                    "ledger": str(output_dir / "ledger.json"),
                    "receipts": str(output_dir / "receipts.json"),
                },
                "counts": {
                    "dispatch_messages": 4,
                    "flow_messages": 10,
                    "receipts": 2,
                    "ledger_records": 2,
                    "total_contribution_units": 3,
                    "credited_receipt_units": 3,
                },
            },
        )

    def test_missing_receipts_returns_audit_error_without_mutating_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            receipts_path = output_dir / "receipts.json"
            receipts_path.unlink()
            before = _artifact_contents(output_dir)

            with self.assertRaisesRegex(FlowAuditError, "receipt file does not exist"):
                audit_local_flow(output_dir)

            after = _artifact_contents(output_dir)

        self.assertEqual(after, before)

    def test_mismatched_receipt_units_returns_audit_error_without_mutating_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            receipts_path = output_dir / "receipts.json"
            receipts = json.loads(receipts_path.read_text(encoding="utf-8"))
            receipts["receipts"][0]["credited_units"] = 99
            receipts_path.write_text(
                json.dumps(receipts, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            before = _artifact_contents(output_dir)

            with self.assertRaisesRegex(
                FlowAuditError,
                "validation.contribution_units_after_validation mismatch",
            ):
                audit_local_flow(output_dir)

            after = _artifact_contents(output_dir)

        self.assertEqual(after, before)

    def test_tampered_receipt_validation_reason_returns_audit_error_without_mutating_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            receipts_path = output_dir / "receipts.json"
            receipts = json.loads(receipts_path.read_text(encoding="utf-8"))
            receipts["receipts"][0]["validation"]["reason"] = "tampered reason"
            receipts_path.write_text(
                json.dumps(receipts, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            before = _artifact_contents(output_dir)

            with self.assertRaisesRegex(FlowAuditError, "validation.reason mismatch"):
                audit_local_flow(output_dir)

            after = _artifact_contents(output_dir)

        self.assertEqual(after, before)

    def test_tampered_receipt_output_summary_returns_audit_error_without_mutating_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            receipts_path = output_dir / "receipts.json"
            receipts = json.loads(receipts_path.read_text(encoding="utf-8"))
            receipts["receipts"][0]["output_summary"] = {"tampered": True}
            receipts_path.write_text(
                json.dumps(receipts, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            before = _artifact_contents(output_dir)

            with self.assertRaisesRegex(
                FlowAuditError, "result.output_summary mismatch"
            ):
                audit_local_flow(output_dir)

            after = _artifact_contents(output_dir)

        self.assertEqual(after, before)

    def test_tampered_ledger_record_returns_audit_error_without_mutating_artifacts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            ledger_path = output_dir / "ledger.json"
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))
            ledger["records"][0]["validation_reason"] = "tampered reason"
            ledger_path.write_text(
                json.dumps(ledger, indent=2, sort_keys=True) + "\n", encoding="utf-8"
            )
            before = _artifact_contents(output_dir)

            with self.assertRaisesRegex(
                FlowAuditError, "contribution claims do not match ledger records"
            ):
                audit_local_flow(output_dir)

            after = _artifact_contents(output_dir)

        self.assertEqual(after, before)

    def test_duplicate_receipt_contribution_claims_must_match_duplicate_ledger_records(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = _write_manifest(Path(temp_dir))
            output_dir = Path(temp_dir) / "flow"
            run_local_flow(str(manifest_path), str(output_dir))
            receipts_path = output_dir / "receipts.json"
            receipts = _load_json(receipts_path)
            receipt_entries = receipts["receipts"]
            assert isinstance(receipt_entries, list)
            receipt_entries.append(dict(receipt_entries[0]))
            _write_json(receipts_path, receipts)
            before = _artifact_contents(output_dir)

            with self.assertRaises(FlowAuditError) as cm:
                audit_local_flow(output_dir)
            after = _artifact_contents(output_dir)

        self.assertIn(
            "receipt contribution claims do not match ledger records", str(cm.exception)
        )
        self.assertIn("missing node=local-node-a job=echo-1", str(cm.exception))
        self.assertEqual(after, before)

    def test_cross_artifact_assertion_error_messages_name_each_field(self) -> None:
        receipt = _sample_receipt()
        context = "receipt entry 0 job=echo-1 node=local-node-a"
        cases: list[tuple[Callable[[], None], str]] = [
            (
                lambda: _assert_assignment_matches_receipt(
                    context,
                    receipt,
                    {"job_id": "other-job", "job_type": "echo"},
                ),
                "receipt entry 0 job=echo-1 node=local-node-a assignment.job_id mismatch: receipt='echo-1' artifact='other-job'",
            ),
            (
                lambda: _assert_assignment_matches_receipt(
                    context,
                    receipt,
                    {"job_id": "echo-1", "job_type": "text_stats"},
                ),
                "receipt entry 0 job=echo-1 node=local-node-a assignment.job_type mismatch: receipt='echo' artifact='text_stats'",
            ),
            (
                lambda: _assert_result_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_result_reported", payload={"job_id": "other-job"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a result.job_id mismatch: receipt='echo-1' artifact='other-job'",
            ),
            (
                lambda: _assert_result_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_result_reported", sender_node_id="local-node-b"
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a result.node_id mismatch: receipt='local-node-a' artifact='local-node-b'",
            ),
            (
                lambda: _assert_result_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_result_reported", payload={"status": "failed"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a result.status mismatch: receipt='completed' artifact='failed'",
            ),
            (
                lambda: _assert_result_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_validated"),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a result_message_id references job_validated, expected job_result_reported",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_validated", payload={"job_id": "other-job"}),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.job_id mismatch: receipt='echo-1' artifact='other-job'",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_validated", payload={"node_id": "local-node-b"}
                    ),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.node_id mismatch: receipt='local-node-a' artifact='local-node-b'",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_validated", payload={"assignment_message_id": "other"}
                    ),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.assignment_message_id mismatch: receipt='msg-0003' artifact='other'",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_validated", payload={"result_message_id": "other"}
                    ),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.result_message_id mismatch: receipt='msg-0005' artifact='other'",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_validated", payload={"valid": False}),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.valid mismatch: receipt=True artifact=False",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_validated", payload={"reason": "bad"}),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.reason mismatch: receipt='ok' artifact='bad'",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "job_validated",
                        payload={"contribution_units_after_validation": 99},
                    ),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation.contribution_units_after_validation mismatch: receipt=1 artifact=99",
            ),
            (
                lambda: _assert_validation_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_result_reported"),
                    "msg-0005",
                ),
                "receipt entry 0 job=echo-1 node=local-node-a validation_message_id references job_result_reported, expected job_validated",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "contribution_recorded", payload={"job_id": "other-job"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.job_id mismatch: receipt='echo-1' artifact='other-job'",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "contribution_recorded", payload={"node_id": "local-node-b"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.node_id mismatch: receipt='local-node-a' artifact='local-node-b'",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "contribution_recorded", payload={"status": "failed"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.status mismatch: receipt='completed' artifact='failed'",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("contribution_recorded", payload={"valid": False}),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.valid mismatch: receipt=True artifact=False",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "contribution_recorded", payload={"validation": "bad"}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.validation mismatch: receipt='ok' artifact='bad'",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message(
                        "contribution_recorded", payload={"contribution_units": 99}
                    ),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.contribution_units mismatch: receipt=1 artifact=99",
            ),
            (
                lambda: _assert_contribution_message_matches_receipt(
                    context,
                    receipt,
                    _sample_message("job_validated"),
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution_message_id references job_validated, expected contribution_recorded",
            ),
        ]
        for call, expected_message in cases:
            with self.subTest(expected_message=expected_message):
                with self.assertRaises(FlowAuditError) as cm:
                    call()
                self.assertEqual(str(cm.exception), expected_message)


FlowTamper = Callable[[Path], None]


class FlowAuditTamperTests(unittest.TestCase):
    def test_audit_local_flow_rejects_tampered_cross_artifact_fields(self) -> None:
        cases: list[tuple[FlowTamper, str]] = [
            (
                lambda output_dir: _set_receipt_field(
                    output_dir, 0, "assignment_message_id", "missing-assignment"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a assignment_message_id not found in dispatch log: missing-assignment",
            ),
            (
                lambda output_dir: _set_receipt_field(
                    output_dir, 0, "result_message_id", "missing-result"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a job_result_reported message_id not found in flow log: missing-result",
            ),
            (
                lambda output_dir: _set_receipt_field(
                    output_dir, 0, "contribution_message_id", ""
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution_message_id must be present",
            ),
            (
                lambda output_dir: _set_dispatch_assignment_payload(
                    output_dir, "msg-0003", "job_type", "text_stats"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a assignment.job_type mismatch: receipt='echo' artifact='text_stats'",
            ),
            (
                lambda output_dir: _set_flow_message_sender(
                    output_dir, "job_result_reported", "echo-1", "local-node-b"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a job_result_reported message_id not found in flow log: msg-0005",
            ),
            (
                lambda output_dir: _set_flow_message_payload(
                    output_dir, "job_result_reported", "echo-1", "job_id", "other-job"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a job_result_reported message_id not found in flow log: msg-0005",
            ),
            (
                lambda output_dir: _set_flow_message_payload(
                    output_dir, "job_result_reported", "echo-1", "status", "failed"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a result.status mismatch: receipt='completed' artifact='failed'",
            ),
            (
                lambda output_dir: _set_flow_message_payload(
                    output_dir, "contribution_recorded", "echo-1", "job_id", "other-job"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution_recorded message_id not found in flow log: msg-0007",
            ),
            (
                lambda output_dir: _set_flow_message_payload(
                    output_dir, "contribution_recorded", "echo-1", "status", "failed"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.status mismatch: receipt='completed' artifact='failed'",
            ),
            (
                lambda output_dir: _set_flow_message_payload(
                    output_dir, "contribution_recorded", "echo-1", "valid", False
                ),
                "receipt entry 0 job=echo-1 node=local-node-a contribution.valid mismatch: receipt=True artifact=False",
            ),
            (
                lambda output_dir: _duplicate_flow_message(
                    output_dir, "job_result_reported", "echo-1"
                ),
                "receipt entry 0 job=echo-1 node=local-node-a job_result_reported message_id is ambiguous in flow log: msg-0005",
            ),
        ]
        for tamper, match in cases:
            with tempfile.TemporaryDirectory() as temp_dir:
                manifest_path = _write_manifest(Path(temp_dir))
                output_dir = Path(temp_dir) / "flow"
                run_local_flow(str(manifest_path), str(output_dir))
                tamper(output_dir)
                before = _artifact_contents(output_dir)
                with self.subTest(match=match):
                    with self.assertRaises(FlowAuditError) as cm:
                        audit_local_flow(output_dir)
                    self.assertEqual(str(cm.exception), match)
                after = _artifact_contents(output_dir)
            self.assertEqual(after, before)

    def test_audit_local_flow_rejects_tampered_result_hashes(self) -> None:
        cases: list[tuple[FlowTamper, str]] = [
            (
                lambda output_dir: _set_receipt_field(
                    output_dir, 0, "result_hash", "0" * 64
                ),
                "result.result_hash mismatch",
            ),
            (
                lambda output_dir: _tamper_result_output_without_rehash(output_dir),
                "result.recomputed_hash mismatch",
            ),
            (
                lambda output_dir: _set_ledger_record_field(
                    output_dir, 0, "result_hash", "0" * 64
                ),
                "receipt contribution claims do not match ledger records",
            ),
        ]
        for tamper, match in cases:
            with tempfile.TemporaryDirectory() as temp_dir:
                manifest_path = _write_manifest(Path(temp_dir))
                output_dir = Path(temp_dir) / "flow"
                run_local_flow(str(manifest_path), str(output_dir))
                tamper(output_dir)
                before = _artifact_contents(output_dir)
                with (
                    self.subTest(match=match),
                    self.assertRaisesRegex(FlowAuditError, match),
                ):
                    audit_local_flow(output_dir)
                after = _artifact_contents(output_dir)
            self.assertEqual(after, before)


def _sample_receipt() -> dict[str, object]:
    return {
        "assignment_message_id": "msg-0003",
        "result_message_id": "msg-0005",
        "validation_message_id": "msg-0006",
        "contribution_message_id": "msg-0007",
        "job_id": "echo-1",
        "job_type": "echo",
        "node_id": "local-node-a",
        "result_status": "completed",
        "credited_units": 1,
        "output_summary": {"message": "hello mesh"},
        "validation": {"valid": True, "reason": "ok"},
    }


def _sample_message(
    message_type: str,
    *,
    sender_node_id: str = "local-node-a",
    payload: dict[str, object] | None = None,
) -> MeshMessage:
    base_payload: dict[str, object]
    if message_type == "job_validated":
        base_payload = {
            "job_id": "echo-1",
            "node_id": "local-node-a",
            "assignment_message_id": "msg-0003",
            "result_message_id": "msg-0005",
            "valid": True,
            "reason": "ok",
            "contribution_units_after_validation": 1,
        }
    elif message_type == "contribution_recorded":
        base_payload = {
            "job_id": "echo-1",
            "node_id": "local-node-a",
            "status": "completed",
            "valid": True,
            "validation": "ok",
            "contribution_units": 1,
        }
    else:
        base_payload = {
            "job_id": "echo-1",
            "status": "completed",
            "output": {"message": "hello mesh"},
        }
    if payload is not None:
        base_payload.update(payload)
    return MeshMessage(
        message_id="msg-0005",
        message_type=message_type,
        sender_node_id=sender_node_id,
        recipient_node_id="local-ledger",
        payload=base_payload,
        correlation_id="echo-1",
    )


def _write_json(path: Path, document: dict[str, object]) -> None:
    path.write_text(
        json.dumps(document, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _set_receipt_field(
    output_dir: Path, index: int, field_name: str, value: object
) -> None:
    path = output_dir / "receipts.json"
    document = _load_json(path)
    receipts = document["receipts"]
    assert isinstance(receipts, list)
    receipt = receipts[index]
    assert isinstance(receipt, dict)
    receipt[field_name] = value
    _write_json(path, document)


def _set_dispatch_assignment_payload(
    output_dir: Path, message_id: str, field_name: str, value: object
) -> None:
    path = output_dir / "dispatch-message-log.json"
    document = _load_json(path)
    message = _find_message(document, "job_assigned", "job_id", "echo-1", message_id)
    payload = message["payload"]
    assert isinstance(payload, dict)
    payload[field_name] = value
    _write_json(path, document)


def _set_flow_message_type(
    output_dir: Path, message_type: str, job_id: str, replacement_type: str
) -> None:
    path = output_dir / "flow-message-log.json"
    document = _load_json(path)
    message = _find_message(document, message_type, "job_id", job_id)
    message["message_type"] = replacement_type
    _write_json(path, document)


def _set_flow_message_sender(
    output_dir: Path, message_type: str, job_id: str, sender_node_id: str
) -> None:
    path = output_dir / "flow-message-log.json"
    document = _load_json(path)
    message = _find_message(document, message_type, "job_id", job_id)
    message["sender_node_id"] = sender_node_id
    _write_json(path, document)


def _set_flow_message_payload(
    output_dir: Path, message_type: str, job_id: str, field_name: str, value: object
) -> None:
    path = output_dir / "flow-message-log.json"
    document = _load_json(path)
    message = _find_message(document, message_type, "job_id", job_id)
    payload = message["payload"]
    assert isinstance(payload, dict)
    payload[field_name] = value
    _write_json(path, document)


def _duplicate_flow_message(output_dir: Path, message_type: str, job_id: str) -> None:
    path = output_dir / "flow-message-log.json"
    document = _load_json(path)
    messages = document["messages"]
    assert isinstance(messages, list)
    messages.append(dict(_find_message(document, message_type, "job_id", job_id)))
    _write_json(path, document)


def _tamper_result_output_without_rehash(output_dir: Path) -> None:
    _set_flow_message_payload(
        output_dir, "job_result_reported", "echo-1", "output", "tampered"
    )
    _set_receipt_field(output_dir, 0, "output_summary", {"value": "tampered"})


def _set_ledger_record_field(
    output_dir: Path, index: int, field_name: str, value: object
) -> None:
    path = output_dir / "ledger.json"
    document = _load_json(path)
    records = document["records"]
    assert isinstance(records, list)
    record = records[index]
    assert isinstance(record, dict)
    record[field_name] = value
    _write_json(path, document)


def _find_message(
    document: dict[str, object],
    message_type: str,
    payload_field: str,
    payload_value: object,
    message_id: str | None = None,
) -> dict[str, object]:
    messages = document["messages"]
    assert isinstance(messages, list)
    for message in messages:
        assert isinstance(message, dict)
        payload = message.get("payload")
        if (
            isinstance(payload, dict)
            and message.get("message_type") == message_type
            and payload.get(payload_field) == payload_value
            and (message_id is None or message.get("message_id") == message_id)
        ):
            return message
    raise AssertionError(
        f"missing {message_type} message for {payload_field}={payload_value}"
    )


def _write_manifest(directory: Path) -> Path:
    manifest_path = directory / "local-batch.json"
    manifest_path.write_text(
        json.dumps(
            {
                "version": 1,
                "nodes": ["local-node-a", "local-node-b"],
                "jobs": [
                    {
                        "job_id": "echo-1",
                        "job_type": "echo",
                        "payload": {"message": "hello mesh"},
                    },
                    {
                        "job_id": "text-stats-1",
                        "job_type": "text_stats",
                        "payload": {"text": "hello mesh\nhello node"},
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    return manifest_path


def _artifact_contents(output_dir: Path) -> dict[str, str]:
    return {
        path.relative_to(output_dir).as_posix(): path.read_text(encoding="utf-8")
        for path in sorted(output_dir.rglob("*.json"))
    }


if __name__ == "__main__":
    unittest.main()
