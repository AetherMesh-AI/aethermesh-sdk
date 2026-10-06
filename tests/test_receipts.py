import json
import tempfile
import unittest
from dataclasses import replace
from pathlib import Path
from unittest import mock

from aethermesh_core.contribution import score_validated_contribution
from aethermesh_core.ledger import ContributionLedger
from aethermesh_core.messages import MeshMessage
from aethermesh_core.models import Job, JobResult
from aethermesh_core.node_service import ProcessedAssignment
from aethermesh_core.receipts import (
    ReceiptPersistenceError,
    build_receipt_document,
    load_receipt_document_if_exists,
    write_receipt_document,
)
from aethermesh_core.result_hash import result_hash
from aethermesh_core.validation import validate_job_result
from aethermesh_core.version_metadata import (
    capture_version_metadata,
    version_metadata_ref,
)


def _test_version_metadata() -> dict[str, object]:
    return capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")


class ReceiptTests(unittest.TestCase):
    def test_build_receipt_document_uses_processed_assignment_audit_fields(
        self,
    ) -> None:
        assignment = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("echo-1", "echo", {"message": "hello mesh"}),
            result=JobResult(
                "echo-1", "local-node-a", "completed", "hello mesh", None, 1
            ),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )
        expected_hash = result_hash(assignment.result)
        metadata = _test_version_metadata()
        metadata_ref = version_metadata_ref(metadata)

        document = build_receipt_document([assignment], version_metadata=metadata)

        self.assertEqual(document["version"], 1)
        self.assertEqual(document["run_source"], "run-local-flow")
        self.assertEqual(document["version_metadata"], metadata)
        self.assertEqual(document["version_metadata_ref"], metadata_ref)
        self.assertEqual(document["version_metadata_by_ref"], {metadata_ref: metadata})
        self.assertEqual(
            document["receipts"],
            [
                {
                    "job_id": "echo-1",
                    "job_type": "echo",
                    "node_id": "local-node-a",
                    "assignment_message_id": "msg-0003",
                    "correlation_id": "echo-1",
                    "result_message_id": "msg-0004",
                    "validation_message_id": "validation-msg-0004",
                    "contribution_message_id": "msg-0005",
                    "result_status": "completed",
                    "result_hash": expected_hash,
                    "validation": {"valid": True, "reason": "ok"},
                    "version_metadata_ref": metadata_ref,
                    "credited_units": 1,
                    "output_summary": {"value": "hello mesh"},
                }
            ],
        )

    def test_build_receipt_document_requires_exactly_one_result_message(self) -> None:
        assignment = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("echo-1", "echo", {"message": "hello mesh"}),
            result=JobResult(
                "echo-1", "local-node-a", "completed", "hello mesh", None, 1
            ),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )
        assignment = replace(
            assignment,
            emitted_messages=[
                message
                for message in assignment.emitted_messages
                if message.message_type != "job_result_reported"
            ],
        )

        with self.assertRaises(ValueError) as cm:
            build_receipt_document([assignment])

        self.assertEqual(
            str(cm.exception),
            "processed assignment msg-0003 must emit one job_result_reported message",
        )

    def test_invalid_validation_receipt_keeps_zero_credit(self) -> None:
        job = Job("bad-1", "echo", {"message": "expected"})
        invalid_result = JobResult(
            "bad-1", "local-node-a", "completed", "actual", None, 1
        )
        assignment = _processed_assignment(
            message_id="msg-0003",
            correlation_id="bad-1",
            job=job,
            result=invalid_result,
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )

        receipt = build_receipt_document([assignment])["receipts"][0]

        self.assertEqual(
            receipt["validation"], {"valid": False, "reason": "output_mismatch"}
        )
        self.assertEqual(receipt["credited_units"], 0)
        self.assertEqual(receipt["contribution_message_id"], "msg-0005")

    def test_receipts_sort_deterministically_and_keep_compact_dict_summary(
        self,
    ) -> None:
        first = _processed_assignment(
            message_id="msg-0007",
            correlation_id="text-stats-1",
            job=Job("text-stats-1", "text_stats", {"text": "hello mesh\nhello node"}),
            result=JobResult(
                "text-stats-1",
                "local-node-b",
                "completed",
                {
                    "word_count": 4,
                    "line_count": 2,
                    "normalized_preview": "hello mesh hello node",
                },
                None,
                1,
            ),
            result_message_id="msg-0008",
            contribution_message_id="msg-0009",
        )
        second = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("echo-1", "echo", {"message": "hello mesh"}),
            result=JobResult(
                "echo-1", "local-node-a", "completed", "hello mesh", None, 1
            ),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )

        document = build_receipt_document([first, second])

        self.assertEqual(
            [receipt["job_id"] for receipt in document["receipts"]],
            ["echo-1", "text-stats-1"],
        )
        self.assertEqual(
            document["receipts"][1]["output_summary"],
            {
                "line_count": 2,
                "normalized_preview": "hello mesh hello node",
                "word_count": 4,
            },
        )

    def test_receipts_sort_same_job_by_node_then_assignment_message_id(self) -> None:
        assignments = [
            _processed_assignment(
                message_id="msg-0009",
                correlation_id="same-job-c",
                job=Job("same-job", "echo", {"message": "c"}),
                result=JobResult("same-job", "node-b", "completed", "c", None, 1),
                result_message_id="msg-0010",
                contribution_message_id="msg-0011",
            ),
            _processed_assignment(
                message_id="msg-0003",
                correlation_id="same-job-a",
                job=Job("same-job", "echo", {"message": "a"}),
                result=JobResult("same-job", "node-a", "completed", "a", None, 1),
                result_message_id="msg-0004",
                contribution_message_id="msg-0005",
            ),
            _processed_assignment(
                message_id="msg-0006",
                correlation_id="same-job-b",
                job=Job("same-job", "echo", {"message": "b"}),
                result=JobResult("same-job", "node-a", "completed", "b", None, 1),
                result_message_id="msg-0007",
                contribution_message_id="msg-0008",
            ),
        ]

        document = build_receipt_document(assignments)

        self.assertEqual(
            [
                (receipt["node_id"], receipt["assignment_message_id"])
                for receipt in document["receipts"]
            ],
            [("node-a", "msg-0003"), ("node-a", "msg-0006"), ("node-b", "msg-0009")],
        )

    def test_existing_receipts_are_merged_by_assignment_id_for_reruns(self) -> None:
        original = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("echo-1", "echo", {"message": "hello mesh"}),
            result=JobResult(
                "echo-1", "local-node-a", "completed", "hello mesh", None, 1
            ),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )
        existing = build_receipt_document([original])

        merged = build_receipt_document([], existing_document=existing)

        self.assertEqual(merged, existing)

    def test_existing_receipts_keep_distinct_assignment_ids_when_merged(self) -> None:
        first = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("same-job", "echo", {"message": "one"}),
            result=JobResult("same-job", "node-a", "completed", "one", None, 1),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )
        second = _processed_assignment(
            message_id="msg-0006",
            correlation_id="echo-2",
            job=Job("same-job", "echo", {"message": "two"}),
            result=JobResult("same-job", "node-b", "completed", "two", None, 1),
            result_message_id="msg-0007",
            contribution_message_id="msg-0008",
        )
        existing = build_receipt_document([second, first])

        merged = build_receipt_document([], existing_document=existing)

        self.assertEqual(merged, existing)
        self.assertEqual(
            [receipt["assignment_message_id"] for receipt in merged["receipts"]],
            ["msg-0003", "msg-0006"],
        )

    def test_existing_receipts_keep_original_metadata_ref_when_new_run_is_merged(
        self,
    ) -> None:
        original_metadata = capture_version_metadata(
            captured_at="2026-07-08T00:00:00+00:00"
        )
        new_metadata = capture_version_metadata(captured_at="2026-07-08T00:01:00+00:00")
        original_ref = version_metadata_ref(original_metadata)
        new_ref = version_metadata_ref(new_metadata)
        original = _processed_assignment(
            message_id="msg-0003",
            correlation_id="echo-1",
            job=Job("echo-1", "echo", {"message": "one"}),
            result=JobResult("echo-1", "node-a", "completed", "one", None, 1),
            result_message_id="msg-0004",
            contribution_message_id="msg-0005",
        )
        new = _processed_assignment(
            message_id="msg-0006",
            correlation_id="echo-2",
            job=Job("echo-2", "echo", {"message": "two"}),
            result=JobResult("echo-2", "node-a", "completed", "two", None, 1),
            result_message_id="msg-0007",
            contribution_message_id="msg-0008",
        )
        existing = build_receipt_document(
            [original], version_metadata=original_metadata
        )

        merged = build_receipt_document(
            [new], existing_document=existing, version_metadata=new_metadata
        )

        refs_by_assignment = {
            receipt["assignment_message_id"]: receipt["version_metadata_ref"]
            for receipt in merged["receipts"]
        }
        self.assertEqual(refs_by_assignment["msg-0003"], original_ref)
        self.assertEqual(refs_by_assignment["msg-0006"], new_ref)
        self.assertEqual(
            merged["version_metadata_by_ref"],
            {original_ref: original_metadata, new_ref: new_metadata},
        )

    def test_receipt_document_write_and_load_round_trip(self) -> None:
        document = build_receipt_document([])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "receipts.json"

            write_receipt_document(path, document)
            loaded = load_receipt_document_if_exists(path)
            missing = load_receipt_document_if_exists(Path(temp_dir) / "missing.json")
            raw = path.read_text(encoding="utf-8")

        self.assertEqual(loaded, document)
        self.assertIsNone(missing)
        self.assertTrue(raw.endswith("\n"))
        self.assertEqual(json.loads(raw), document)

    def test_write_receipt_document_uses_stable_json_and_cleans_temp_files(
        self,
    ) -> None:
        document = build_receipt_document([])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "deep" / "nested" / "receipts.json"

            write_receipt_document(path, document)
            raw = path.read_text(encoding="utf-8")

            self.assertTrue(raw.endswith("\n"))
            self.assertEqual(json.loads(raw), document)
            self.assertEqual(
                sorted(child.name for child in path.parent.iterdir()),
                ["receipts.json"],
            )

    def test_write_receipt_document_preserves_existing_file_on_atomic_replace_failure(
        self,
    ) -> None:
        document = build_receipt_document([])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "receipts.json"
            path.write_text('{"original": true}\n', encoding="utf-8")

            with (
                mock.patch(
                    "aethermesh_core.receipts.os.replace",
                    side_effect=OSError("replace failed"),
                ),
                self.assertRaisesRegex(ReceiptPersistenceError, "replace failed"),
            ):
                write_receipt_document(path, document)

            self.assertEqual(path.read_text(encoding="utf-8"), '{"original": true}\n')
            self.assertEqual(
                sorted(child.name for child in path.parent.iterdir()),
                ["receipts.json"],
            )

    def test_receipt_document_validation_rejects_malformed_entries(self) -> None:
        valid = build_receipt_document(
            [
                _processed_assignment(
                    message_id="msg-0003",
                    correlation_id="echo-1",
                    job=Job("echo-1", "echo", {"message": "hello mesh"}),
                    result=JobResult(
                        "echo-1", "node-a", "completed", "hello mesh", None, 1
                    ),
                    result_message_id="msg-0004",
                    contribution_message_id="msg-0005",
                )
            ]
        )
        cases = []
        cases.append({**valid, "version": 2})
        cases.append({**valid, "version": True})
        cases.append({**valid, "run_source": "other"})
        cases.append({**valid, "receipts": {}})
        cases.append({**valid, "receipts": ["not-object"]})
        receipt = dict(valid["receipts"][0])
        for field_name in (
            "job_id",
            "job_type",
            "node_id",
            "assignment_message_id",
            "result_message_id",
            "validation_message_id",
            "result_status",
        ):
            bad = dict(receipt)
            bad[field_name] = ""
            cases.append({**valid, "receipts": [bad]})
            bad = dict(receipt)
            bad[field_name] = None
            cases.append({**valid, "receipts": [bad]})
        for field_name in ("correlation_id", "contribution_message_id"):
            bad = dict(receipt)
            bad[field_name] = 7
            cases.append({**valid, "receipts": [bad]})
        for bad_units in (True, "1", None):
            bad = dict(receipt)
            bad["credited_units"] = bad_units
            cases.append({**valid, "receipts": [bad]})
        bad = dict(receipt)
        bad["validation"] = []
        cases.append({**valid, "receipts": [bad]})
        bad = dict(receipt)
        bad["validation"] = {"valid": "yes", "reason": "ok"}
        cases.append({**valid, "receipts": [bad]})
        bad = dict(receipt)
        bad["validation"] = {"valid": True, "reason": None}
        cases.append({**valid, "receipts": [bad]})
        bad = dict(receipt)
        bad["output_summary"] = []
        cases.append({**valid, "receipts": [bad]})
        for bad_hash in (7, "a" * 63, "A" * 64, "g" * 64):
            bad = dict(receipt)
            bad["result_hash"] = bad_hash
            cases.append({**valid, "receipts": [bad]})

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "receipts.json"
            for index, document in enumerate(cases):
                with self.subTest(index=index, document=document):
                    path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(ReceiptPersistenceError):
                        load_receipt_document_if_exists(path)

    def test_write_receipt_document_uses_target_parent_temp_file_contract(self) -> None:
        document = build_receipt_document([])
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "deep" / "nested" / "receipts.json"
            calls: list[dict[str, object]] = []
            real_named_temporary_file = tempfile.NamedTemporaryFile

            def capture_named_temporary_file(*args, **kwargs):
                calls.append({"args": args, "kwargs": dict(kwargs)})
                return real_named_temporary_file(*args, **kwargs)

            with mock.patch(
                "aethermesh_core.receipts.tempfile.NamedTemporaryFile",
                side_effect=capture_named_temporary_file,
            ):
                write_receipt_document(path, document)

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["args"], ("w",))
            self.assertEqual(calls[0]["kwargs"]["encoding"], "utf-8")
            self.assertEqual(calls[0]["kwargs"]["dir"], path.parent)
            self.assertEqual(calls[0]["kwargs"]["prefix"], ".receipts.json.")
            self.assertEqual(calls[0]["kwargs"]["suffix"], ".tmp")
            self.assertEqual(calls[0]["kwargs"]["delete"], False)

    def test_receipt_document_validation_error_messages_are_stable(self) -> None:
        valid = build_receipt_document(
            [
                _processed_assignment(
                    message_id="msg-0003",
                    correlation_id="echo-1",
                    job=Job("echo-1", "echo", {"message": "hello mesh"}),
                    result=JobResult(
                        "echo-1", "node-a", "completed", "hello mesh", None, 1
                    ),
                    result_message_id="msg-0004",
                    contribution_message_id="msg-0005",
                )
            ]
        )
        receipt = dict(valid["receipts"][0])
        cases = [
            ({**valid, "version": True}, "receipt JSON must contain version 1"),
            ({**valid, "receipts": {}}, "receipt JSON field 'receipts' must be a list"),
            ({**valid, "version": 2}, "receipt JSON must contain version 1"),
            (
                {**valid, "receipts": [{**receipt, "job_id": ""}]},
                "receipt entry 0 field 'job_id' must be a non-empty string",
            ),
            (
                {**valid, "receipts": [{**receipt, "correlation_id": 7}]},
                "receipt entry 0 field 'correlation_id' must be a string or null",
            ),
            (
                {**valid, "receipts": [{**receipt, "contribution_message_id": 7}]},
                "receipt entry 0 field 'contribution_message_id' must be a string or null",
            ),
            (
                {**valid, "receipts": [{**receipt, "validation": []}]},
                "receipt entry 0 field 'validation' must be an object",
            ),
            (
                {
                    **valid,
                    "receipts": [
                        {**receipt, "validation": {"valid": True, "reason": None}}
                    ],
                },
                "receipt entry 0 validation field 'reason' must be a string",
            ),
            (
                {**valid, "run_source": "other"},
                "receipt JSON field 'run_source' must be run-local-flow",
            ),
            (
                {**valid, "receipts": ["not-object"]},
                "receipt entry 0 must be an object",
            ),
            (
                {**valid, "receipts": [{**receipt, "credited_units": True}]},
                "receipt entry 0 field 'credited_units' must be an integer",
            ),
            (
                {
                    **valid,
                    "receipts": [
                        {**receipt, "validation": {"valid": "yes", "reason": "ok"}}
                    ],
                },
                "receipt entry 0 validation field 'valid' must be a boolean",
            ),
            (
                {**valid, "receipts": [{**receipt, "output_summary": []}]},
                "receipt entry 0 field 'output_summary' must be an object",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "receipts.json"
            for document, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(ReceiptPersistenceError) as cm:
                        load_receipt_document_if_exists(path)
                    self.assertEqual(str(cm.exception), expected_message)


def _processed_assignment(
    *,
    message_id: str,
    correlation_id: str,
    job: Job,
    result: JobResult,
    result_message_id: str,
    contribution_message_id: str,
) -> ProcessedAssignment:
    validation = validate_job_result(job, result)
    credited_units = (
        score_validated_contribution(job, result) if validation.valid else 0
    )
    accounted_result = replace(result, contribution_units=credited_units)
    record = ContributionLedger().record(
        accounted_result,
        validation_valid=validation.valid,
        validation_reason=validation.reason,
        job_type=job.job_type,
    )
    return ProcessedAssignment(
        message_id=message_id,
        correlation_id=correlation_id,
        job=job,
        result=result,
        validation=validation,
        contribution_record=record,
        emitted_messages=[
            MeshMessage(
                message_id=result_message_id,
                message_type="job_result_reported",
                sender_node_id=result.node_id,
                recipient_node_id="local-ledger",
                payload={"job_id": result.job_id, "status": result.status},
                correlation_id=correlation_id,
            ),
            MeshMessage(
                message_id=f"validation-{result_message_id}",
                message_type="job_validated",
                sender_node_id=result.node_id,
                recipient_node_id="local-ledger",
                payload={
                    "job_id": result.job_id,
                    "node_id": result.node_id,
                    "assignment_message_id": message_id,
                    "result_message_id": result_message_id,
                    "validator_id": result.node_id,
                    "valid": validation.valid,
                    "reason": validation.reason,
                    "contribution_units_after_validation": record.contribution_units,
                },
                correlation_id=correlation_id,
            ),
            MeshMessage(
                message_id=contribution_message_id,
                message_type="contribution_recorded",
                sender_node_id="local-ledger",
                recipient_node_id=result.node_id,
                payload={
                    "job_id": result.job_id,
                    "contribution_units": record.contribution_units,
                },
                correlation_id=correlation_id,
            ),
        ],
    )


if __name__ == "__main__":
    unittest.main()
