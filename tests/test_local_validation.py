import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from aethermesh_core.local_validation import (
    LocalValidationError,
    _job_from_assignment,
    _required_non_empty_string,
    _result_from_message,
    _sort_key,
    validate_local_results,
)
from aethermesh_core.message_log import write_message_log
from aethermesh_core.messages import MeshMessage


class LocalValidationReplayTests(unittest.TestCase):
    def test_validate_local_results_writes_deterministic_report(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log(
                    [
                        _assignment("msg-0002", "job-b", "second", "node-b"),
                        _assignment("msg-0001", "job-a", "first", "node-a"),
                    ]
                ),
            )
            write_message_log(
                result_path,
                _message_log(
                    [
                        _result("msg-0004", "job-b", "second", "node-b"),
                        _result("msg-0003", "job-a", "first", "node-a"),
                    ]
                ),
            )

            first = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )
            persisted = validation_path.read_text(encoding="utf-8")

        self.assertEqual(first["version"], 1)
        self.assertEqual(first["kind"], "local_validation_report")
        self.assertEqual(
            first["summary"],
            {
                "assignments_seen": 2,
                "results_seen": 2,
                "results_validated": 2,
                "valid_results": 2,
                "invalid_results": 0,
            },
        )
        self.assertEqual(
            [entry["job_id"] for entry in first["validations"]],
            ["job-a", "job-b"],
        )
        self.assertEqual(
            first["validations"][0],
            {
                "message_type": "job_result_validated",
                "assignment_message_id": "msg-0001",
                "result_message_id": "msg-0003",
                "job_id": "job-a",
                "correlation_id": "job-a",
                "result_sender": "node-a",
                "valid": True,
                "reason": "ok",
            },
        )
        self.assertEqual(persisted, json.dumps(first, indent=2, sort_keys=True) + "\n")

    def test_invalid_job_result_is_reported_not_raised(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log([_assignment("msg-0001", "job-a", "expected", "node-a")]),
            )
            write_message_log(
                result_path,
                _message_log([_result("msg-0002", "job-a", "wrong", "node-a")]),
            )

            report = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )

        self.assertEqual(report["summary"]["valid_results"], 0)
        self.assertEqual(report["summary"]["invalid_results"], 1)
        self.assertEqual(report["validations"][0]["valid"], False)
        self.assertEqual(report["validations"][0]["reason"], "output_mismatch")

    def test_result_from_node_other_than_assignee_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log([_assignment("msg-0001", "job-a", "expected", "node-a")]),
            )
            write_message_log(
                result_path,
                _message_log([_result("msg-0002", "job-a", "expected", "node-b")]),
            )

            report = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )

        self.assertEqual(report["summary"]["valid_results"], 0)
        self.assertEqual(report["summary"]["invalid_results"], 1)
        self.assertEqual(report["validations"][0]["reason"], "result_node_id_mismatch")

    def test_valid_scored_result_units_do_not_make_replay_invalid(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log(
                    [
                        MeshMessage(
                            message_id="msg-0001",
                            message_type="job_assigned",
                            sender_node_id="local-scheduler",
                            recipient_node_id="node-a",
                            payload={
                                "job_id": "job-a",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh"},
                            },
                            correlation_id="job-a",
                        )
                    ]
                ),
            )
            write_message_log(
                result_path,
                _message_log(
                    [
                        _result_with_payload(
                            "msg-0002",
                            {
                                "job_id": "job-a",
                                "status": "completed",
                                "success": True,
                                "output": {
                                    "character_count": 10,
                                    "word_count": 2,
                                    "line_count": 1,
                                    "normalized_preview": "hello mesh",
                                },
                                "error": None,
                                "contribution_units": 2,
                            },
                        )
                    ]
                ),
            )

            report = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )

        self.assertEqual(report["summary"]["valid_results"], 1)
        self.assertEqual(report["summary"]["invalid_results"], 0)
        self.assertEqual(report["validations"][0]["reason"], "ok")

    def test_failed_result_stays_invalid_during_replay(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log([_assignment("msg-0001", "job-a", "expected", "node-a")]),
            )
            write_message_log(
                result_path,
                _message_log(
                    [
                        _result_with_payload(
                            "msg-0002",
                            {
                                "job_id": "job-a",
                                "status": "failed",
                                "success": False,
                                "output": None,
                                "error": "worker failed",
                                "contribution_units": 0,
                            },
                        )
                    ]
                ),
            )

            report = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )

        self.assertEqual(report["summary"]["valid_results"], 0)
        self.assertEqual(report["summary"]["invalid_results"], 1)
        self.assertEqual(report["validations"][0]["reason"], "result_not_completed")

    def test_missing_assignment_duplicate_collisions_and_no_overwrite_fail(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            validation_path.write_text('{"keep": true}\n', encoding="utf-8")
            with self.assertRaisesRegex(LocalValidationError, "already exists"):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )

            validation_path.unlink()
            write_message_log(assignment_path, _message_log([]))
            write_message_log(
                result_path,
                _message_log([_result("msg-0002", "job-a", "expected", "node-a")]),
            )
            with self.assertRaisesRegex(LocalValidationError, "no matching"):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )
            self.assertFalse(validation_path.exists())

            write_message_log(
                assignment_path,
                _message_log(
                    [
                        _assignment("msg-0001", "job-a", "expected", "node-a"),
                        _assignment("msg-0002", "job-a", "expected", "node-a"),
                    ]
                ),
            )
            with self.assertRaisesRegex(LocalValidationError, "multiple job_assigned"):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )

            write_message_log(
                assignment_path,
                _message_log([_assignment("msg-0001", "job-a", "expected", "node-a")]),
            )
            write_message_log(
                result_path,
                _message_log(
                    [
                        _result("msg-0002", "job-a", "expected", "node-a"),
                        _result("msg-0003", "job-a", "expected", "node-a"),
                    ]
                ),
            )
            with self.assertRaisesRegex(
                LocalValidationError, "multiple job_result_reported"
            ):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )

    def test_malformed_logs_required_fields_and_write_errors_fail_without_artifact(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            assignment_path.write_text("not json", encoding="utf-8")
            with self.assertRaisesRegex(LocalValidationError, "malformed"):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )

            write_message_log(
                assignment_path,
                _message_log(
                    [
                        MeshMessage(
                            message_id="msg-0001",
                            message_type="job_assigned",
                            sender_node_id="local-scheduler",
                            recipient_node_id="node-a",
                            payload={"job_id": "job-a", "job_type": "echo"},
                            correlation_id="job-a",
                        )
                    ]
                ),
            )
            write_message_log(result_path, _message_log([]))
            with self.assertRaisesRegex(LocalValidationError, "field 'payload'"):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )

            write_message_log(
                assignment_path,
                _message_log([_assignment("msg-0001", "job-a", "expected", "node-a")]),
            )
            bad_results = [
                {
                    "job_id": "job-a",
                    "status": "completed",
                    "error": None,
                    "contribution_units": 1,
                },
                {
                    "job_id": "job-a",
                    "status": "completed",
                    "output": "expected",
                    "error": 7,
                    "contribution_units": 1,
                },
                {
                    "job_id": "job-a",
                    "status": "completed",
                    "output": "expected",
                    "error": None,
                    "contribution_units": True,
                },
            ]
            errors = ["field 'output'", "field 'error'", "field 'contribution_units'"]
            for payload, error in zip(bad_results, errors, strict=True):
                write_message_log(
                    result_path,
                    _message_log([_result_with_payload("msg-0002", payload)]),
                )
                with self.assertRaisesRegex(LocalValidationError, error):
                    validate_local_results(
                        assignment_log_path=assignment_path,
                        result_log_path=result_path,
                        validation_log_path=validation_path,
                    )

            write_message_log(
                result_path,
                _message_log([_result("msg-0002", "job-a", "expected", "node-a")]),
            )
            with (
                mock.patch(
                    "aethermesh_core.local_validation.atomic_write_json",
                    side_effect=OSError("disk full"),
                ),
                self.assertRaisesRegex(LocalValidationError, "could not write"),
            ):
                validate_local_results(
                    assignment_log_path=assignment_path,
                    result_log_path=result_path,
                    validation_log_path=validation_path,
                )
            self.assertFalse(validation_path.exists())

    def test_result_without_correlation_can_match_assignment_without_correlation(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                _message_log(
                    [_assignment("msg-0001", "job-a", "expected", "node-a", None)]
                ),
            )
            write_message_log(
                result_path,
                _message_log(
                    [_result("msg-0002", "job-a", "expected", "node-a", None)]
                ),
            )

            report = validate_local_results(
                assignment_log_path=assignment_path,
                result_log_path=result_path,
                validation_log_path=validation_path,
            )

        self.assertEqual(report["validations"][0]["correlation_id"], None)
        self.assertEqual(report["summary"]["valid_results"], 1)

    def test_validation_helpers_preserve_message_context_and_payload_shape(
        self,
    ) -> None:
        assignment = _assignment("msg-0001", "job-a", "expected", "node-a")
        job = _job_from_assignment(assignment)
        self.assertEqual(job.job_id, "job-a")
        self.assertEqual(job.job_type, "echo")
        self.assertEqual(job.payload, {"message": "expected"})

        result_message = _result("msg-0002", "job-a", "expected", "node-a")
        result = _result_from_message(result_message)
        self.assertEqual(result.job_id, "job-a")
        self.assertEqual(result.node_id, "node-a")
        self.assertEqual(result.status, "completed")
        self.assertEqual(result.output, "expected")
        self.assertIsNone(result.error)
        self.assertEqual(result.contribution_units, 1)
        self.assertEqual(_sort_key((None, "job-b")), ("", "job-b"))
        self.assertEqual(_sort_key(("corr-a", "job-b")), ("corr-a", "job-b"))

        with self.assertRaisesRegex(
            LocalValidationError,
            "job_assigned payload field 'job_id' must be a non-empty string",
        ):
            _job_from_assignment(
                MeshMessage(
                    message_id="msg-bad-assignment",
                    message_type="job_assigned",
                    sender_node_id="local-scheduler",
                    recipient_node_id="node-a",
                    payload={"job_type": "echo", "payload": {}},
                    correlation_id="job-a",
                )
            )
        with self.assertRaisesRegex(
            LocalValidationError,
            "job_result_reported payload field 'job_id' must be a non-empty string",
        ):
            _result_from_message(
                MeshMessage(
                    message_id="msg-bad-result",
                    message_type="job_result_reported",
                    sender_node_id="node-a",
                    recipient_node_id="local-scheduler",
                    payload={
                        "status": "completed",
                        "output": "expected",
                        "error": None,
                        "contribution_units": 1,
                    },
                    correlation_id="job-a",
                )
            )

        for payload, field, message_type in [
            ({"job_type": "echo", "payload": {}}, "job_id", "job_assigned"),
            ({"job_id": "job-a", "payload": {}}, "job_type", "job_assigned"),
            (
                {
                    "status": "completed",
                    "output": "expected",
                    "error": None,
                    "contribution_units": 1,
                },
                "job_id",
                "job_result_reported",
            ),
            (
                {
                    "job_id": "job-a",
                    "output": "expected",
                    "error": None,
                    "contribution_units": 1,
                },
                "status",
                "job_result_reported",
            ),
        ]:
            with (
                self.subTest(field=field, message_type=message_type),
                self.assertRaisesRegex(
                    LocalValidationError,
                    f"{message_type} payload field '{field}' must be a non-empty string",
                ),
            ):
                _required_non_empty_string(payload, field, message_type)


def _message_log(messages: list[MeshMessage]) -> dict[str, object]:
    return {
        "version": 1,
        "metadata": {"message_count": len(messages)},
        "messages": [message.to_dict() for message in messages],
    }


def _assignment(
    message_id: str,
    job_id: str,
    message: str,
    node_id: str,
    correlation_id: str | None = "use-job-id",
) -> MeshMessage:
    effective_correlation_id = (
        job_id if correlation_id == "use-job-id" else correlation_id
    )
    return MeshMessage(
        message_id=message_id,
        message_type="job_assigned",
        sender_node_id="local-scheduler",
        recipient_node_id=node_id,
        payload={"job_id": job_id, "job_type": "echo", "payload": {"message": message}},
        correlation_id=effective_correlation_id,
    )


def _result(
    message_id: str,
    job_id: str,
    output: str,
    node_id: str,
    correlation_id: str | None = "use-job-id",
) -> MeshMessage:
    return _result_with_payload(
        message_id,
        {
            "job_id": job_id,
            "status": "completed",
            "success": True,
            "output": output,
            "error": None,
            "contribution_units": 1,
        },
        sender_node_id=node_id,
        correlation_id=job_id if correlation_id == "use-job-id" else correlation_id,
    )


def _result_with_payload(
    message_id: str,
    payload: dict[str, object],
    sender_node_id: str = "node-a",
    correlation_id: str | None = "job-a",
) -> MeshMessage:
    return MeshMessage(
        message_id=message_id,
        message_type="job_result_reported",
        sender_node_id=sender_node_id,
        recipient_node_id="local-ledger",
        payload=payload,
        correlation_id=correlation_id,
    )
