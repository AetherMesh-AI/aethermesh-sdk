import json
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast
from unittest import mock

from aethermesh_core.ledger import (
    ContributionLedger,
    ContributionRecord,
    LedgerPersistenceError,
    load_existing_ledger_document,
    load_ledger_document,
    save_ledger_document,
)
from aethermesh_core.models import JobResult
from aethermesh_core.result_hash import result_hash


class ContributionLedgerTests(unittest.TestCase):
    def test_completed_result_adds_positive_contribution_units(self) -> None:
        ledger = ContributionLedger(
            clock=lambda: datetime(2026, 7, 14, 14, 8, 7, tzinfo=UTC)
        )
        result = JobResult(
            job_id="job-1",
            node_id="node-a",
            status="completed",
            output="hello mesh",
            error=None,
            contribution_units=3,
        )

        record = ledger.record(result)
        summary = ledger.summary_for_node("node-a")

        self.assertEqual(record.node_id, "node-a")
        self.assertEqual(record.job_id, "job-1")
        self.assertEqual(record.status, "completed")
        self.assertEqual(record.contribution_units, 3)
        self.assertEqual(record.message, "hello mesh")
        self.assertIsNone(record.validation_valid)
        self.assertIsNone(record.validation_reason)
        self.assertIsNone(record.job_type)
        self.assertEqual(record.result_hash, result_hash(result))
        self.assertEqual(record.created_at, "2026-07-14T14:08:07Z")
        assert record.created_at is not None
        self.assertEqual(
            datetime.strptime(record.created_at, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            ),
            datetime(2026, 7, 14, 14, 8, 7, tzinfo=UTC),
        )
        self.assertEqual(summary.node_id, "node-a")
        self.assertEqual(summary.completed_job_count, 1)
        self.assertEqual(summary.failed_job_count, 0)
        self.assertEqual(summary.total_result_count, 1)
        self.assertEqual(summary.total_contribution_units, 3)

    def test_failed_result_is_recorded_without_contribution_units(self) -> None:
        ledger = ContributionLedger()
        result = JobResult(
            job_id="job-2",
            node_id="node-a",
            status="failed",
            output=None,
            error="Unsupported job type: nope",
            contribution_units=5,
        )

        record = ledger.record(result)
        summary = ledger.summary_for_node("node-a")

        self.assertEqual(record.status, "failed")
        self.assertEqual(record.contribution_units, 0)
        self.assertEqual(record.message, "Unsupported job type: nope")
        self.assertEqual(summary.completed_job_count, 0)
        self.assertEqual(summary.failed_job_count, 1)
        self.assertEqual(summary.total_result_count, 1)
        self.assertEqual(summary.total_contribution_units, 0)

    def test_multiple_results_for_one_node_aggregate_deterministically(self) -> None:
        ledger = ContributionLedger()
        ledger.record(
            JobResult(
                job_id="job-1",
                node_id="node-a",
                status="completed",
                output="one",
                error=None,
                contribution_units=1,
            )
        )
        ledger.record(
            JobResult(
                job_id="job-2",
                node_id="node-a",
                status="completed",
                output="two",
                error=None,
                contribution_units=2,
            )
        )
        ledger.record(
            JobResult(
                job_id="job-3",
                node_id="node-a",
                status="failed",
                output=None,
                error="boom",
                contribution_units=7,
            )
        )
        ledger.record(
            JobResult(
                job_id="job-other",
                node_id="node-b",
                status="completed",
                output="other",
                error=None,
                contribution_units=100,
            )
        )

        summary = ledger.summary_for_node("node-a")

        self.assertEqual(summary.completed_job_count, 2)
        self.assertEqual(summary.failed_job_count, 1)
        self.assertEqual(summary.total_result_count, 3)
        self.assertEqual(summary.total_contribution_units, 3)

    def test_negative_completed_units_are_clamped_to_zero(self) -> None:
        ledger = ContributionLedger()
        result = JobResult(
            job_id="job-negative",
            node_id="node-a",
            status="completed",
            output="invalid units",
            error=None,
            contribution_units=-2,
        )

        record = ledger.record(result)
        summary = ledger.summary_for_node("node-a")

        self.assertEqual(record.contribution_units, 0)
        self.assertEqual(summary.completed_job_count, 1)
        self.assertEqual(summary.total_result_count, 1)
        self.assertEqual(summary.total_contribution_units, 0)

    def test_non_integer_completed_units_raise_value_error(self) -> None:
        ledger = ContributionLedger()
        result = JobResult(
            job_id="job-invalid",
            node_id="node-a",
            status="completed",
            output="invalid units",
            error=None,
            contribution_units=cast(Any, "1"),
        )

        with self.assertRaisesRegex(
            ValueError, "contribution_units must be an integer"
        ):
            ledger.record(result)

    def test_contribution_record_round_trips_through_json_dict(self) -> None:
        record = ContributionRecord(
            node_id="node-a",
            job_id="job-1",
            status="completed",
            contribution_units=1,
            message="hello mesh",
            validation_valid=True,
            validation_reason="ok",
            job_type="echo",
            result_hash="a" * 64,
            manifest_ref="examples/job-envelopes/minimal-local-echo.json",
        )

        decoded = ContributionRecord.from_dict(json.loads(json.dumps(record.to_dict())))

        self.assertEqual(decoded, record)
        self.assertEqual(
            decoded.manifest_ref,
            "examples/job-envelopes/minimal-local-echo.json",
        )

    def test_contribution_record_rejects_invalid_result_hash(self) -> None:
        valid = ContributionRecord(
            node_id="node-a",
            job_id="job-1",
            status="completed",
            contribution_units=1,
            message="hello mesh",
            result_hash="a" * 64,
        ).to_dict()
        for bad_hash in (7, "a" * 63, "A" * 64, "g" * 64):
            with self.subTest(bad_hash=bad_hash):
                payload = {**valid, "result_hash": bad_hash}
                with self.assertRaisesRegex(
                    LedgerPersistenceError, "lowercase SHA-256 hex digest"
                ):
                    ContributionRecord.from_dict(payload)

    def test_contribution_record_rejects_empty_manifest_reference(self) -> None:
        payload = ContributionRecord(
            node_id="node-a",
            job_id="job-1",
            status="completed",
            contribution_units=1,
        ).to_dict()

        with self.assertRaisesRegex(LedgerPersistenceError, "manifest_ref"):
            ContributionRecord.from_dict({**payload, "manifest_ref": ""})

    def test_record_rejects_empty_manifest_reference_before_persistence(self) -> None:
        ledger = ContributionLedger()

        with self.assertRaisesRegex(LedgerPersistenceError, "manifest_ref"):
            ledger.record(
                JobResult("job-1", "node-a", "completed", "hello mesh", None, 1),
                manifest_ref="",
            )

        self.assertEqual(ledger.to_document()["records"], [])

    def test_legacy_contribution_record_without_validation_metadata_loads(self) -> None:
        payload = {
            "node_id": "node-a",
            "job_id": "job-legacy",
            "status": "completed",
            "contribution_units": 1,
            "message": "legacy ok",
        }

        record = ContributionRecord.from_dict(payload)

        self.assertEqual(record.node_id, "node-a")
        self.assertEqual(record.job_id, "job-legacy")
        self.assertEqual(record.status, "completed")
        self.assertEqual(record.contribution_units, 1)
        self.assertEqual(record.message, "legacy ok")
        self.assertIsNone(record.validation_valid)
        self.assertIsNone(record.validation_reason)
        self.assertIsNone(record.job_type)
        self.assertIsNone(record.result_hash)
        self.assertIsNone(record.manifest_ref)

    def test_record_persists_validation_metadata_when_supplied(self) -> None:
        ledger = ContributionLedger(
            clock=lambda: datetime(2026, 7, 14, 14, 8, 7, tzinfo=UTC)
        )

        ledger.record(
            JobResult(
                job_id="job-invalid",
                node_id="node-a",
                status="completed",
                output="unexpected",
                error=None,
                contribution_units=0,
            ),
            validation_valid=False,
            validation_reason="output_mismatch",
            job_type="echo",
        )

        self.assertEqual(
            ledger.to_document()["records"][0],
            {
                "node_id": "node-a",
                "job_id": "job-invalid",
                "status": "completed",
                "contribution_units": 0,
                "message": "unexpected",
                "result_hash": result_hash(
                    JobResult(
                        "job-invalid", "node-a", "completed", "unexpected", None, 0
                    )
                ),
                "validation_valid": False,
                "validation_reason": "output_mismatch",
                "job_type": "echo",
                "created_at": "2026-07-14T14:08:07Z",
            },
        )

    def test_record_preserves_explicit_audit_references(self) -> None:
        ledger = ContributionLedger(
            clock=lambda: datetime(2026, 7, 14, 14, 8, 7, tzinfo=UTC)
        )

        record = ledger.record(
            JobResult("job-1", "node-a", "completed", "hello mesh", None, 1),
            validation_valid=True,
            validation_reason="ok",
            job_type="echo",
            version_metadata_ref="version-metadata.json",
            manifest_ref="manifests/local-job-1.json",
        )

        self.assertEqual(record.node_id, "node-a")
        self.assertEqual(record.job_id, "job-1")
        self.assertEqual(record.validation_valid, True)
        self.assertEqual(record.validation_reason, "ok")
        self.assertEqual(record.version_metadata_ref, "version-metadata.json")
        self.assertEqual(record.manifest_ref, "manifests/local-job-1.json")
        self.assertEqual(record.created_at, "2026-07-14T14:08:07Z")

    def test_non_manifest_record_omits_manifest_reference(self) -> None:
        ledger = ContributionLedger()

        record = ledger.record(
            JobResult("job-1", "node-a", "completed", "hello mesh", None, 1)
        )

        self.assertIsNone(record.manifest_ref)
        self.assertNotIn("manifest_ref", record.to_dict())

    def test_timestamp_rejects_non_utc_or_malformed_persisted_values(self) -> None:
        record = ContributionRecord(
            "node-a", "job-1", "completed", 1, created_at="2026-07-14T14:08:07Z"
        ).to_dict()
        for timestamp in (
            "2026-07-14T14:08:07+00:00",
            "2026-7-14T14:08:07Z",
            "not-a-time",
        ):
            with (
                self.subTest(timestamp=timestamp),
                self.assertRaisesRegex(LedgerPersistenceError, "created_at"),
            ):
                ContributionRecord.from_dict({**record, "created_at": timestamp})

    def test_missing_json_ledger_loads_as_empty(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"

            ledger, extra_fields = load_ledger_document(ledger_path)

        self.assertEqual(extra_fields, {})
        self.assertEqual(ledger.summary_for_node("node-a").total_result_count, 0)

    def test_json_ledger_appends_and_persists_records(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger = ContributionLedger()
            ledger.record(
                JobResult(
                    job_id="job-1",
                    node_id="node-a",
                    status="completed",
                    output="hello mesh",
                    error=None,
                    contribution_units=1,
                )
            )
            save_ledger_document(ledger_path, ledger, {"future_field": {"kept": True}})

            loaded, extra_fields = load_ledger_document(ledger_path)
            loaded.record(
                JobResult(
                    job_id="job-2",
                    node_id="node-a",
                    status="failed",
                    output=None,
                    error="boom",
                    contribution_units=5,
                )
            )
            save_ledger_document(ledger_path, loaded, extra_fields)
            reloaded, reloaded_extras = load_ledger_document(ledger_path)

        summary = reloaded.summary_for_node("node-a")
        self.assertEqual(reloaded_extras, {"future_field": {"kept": True}})
        self.assertEqual(summary.total_result_count, 2)
        self.assertEqual(summary.failed_job_count, 1)
        self.assertEqual(summary.total_contribution_units, 1)

    def test_malformed_json_ledger_raises_clear_error_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text("{not json", encoding="utf-8")

            with self.assertRaisesRegex(LedgerPersistenceError, "malformed"):
                load_ledger_document(ledger_path)

            self.assertEqual(ledger_path.read_text(encoding="utf-8"), "{not json")

    def test_existing_ledger_loader_rejects_missing_file_without_creating_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"

            with self.assertRaisesRegex(LedgerPersistenceError, "does not exist"):
                load_existing_ledger_document(ledger_path)

            self.assertFalse(ledger_path.exists())

    def test_summary_document_sorts_nodes_and_uses_ledger_semantics(self) -> None:
        ledger = ContributionLedger(
            [
                ContributionRecord("node-b", "job-1", "completed", 10, "ok"),
                ContributionRecord("node-a", "job-2", "failed", 0, "boom"),
                ContributionRecord("node-a", "job-3", "completed", 5, "ok"),
            ]
        )

        summary = ledger.summary_document("./local-ledger.json")

        self.assertEqual(
            summary,
            {
                "ledger_path": "./local-ledger.json",
                "record_count": 3,
                "completed_result_count": 2,
                "failed_result_count": 1,
                "total_contribution_units": 15,
                "nodes": [
                    {
                        "node_id": "node-a",
                        "record_count": 2,
                        "completed_result_count": 1,
                        "failed_result_count": 1,
                        "total_contribution_units": 5,
                    },
                    {
                        "node_id": "node-b",
                        "record_count": 1,
                        "completed_result_count": 1,
                        "failed_result_count": 0,
                        "total_contribution_units": 10,
                    },
                ],
            },
        )

    def test_save_ledger_document_uses_stable_json_and_cleans_temp_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "deep" / "nested" / "ledger.json"
            ledger = ContributionLedger(
                clock=lambda: datetime(2026, 7, 14, 14, 8, 7, tzinfo=UTC)
            )
            ledger.record(
                JobResult(
                    job_id="job-1",
                    node_id="node-a",
                    status="completed",
                    output="hello mesh",
                    error=None,
                    contribution_units=1,
                )
            )

            save_ledger_document(
                ledger_path, ledger, {"z_future": True, "a_note": "kept"}
            )
            raw = ledger_path.read_text(encoding="utf-8")

            expected_hash = result_hash(
                JobResult("job-1", "node-a", "completed", "hello mesh", None, 1)
            )
            self.assertEqual(
                raw,
                '{\n  "a_note": "kept",\n  "records": [\n    {\n      "contribution_units": 1,\n      "created_at": "2026-07-14T14:08:07Z",\n      "job_id": "job-1",\n      "job_type": null,\n      "message": "hello mesh",\n      "node_id": "node-a",\n      "result_hash": "'
                + expected_hash
                + '",\n      "status": "completed",\n      "validation_reason": null,\n      "validation_valid": null\n    }\n  ],\n  "version": 1,\n  "z_future": true\n}\n',
            )
            self.assertEqual(
                sorted(path.name for path in ledger_path.parent.iterdir()),
                ["ledger.json"],
            )

    def test_save_ledger_document_preserves_existing_file_on_atomic_replace_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text('{"original": true}\n', encoding="utf-8")
            ledger = ContributionLedger()
            ledger.record(JobResult("job-1", "node-a", "completed", "hello", None, 1))

            with (
                mock.patch(
                    "aethermesh_core.ledger.os.replace",
                    side_effect=OSError("replace failed"),
                ),
                self.assertRaisesRegex(LedgerPersistenceError, "replace failed"),
            ):
                save_ledger_document(ledger_path, ledger)

            self.assertEqual(
                ledger_path.read_text(encoding="utf-8"), '{"original": true}\n'
            )
            self.assertEqual(
                sorted(path.name for path in ledger_path.parent.iterdir()),
                ["ledger.json"],
            )

    def test_bool_version_is_rejected(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text(
                json.dumps({"version": True, "records": []}), encoding="utf-8"
            )

            with self.assertRaisesRegex(LedgerPersistenceError, "version 1"):
                load_existing_ledger_document(ledger_path)

    def test_save_ledger_document_uses_target_parent_temp_file_contract(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "deep" / "nested" / "ledger.json"
            ledger = ContributionLedger()
            calls: list[dict[str, object]] = []
            real_named_temporary_file = tempfile.NamedTemporaryFile

            def capture_named_temporary_file(*args, **kwargs):
                calls.append({"args": args, "kwargs": dict(kwargs)})
                return real_named_temporary_file(*args, **kwargs)

            with mock.patch(
                "aethermesh_core.ledger.tempfile.NamedTemporaryFile",
                side_effect=capture_named_temporary_file,
            ):
                save_ledger_document(ledger_path, ledger)

            self.assertEqual(len(calls), 1)
            self.assertEqual(calls[0]["args"], ("w",))
            self.assertEqual(calls[0]["kwargs"]["encoding"], "utf-8")
            self.assertEqual(calls[0]["kwargs"]["dir"], ledger_path.parent)
            self.assertEqual(calls[0]["kwargs"]["prefix"], ".ledger.json.")
            self.assertEqual(calls[0]["kwargs"]["suffix"], ".tmp")
            self.assertEqual(calls[0]["kwargs"]["delete"], False)


if __name__ == "__main__":
    unittest.main()
