import copy
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.job_result_schema import (
    JobResultSchemaError,
    validate_job_result_document,
)
from aethermesh_core.result_hash import canonical_result_document_hash


class JobResultSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        self.success = json.loads(
            (root / "examples/job-results/local-echo-success.json").read_text("utf-8")
        )
        self.failed = json.loads(
            (root / "examples/job-results/local-echo-failed.json").read_text("utf-8")
        )
        self.pending = json.loads(
            (
                root / "examples/job-results/local-echo-pending-validation.json"
            ).read_text("utf-8")
        )

    def test_success_and_failed_examples_validate(self) -> None:
        self.assertIs(validate_job_result_document(self.success), self.success)
        self.assertIs(validate_job_result_document(self.failed), self.failed)
        self.assertEqual(self.failed["validation_status"], "failed")

    def test_pending_validation_is_explicit_and_retains_provenance(self) -> None:
        self.assertIs(validate_job_result_document(self.pending), self.pending)
        self.assertEqual(self.pending["validation_status"], "pending")
        self.assertEqual(
            self.pending["validation_receipt_id"],
            "local-validation-receipt-echo-pending-001",
        )
        self.assertIn(
            self.pending["validation_receipt_id"],
            self.pending["references"]["validation_receipt_ids"],
        )
        self.assertIsNone(self.pending["result_hash"])
        self.assertEqual(self.pending["creator_node_id"], "node.local-creator")
        self.assertEqual(
            self.pending["manifest_id"], self.pending["references"]["manifest_hash"]
        )
        self.assertEqual(self.pending["lineage"]["parent_job_ids"], [])
        self.assertEqual(
            self.pending["contribution"]["creator_node_id"],
            self.pending["creator_node_id"],
        )

    def test_local_output_attribution_round_trips_with_authorizing_evidence(
        self,
    ) -> None:
        output = copy.deepcopy(self.success)
        output["lineage"]["parent_job_ids"] = ["local-parent-job-001"]
        output["contribution"]["upstream_lineage_sources"] = ["local-parent-job-001"]
        output["result_hash"] = canonical_result_document_hash(output)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "local-output.json"
            path.write_text(json.dumps(output, sort_keys=True), encoding="utf-8")
            reloaded = json.loads(path.read_text(encoding="utf-8"))

        self.assertIs(validate_job_result_document(reloaded), reloaded)
        self.assertEqual(reloaded["creator_node_id"], "node.local-creator")
        self.assertEqual(
            reloaded["manifest_id"],
            "sha256:72426ae139e40863ceb9ea2896c01d33114c226f944659de08eba371bbe8791c",
        )
        self.assertEqual(
            reloaded["manifest_id"], reloaded["references"]["manifest_hash"]
        )
        self.assertEqual(
            reloaded["validation_receipt_id"],
            "local-validation-receipt-local-job-echo-001",
        )
        self.assertIn(
            reloaded["validation_receipt_id"],
            reloaded["references"]["validation_receipt_ids"],
        )
        self.assertEqual(
            reloaded["lineage"]["parent_job_ids"], ["local-parent-job-001"]
        )
        self.assertEqual(
            reloaded["contribution"]["creator_node_id"], reloaded["creator_node_id"]
        )
        self.assertEqual(
            reloaded["contribution"]["upstream_lineage_sources"],
            reloaded["lineage"]["parent_job_ids"],
        )

        for label, mutation, message in (
            (
                "missing",
                lambda document: document.pop("creator_node_id"),
                "missing: creator_node_id",
            ),
            (
                "empty",
                lambda document: document.__setitem__("creator_node_id", ""),
                "non-empty identifier",
            ),
            (
                "mismatched",
                lambda document: document["contribution"].__setitem__(
                    "creator_node_id", "node.local-other"
                ),
                "must match the top-level record",
            ),
        ):
            with self.subTest(label=label):
                invalid = copy.deepcopy(reloaded)
                mutation(invalid)
                with self.assertRaisesRegex(JobResultSchemaError, message):
                    validate_job_result_document(invalid)

    def test_model_expert_id_and_version_are_required_stable_local_provenance(
        self,
    ) -> None:
        self.assertEqual(
            self.success["model_expert_id"], "local-runner:aethermesh-local-runner@1"
        )
        self.assertEqual(self.success["expert_version"], "1")
        self.assertIs(validate_job_result_document(self.success), self.success)

        for field in ("model_expert_id", "expert_version"):
            with self.subTest(field=field):
                missing = copy.deepcopy(self.success)
                missing.pop(field)
                with self.assertRaisesRegex(JobResultSchemaError, f"missing: {field}"):
                    validate_job_result_document(missing)

        invalid = copy.deepcopy(self.success)
        invalid["expert_version"] = "ambiguous local worker"
        with self.assertRaisesRegex(JobResultSchemaError, "expert_version"):
            validate_job_result_document(invalid)

    def test_required_attribution_lineage_manifest_and_validation_fields_reject_omission(
        self,
    ) -> None:
        for field in (
            "creator_node_id",
            "capability",
            "model_expert_id",
            "expert_version",
            "executor_node_id",
            "manifest_id",
            "references",
            "error_summary",
            "validation_status",
            "validation_receipt_id",
            "validator_node_id",
            "lineage",
            "contribution",
            "result_hash",
            "reported_at",
        ):
            with self.subTest(field=field):
                document = copy.deepcopy(self.success)
                document.pop(field)
                with self.assertRaisesRegex(JobResultSchemaError, f"missing: {field}"):
                    validate_job_result_document(document)

    def test_missing_or_stale_result_hash_is_rejected(self) -> None:
        missing = copy.deepcopy(self.success)
        missing.pop("result_hash")
        with self.assertRaisesRegex(JobResultSchemaError, "missing: result_hash"):
            validate_job_result_document(missing)

        stale = copy.deepcopy(self.success)
        stale["summary"] = "mutated after hashing"
        with self.assertRaisesRegex(JobResultSchemaError, "does not match"):
            validate_job_result_document(stale)

    def test_invalid_status_and_missing_identifiers_are_rejected(self) -> None:
        old_version = copy.deepcopy(self.success)
        old_version["schema_version"] = 1
        with self.assertRaisesRegex(JobResultSchemaError, "must be integer 10"):
            validate_job_result_document(old_version)

        invalid_status = copy.deepcopy(self.success)
        invalid_status["status"] = "complete"
        with self.assertRaisesRegex(JobResultSchemaError, "status is unsupported"):
            validate_job_result_document(invalid_status)

        for field in ("result_id", "job_id", "task_id"):
            with self.subTest(field=field):
                document = copy.deepcopy(self.success)
                document[field] = ""
                with self.assertRaisesRegex(
                    JobResultSchemaError, "non-empty identifier"
                ):
                    validate_job_result_document(document)

    def test_manifest_ref_is_a_stable_fallback_when_hash_is_unavailable(self) -> None:
        fallback = copy.deepcopy(self.pending)
        manifest_ref = (
            "data/job-submissions/local-job-0123456789abcdef0123456789abcdef.json"
        )
        fallback["manifest_id"] = manifest_ref
        fallback["references"]["manifest_hash"] = None
        fallback["references"]["manifest_ref"] = manifest_ref
        fallback["lineage"]["input_manifest_ids"] = [manifest_ref]
        self.assertIs(validate_job_result_document(fallback), fallback)

        missing_linkage = copy.deepcopy(fallback)
        missing_linkage["references"]["manifest_ref"] = None
        with self.assertRaisesRegex(
            JobResultSchemaError, "exactly one of manifest_hash or manifest_ref"
        ):
            validate_job_result_document(missing_linkage)

        mutable_ref = copy.deepcopy(fallback)
        mutable_ref["manifest_id"] = "draft-work-manifest"
        mutable_ref["references"]["manifest_ref"] = "draft-work-manifest"
        mutable_ref["lineage"]["input_manifest_ids"] = ["draft-work-manifest"]
        with self.assertRaisesRegex(
            JobResultSchemaError, "immutable local work manifest"
        ):
            validate_job_result_document(mutable_ref)

    def test_output_payload_requires_one_safe_delivery_mode_for_success(self) -> None:
        missing = copy.deepcopy(self.success)
        missing.pop("output_payload")
        with self.assertRaisesRegex(JobResultSchemaError, "missing: output_payload"):
            validate_job_result_document(missing)

        absent = copy.deepcopy(self.success)
        absent["output_payload"] = {
            "inline_payload": None,
            "payload_ref": None,
            "payload_digest": None,
        }
        with self.assertRaisesRegex(
            JobResultSchemaError, "must include an output payload"
        ):
            validate_job_result_document(absent)

        referenced = copy.deepcopy(self.success)
        referenced["output_payload"] = {
            "inline_payload": None,
            "payload_ref": "data/job-output-payloads/local-echo-001.json",
            "payload_digest": "sha256:" + "e" * 64,
        }
        referenced["result_hash"] = canonical_result_document_hash(referenced)
        self.assertIs(validate_job_result_document(referenced), referenced)

        unsafe_reference = copy.deepcopy(referenced)
        unsafe_reference["output_payload"]["payload_ref"] = (
            "https://example.test/output"
        )
        with self.assertRaisesRegex(JobResultSchemaError, "relative local paths"):
            validate_job_result_document(unsafe_reference)

    def test_rejects_inconsistent_runtime_and_attribution_values(self) -> None:
        cases = (
            ("duration_ms", 124, "must match"),
            ("summary", "x" * 513, "up to 512"),
            ("validation_status", "unknown", "is unsupported"),
        )
        for field, value, message in cases:
            with self.subTest(field=field):
                document = copy.deepcopy(self.success)
                document[field] = value
                with self.assertRaisesRegex(JobResultSchemaError, message):
                    validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["contribution"]["executor_node_id"] = "node.other"
        with self.assertRaisesRegex(
            JobResultSchemaError, "must match the top-level record"
        ):
            validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["failure_reasons"].pop("missing_artifact")
        with self.assertRaisesRegex(JobResultSchemaError, "missing: missing_artifact"):
            validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["future"] = True
        with self.assertRaisesRegex(JobResultSchemaError, "unsupported: future"):
            validate_job_result_document(document)

    def test_outcome_statuses_and_local_artifact_reference_rules(self) -> None:
        for status in (
            "succeeded",
            "failed",
            "timed_out",
            "cancelled",
            "validation_failed",
            "partially_completed",
        ):
            with self.subTest(status=status):
                document = copy.deepcopy(self.success)
                document["status"] = status
                document["error_summary"] = (
                    None if status == "succeeded" else "local outcome"
                )
                document["result_hash"] = canonical_result_document_hash(document)
                self.assertIs(validate_job_result_document(document), document)

        for reference in (
            "https://dashboard.example/result",
            "https:dashboard.example/result",
            "file:/Users/example/private.log",
            "C:/Users/example/private.log",
        ):
            with self.subTest(reference=reference):
                document = copy.deepcopy(self.success)
                document["references"]["artifact_refs"] = [reference]
                with self.assertRaisesRegex(
                    JobResultSchemaError, "relative local paths"
                ):
                    validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["references"]["artifact_refs"] = ["A" * 100_000]
        document["result_hash"] = canonical_result_document_hash(document)
        with patch(
            "aethermesh_core.job_result_schema.re.match",
            side_effect=AssertionError(
                "artifact references must not use regex matching"
            ),
        ):
            self.assertIs(validate_job_result_document(document), document)

        document = copy.deepcopy(self.failed)
        document["error_summary"] = None
        with self.assertRaisesRegex(
            JobResultSchemaError, "required when the job did not succeed"
        ):
            validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["error_summary"] = "contradictory failure detail"
        with self.assertRaisesRegex(
            JobResultSchemaError, "must be null when the job succeeded"
        ):
            validate_job_result_document(document)

    def test_unvalidated_results_cannot_claim_a_durable_result_hash(self) -> None:
        document = copy.deepcopy(self.pending)
        document["result_hash"] = self.success["result_hash"]
        with self.assertRaisesRegex(JobResultSchemaError, "must be null"):
            validate_job_result_document(document)

        document = copy.deepcopy(self.success)
        document["result_hash"] = None
        with self.assertRaisesRegex(
            JobResultSchemaError, "required after final validation"
        ):
            validate_job_result_document(document)


if __name__ == "__main__":
    unittest.main()
