import copy
import json
import os
import unittest
from importlib import metadata
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.local_json_helpers import canonical_json_hash
from aethermesh_core.result_hash import (
    canonical_result_document_hash,
    validate_validation_receipt_result_hash,
)
from aethermesh_core.validation_receipt_schema import (
    ValidationReceiptSchemaError,
    canonical_validation_receipt_hash,
    capture_validator_software_metadata,
    validate_validation_receipt_document,
    validation_receipt_id,
)


class ValidationReceiptSchemaTests(unittest.TestCase):
    def setUp(self) -> None:
        root = Path(__file__).resolve().parents[1]
        examples = root / "examples" / "validation-receipts"
        self.passing = json.loads(
            (examples / "local-echo-pass.json").read_text("utf-8")
        )
        self.failing = json.loads(
            (examples / "local-echo-fail.json").read_text("utf-8")
        )
        results = root / "examples" / "job-results"
        self.success_result = json.loads(
            (results / "local-echo-success.json").read_text("utf-8")
        )
        self.failed_result = json.loads(
            (results / "local-echo-failed.json").read_text("utf-8")
        )

    def test_passing_and_failing_examples_validate(self) -> None:
        self.assertIs(validate_validation_receipt_document(self.passing), self.passing)
        self.assertIs(validate_validation_receipt_document(self.failing), self.failing)
        self.assertEqual(self.passing["validation_status"], "pass")
        self.assertEqual(self.failing["validation_status"], "fail")
        self.assertEqual(self.passing["status"], "accepted")
        self.assertEqual(self.failing["status"], "rejected")
        self.assertEqual(
            self.failing["rejection_reason"],
            "output did not match deterministic echo expectation",
        )
        self.assertEqual(
            self.failing["lineage"]["prior_receipt_ids"],
            [self.passing["receipt_id"]],
        )
        self.assertEqual(self.failing["contributor_node_id"], "node.local-worker")
        self.assertEqual(self.failing["creator_node_id"], "node.local-creator")
        self.assertEqual(
            self.passing["manifest_reference"],
            {
                "manifest_id": self.passing["manifest_id"],
                "manifest_ref": "examples/job-envelopes/minimal-local-echo.json",
            },
        )
        self.assertEqual(
            self.failing["manifest_reference"],
            {
                "manifest_id": self.failing["manifest_id"],
                "manifest_ref": "examples/job-envelopes/complete-local-echo.json",
            },
        )
        for block in ("validation_method", "lineage", "contribution"):
            with self.subTest(block=block):
                self.assertEqual(
                    self.failing[block]["contributor_node_id"],
                    self.failing["contributor_node_id"],
                )
        validate_validation_receipt_result_hash(
            self.passing, canonical_result_document_hash(self.success_result)
        )
        validate_validation_receipt_result_hash(
            self.failing, canonical_result_document_hash(self.failed_result)
        )

    def test_capture_validator_software_uses_explicit_unknown_for_unsafe_builds(
        self,
    ) -> None:
        with (
            patch(
                "aethermesh_core.validation_receipt_schema.metadata.version",
                side_effect=metadata.PackageNotFoundError,
            ),
            patch.dict(os.environ, {"AETHERMESH_BUILD_ID": "/private/build"}),
        ):
            captured = capture_validator_software_metadata(
                validator_name="deterministic_fixture_replay", receipt_schema_version=8
            )

        self.assertEqual(captured["validator_build_identifier"], "unknown")
        self.assertEqual(captured["receipt_schema_version"], 8)

    def test_manifest_references_resolve_to_their_content_addressed_manifests(
        self,
    ) -> None:
        for receipt in (self.passing, self.failing):
            with self.subTest(receipt_id=receipt["receipt_id"]):
                reference = receipt["manifest_reference"]
                self.assertIsInstance(reference, dict)
                manifest_path = (
                    Path(__file__).resolve().parents[1] / reference["manifest_ref"]
                )
                manifest = json.loads(manifest_path.read_text("utf-8"))
                self.assertEqual(
                    canonical_json_hash(manifest, prefix="sha256:"),
                    reference["manifest_id"],
                )
                self.assertEqual(reference["manifest_id"], receipt["manifest_id"])

    def test_non_manifest_receipt_remains_valid(self) -> None:
        receipt = copy.deepcopy(self.failing)
        receipt["manifest_id"] = None
        receipt["manifest_reference"] = None
        receipt["validation_method"]["manifest_id"] = None
        receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
        self.assertIs(validate_validation_receipt_document(receipt), receipt)

    def test_manifest_reference_is_exact_and_local_when_present(self) -> None:
        mismatched = copy.deepcopy(self.passing)
        mismatched["manifest_reference"]["manifest_id"] = "sha256:" + "0" * 64
        mismatched["receipt_hash"] = canonical_validation_receipt_hash(mismatched)
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "must match receipt"):
            validate_validation_receipt_document(mismatched)

        missing = copy.deepcopy(self.passing)
        missing["manifest_reference"] = None
        missing["receipt_hash"] = canonical_validation_receipt_hash(missing)
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "is required"):
            validate_validation_receipt_document(missing)

    def test_required_fields_and_unknown_fields_are_rejected(self) -> None:
        missing = copy.deepcopy(self.passing)
        missing.pop("job_id")
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "missing: job_id"):
            validate_validation_receipt_document(missing)

        missing_method = copy.deepcopy(self.passing)
        missing_method.pop("validation_method")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: validation_method"
        ):
            validate_validation_receipt_document(missing_method)

        missing_validator_software = copy.deepcopy(self.passing)
        missing_validator_software.pop("validator_software")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: validator_software"
        ):
            validate_validation_receipt_document(missing_validator_software)

        missing_timestamp = copy.deepcopy(self.passing)
        missing_timestamp.pop("validated_at")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: validated_at"
        ):
            validate_validation_receipt_document(missing_timestamp)

        missing_contributor = copy.deepcopy(self.passing)
        missing_contributor.pop("contributor_node_id")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: contributor_node_id"
        ):
            validate_validation_receipt_document(missing_contributor)

        missing_model_expert = copy.deepcopy(self.passing)
        missing_model_expert.pop("model_expert_id")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: model_expert_id"
        ):
            validate_validation_receipt_document(missing_model_expert)

        missing_expert_version = copy.deepcopy(self.passing)
        missing_expert_version.pop("expert_version")
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "missing: expert_version"
        ):
            validate_validation_receipt_document(missing_expert_version)

        malformed_timestamp = copy.deepcopy(self.passing)
        malformed_timestamp["validated_at"] = "2026-07-13T12:00:00+00:00"
        malformed_timestamp["receipt_hash"] = canonical_validation_receipt_hash(
            malformed_timestamp
        )
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "validated_at must be a UTC timestamp"
        ):
            validate_validation_receipt_document(malformed_timestamp)

        blank = copy.deepcopy(self.passing)
        blank["job_id"] = ""
        with self.assertRaisesRegex(
            ValidationReceiptSchemaError, "job_id must be a non-empty identifier"
        ):
            validate_validation_receipt_document(blank)

        unknown = copy.deepcopy(self.passing)
        unknown["unreviewed_critical_field"] = "not silently accepted"
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "unsupported fields"):
            validate_validation_receipt_document(unknown)

    def test_validation_statuses_map_to_explicit_receipt_statuses(self) -> None:
        for state, status in (
            ("pass", "accepted"),
            ("fail", "rejected"),
            ("error", "rejected"),
            ("skipped", "rejected"),
        ):
            with self.subTest(state=state):
                receipt = copy.deepcopy(self.passing)
                receipt["validation_status"] = state
                receipt["status"] = status
                receipt["rejection_reason"] = (
                    None if status == "accepted" else "required validation did not pass"
                )
                receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
                self.assertIs(validate_validation_receipt_document(receipt), receipt)

    def test_invalid_or_mismatched_receipt_status_is_rejected(self) -> None:
        cases = (
            ("pending", None, "must be accepted or rejected"),
            ("accepted", "unexpected", "must be null"),
            ("rejected", "required validation failed", "does not match"),
        )
        for status, rejection_reason, error in cases:
            with self.subTest(status=status):
                receipt = copy.deepcopy(self.passing)
                receipt["status"] = status
                receipt["rejection_reason"] = rejection_reason
                receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
                with self.assertRaisesRegex(ValidationReceiptSchemaError, error):
                    validate_validation_receipt_document(receipt)

        receipt = copy.deepcopy(self.failing)
        receipt["rejection_reason"] = None
        receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "rejection_reason"):
            validate_validation_receipt_document(receipt)

    def test_identical_local_evidence_has_stable_id_and_hash(self) -> None:
        first = copy.deepcopy(self.passing)
        second = copy.deepcopy(self.passing)
        second["created_at"] = "2026-07-13T12:01:00.000000Z"
        second["validated_at"] = "2026-07-13T12:02:00.000000Z"
        self.assertEqual(first["receipt_id"], second["receipt_id"])
        self.assertEqual(first["receipt_id"], validation_receipt_id(first["work_id"]))
        self.assertEqual(
            canonical_validation_receipt_hash(first),
            canonical_validation_receipt_hash(second),
        )
        self.assertEqual(
            first["receipt_hash"], canonical_validation_receipt_hash(first)
        )

    def test_receipt_id_must_be_derived_from_work_id(self) -> None:
        receipt = copy.deepcopy(self.passing)
        receipt["receipt_id"] = "local-validation-receipt-unrelated-work"
        receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "match its work_id"):
            validate_validation_receipt_document(receipt)

    def test_job_id_must_match_work_id(self) -> None:
        receipt = copy.deepcopy(self.passing)
        receipt["job_id"] = "unrelated-job"
        receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "match its work_id"):
            validate_validation_receipt_document(receipt)

    def test_hash_mismatch_is_rejected(self) -> None:
        receipt = copy.deepcopy(self.passing)
        receipt["evidence"]["reason"] = "changed after hashing"
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "does not match"):
            validate_validation_receipt_document(receipt)

    def test_validator_software_metadata_is_hash_bound_to_work_lineage(self) -> None:
        receipt = copy.deepcopy(self.failing)
        receipt["validator_software"]["validator_version"] = "changed-after-validation"
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "receipt_hash"):
            validate_validation_receipt_document(receipt)

    def test_types_hashes_and_json_compatibility_are_strict(self) -> None:
        for field, value in (("schema_version", 5.0), ("validation_status", [])):
            with self.subTest(field=field):
                receipt = copy.deepcopy(self.passing)
                receipt[field] = value
                with self.assertRaises(ValidationReceiptSchemaError):
                    validate_validation_receipt_document(receipt)

        old_version = copy.deepcopy(self.passing)
        old_version["schema_version"] = 9
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "must be integer 10"):
            validate_validation_receipt_document(old_version)

        receipt = copy.deepcopy(self.passing)
        receipt["lineage"]["input_hashes"] = ["sha256:not-a-digest"]
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "content-addressed"):
            validate_validation_receipt_document(receipt)

        for result_hash in (None, "c" * 64, "sha256:not-a-digest"):
            with self.subTest(result_hash=result_hash):
                receipt = copy.deepcopy(self.passing)
                receipt["result_hash"] = result_hash
                with self.assertRaisesRegex(
                    ValidationReceiptSchemaError, "SHA-256 digest"
                ):
                    validate_validation_receipt_document(receipt)

        receipt = copy.deepcopy(self.passing)
        receipt["not_json"] = {float("nan")}
        with self.assertRaisesRegex(ValidationReceiptSchemaError, "JSON-compatible"):
            canonical_validation_receipt_hash(receipt)

    def test_local_references_cannot_leak_machine_paths(self) -> None:
        cases = (
            ("lineage", "source_manifest_refs", "/Users/example/private.json"),
            ("contribution", "contribution_manifest_ref", "../private.json"),
            ("evidence", "log_path", "C:\\Users\\example\\private.log"),
            ("evidence", "artifact_path", "https://example.test/result.json"),
        )
        for block, field, value in cases:
            with self.subTest(block=block, field=field):
                receipt = copy.deepcopy(self.passing)
                receipt[block][field] = [value] if block == "lineage" else value
                with self.assertRaisesRegex(ValidationReceiptSchemaError, "relative"):
                    validate_validation_receipt_document(receipt)

    def test_reason_is_required_validation_evidence(self) -> None:
        for value in (None, ""):
            with self.subTest(value=value):
                receipt = copy.deepcopy(self.passing)
                receipt["evidence"]["reason"] = value
                with self.assertRaisesRegex(ValidationReceiptSchemaError, "non-empty"):
                    validate_validation_receipt_document(receipt)

    def test_next_local_action_is_required_validation_evidence(self) -> None:
        for value in (None, ""):
            with self.subTest(value=value):
                receipt = copy.deepcopy(self.failing)
                receipt["evidence"]["next_local_action"] = value
                receipt["receipt_hash"] = canonical_validation_receipt_hash(receipt)
                with self.assertRaisesRegex(
                    ValidationReceiptSchemaError, "next_local_action"
                ):
                    validate_validation_receipt_document(receipt)

    def test_lineage_and_attribution_ids_reject_whitespace(self) -> None:
        nullable = copy.deepcopy(self.passing)
        nullable["contribution"]["submitter_id"] = None
        nullable["receipt_hash"] = canonical_validation_receipt_hash(nullable)
        self.assertIs(validate_validation_receipt_document(nullable), nullable)

        cases = (
            ("lineage", "parent_work_ids", ["not an id"]),
            ("lineage", "prior_receipt_ids", ["receipt\nleak"]),
            ("contribution", "submitter_id", "/Users/example/private id"),
            ("contribution", "claimed_role", "validator\nsecret"),
            ("contribution", "contributor_node_id", ""),
        )
        for block, field, value in cases:
            with self.subTest(block=block, field=field):
                receipt = copy.deepcopy(self.passing)
                receipt[block][field] = value
                with self.assertRaisesRegex(ValidationReceiptSchemaError, "identifier"):
                    validate_validation_receipt_document(receipt)
