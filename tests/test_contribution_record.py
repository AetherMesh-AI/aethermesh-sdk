import copy
import json
import re
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any
from unittest.mock import patch

from aethermesh_core.contribution_record import (
    PHASE_1_JOB_CAPABILITIES,
    ContributionRecordError,
    apply_local_validation_receipt,
    new_unvalidated_validation,
    record_validated_contribution,
    validate_contribution_record,
    validate_local_contribution_record,
)
from aethermesh_core.local_json_helpers import canonical_json_hash
from aethermesh_core.runtime_service import LOCAL_CAPABILITY_DEFINITIONS


class ContributionRecordTests(unittest.TestCase):
    def setUp(self) -> None:
        self.root = Path(__file__).resolve().parents[1]
        self.minimal = self._load("minimal-local-echo.json")
        self.failed = self._load("failed-local-echo.json")

    def test_minimal_local_record_validates_against_the_versioned_contract(
        self,
    ) -> None:
        self.assertIs(validate_contribution_record(self.minimal), self.minimal)
        self.assertEqual(self.minimal["job_id"], "local-job-echo-001")
        self.assertEqual(self.minimal["job_type"], "echo")
        self.assertEqual(self.minimal["capability"], "work.echo")
        self.assertEqual(
            self.minimal["validation_receipt_id"],
            "local-validation-receipt-local-job-echo-001",
        )
        self.assertEqual(self.minimal["result_hash_algorithm"], "sha256")
        self.assertEqual(
            self.minimal["result_hash"],
            "sha256:54943ac2f5f9b8f921117791792e05efe0cc141a2f8a64719333cac2829be294",
        )
        self.assertEqual(self.minimal["validation"]["status"], "valid")
        self.assertEqual(self.minimal["lineage"]["parent_contribution_ids"], [])
        self.assertIs(
            validate_local_contribution_record(self.minimal, self.root), self.minimal
        )

    def test_linked_failed_record_retains_manifest_lineage_and_attribution(
        self,
    ) -> None:
        receipt = json.loads(
            (self.root / self.failed["validation"]["validation_receipt_ref"]).read_text(
                encoding="utf-8"
            )
        )
        self.assertIs(validate_contribution_record(self.failed), self.failed)
        self.assertEqual(self.failed["validation"]["status"], "invalid")
        work_manifest = self._load_job_envelope("complete-local-echo.json")
        self.assertEqual(self.failed["job_id"], work_manifest["job_id"])
        self.assertEqual(self.failed["job_id"], receipt["job_id"])
        self.assertEqual(self.failed["validation_receipt_id"], receipt["receipt_id"])
        self.assertEqual(receipt["work_id"], receipt["job_id"])

        self.assertEqual(
            self.failed["validation"]["validation_receipt_ref"],
            "examples/validation-receipts/local-echo-fail.json",
        )
        self.assertEqual(
            self.failed["lineage"]["parent_contribution_ids"],
            ["contribution.echo-0001"],
        )
        self.assertEqual(
            self.failed["validation"]["validator_node_id"], receipt["validator_id"]
        )
        self.assertEqual(
            self.failed["validation"]["failure_reason"], receipt["rejection_reason"]
        )
        self.assertIn(receipt["result_hash"], self.failed["lineage"]["output_hashes"])
        self.assertEqual(
            self.failed["lineage"]["contributor_node_id"],
            self.failed["contributor_node_id"],
        )
        self.assertNotEqual(
            self.failed["creator_node_id"], self.failed["contributor_node_id"]
        )
        self.assertEqual(self.failed["attribution"]["author_id"], "node.local-worker")
        self.assertIs(
            validate_local_contribution_record(self.failed, self.root), self.failed
        )

    def test_records_one_accepted_contribution_with_exact_evidence_and_attribution(
        self,
    ) -> None:
        accepted = copy.deepcopy(self.minimal)
        accepted["lineage"]["parent_contribution_ids"] = ["contribution.parent-001"]
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "audit" / "contributions.jsonl"
            entry = record_validated_contribution(
                accepted,
                self.root,
                journal_path,
                clock=lambda: datetime(2026, 7, 13, 12, 0, 2, tzinfo=UTC),
            )

            self.assertEqual(
                entry["manifest_id"],
                "sha256:72426ae139e40863ceb9ea2896c01d33114c226f944659de08eba371bbe8791c",
            )
            self.assertEqual(entry["creator_node_id"], "node.local-creator")
            self.assertEqual(entry["contributor_node_id"], "node.local-worker")
            self.assertEqual(entry["validator_node_id"], "node.local-validator")
            self.assertEqual(
                entry["validation_receipt_id"],
                "local-validation-receipt-local-job-echo-001",
            )
            self.assertEqual(
                entry["lineage_parent_contribution_ids"], ["contribution.parent-001"]
            )
            self.assertEqual(entry["contribution"]["job_id"], "local-job-echo-001")
            self.assertEqual(
                entry["contribution"]["manifest_links"]["work_manifest_ref"],
                "examples/job-envelopes/minimal-local-echo.json",
            )
            self.assertEqual(
                entry["contribution"]["validation"]["validation_receipt_ref"],
                "examples/validation-receipts/local-echo-pass.json",
            )
            self.assertEqual(
                entry["contribution"]["attribution"],
                {
                    "author_id": "node.local-worker",
                    "author_kind": "node",
                    "role": "worker",
                    "declared_tool_or_runtime": "aethermesh-core",
                    "creation_mode": "automatic",
                },
            )
            self.assertEqual(entry["recorded_at"], "2026-07-13T12:00:02Z")
            self.assertIsNone(entry["prior_entry_hash"])
            self.assertRegex(entry["entry_hash"], r"^sha256:[0-9a-f]{64}$")
            self.assertEqual(
                len(journal_path.read_text(encoding="utf-8").splitlines()), 1
            )
            audit_entries = [
                json.loads(line)
                for line in (journal_path.parent / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(audit_entries), 1)
            audit = audit_entries[0]
            self.assertEqual(audit["validation_status"], "recorded")
            self.assertEqual(audit["creator_node_id"], "node.local-creator")
            self.assertEqual(audit["actor_node_id"], "node.local-worker")
            self.assertEqual(audit["manifest_id"], entry["manifest_id"])
            self.assertEqual(
                audit["manifest_ref"],
                "examples/job-envelopes/minimal-local-echo.json",
            )
            self.assertEqual(
                audit["validation_receipt_ref"],
                "examples/validation-receipts/local-echo-pass.json",
            )
            self.assertEqual(
                audit["validation_receipt_id"], entry["validation_receipt_id"]
            )
            self.assertEqual(audit["lineage_parent_ids"], ["contribution.parent-001"])
            self.assertEqual(
                audit["contribution_attribution_ids"], [accepted["record_id"]]
            )
            self.assertEqual(audit["event_id"], audit["local_run_id"])

    def test_failed_or_missing_validation_records_nothing(self) -> None:
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "contributions.jsonl"
            with self.assertRaisesRegex(
                ContributionRecordError,
                "requires an accepted passed validation receipt",
            ):
                record_validated_contribution(self.failed, self.root, journal_path)
            self.assertFalse(journal_path.exists())

            missing = copy.deepcopy(self.minimal)
            missing["validation"]["validation_receipt_ref"] = None
            missing["manifest_links"]["validation_manifest_ref"] = None
            missing["manifest_links"]["work_manifest_ref"] = None
            with self.assertRaisesRegex(ContributionRecordError, "receipt reference"):
                record_validated_contribution(missing, self.root, journal_path)
            self.assertFalse(journal_path.exists())

            malformed = copy.deepcopy(self.minimal)
            malformed_receipt_ref = "examples/contributions/minimal-local-echo.json"
            malformed["validation"]["validation_receipt_ref"] = malformed_receipt_ref
            malformed["validation"]["status_history"][-1]["validation_receipt_ref"] = (
                malformed_receipt_ref
            )
            malformed["manifest_links"]["validation_manifest_ref"] = (
                malformed_receipt_ref
            )
            with self.assertRaisesRegex(
                ContributionRecordError, "receipt file is invalid"
            ):
                record_validated_contribution(malformed, self.root, journal_path)
            self.assertFalse(journal_path.exists())
            audit_entries = [
                json.loads(line)
                for line in (Path(directory) / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(
                [entry["validation_status"] for entry in audit_entries],
                ["rejected", "validation_failed", "validation_failed"],
            )
            self.assertEqual(
                audit_entries[0]["validation_receipt_id"],
                self.failed["validation_receipt_id"],
            )
            self.assertEqual(
                audit_entries[0]["manifest_id"],
                canonical_json_hash(
                    self._load_job_envelope("complete-local-echo.json"),
                    prefix="sha256:",
                ),
            )
            with self.assertRaisesRegex(ContributionRecordError, "missing"):
                record_validated_contribution({}, self.root, journal_path)
            fallback_audit = json.loads(
                (Path(directory) / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[-1]
            )
            self.assertEqual(fallback_audit["actor_node_id"], "local-ledger")
            self.assertIsNone(fallback_audit["creator_node_id"])
            self.assertEqual(fallback_audit["validation_status"], "validation_failed")

            non_json = {
                "record_id": {"not-json"},
                "creator_node_id": "secret-creator",
                "contributor_node_id": "secret-contributor",
                "job_id": "secret-job",
                "validation_receipt_id": "secret-receipt",
            }
            with self.assertRaisesRegex(ContributionRecordError, "missing"):
                record_validated_contribution(non_json, self.root, journal_path)
            non_json_audit = json.loads(
                (Path(directory) / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[-1]
            )
            self.assertEqual(non_json_audit["validation_status"], "validation_failed")
            self.assertEqual(non_json_audit["actor_node_id"], "local-ledger")
            self.assertIsNone(non_json_audit["creator_node_id"])
            self.assertNotIn("work_id", non_json_audit)
            self.assertNotIn("validation_receipt_id", non_json_audit)

            unsafe_rejected = copy.deepcopy(self.failed)
            unsafe_rejected["validation"]["validation_receipt_ref"] = "../../secret"
            with self.assertRaisesRegex(ContributionRecordError, "safe local"):
                record_validated_contribution(unsafe_rejected, self.root, journal_path)
            unsafe_audit = json.loads(
                (Path(directory) / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()[-1]
            )
            self.assertEqual(unsafe_audit["validation_status"], "validation_failed")
            self.assertNotIn("validation_receipt_ref", unsafe_audit)

    def test_duplicate_validated_manifest_is_not_recorded_twice(self) -> None:
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "contributions.jsonl"
            first = record_validated_contribution(self.minimal, self.root, journal_path)
            duplicate = record_validated_contribution(
                self.minimal, self.root, journal_path
            )

            self.assertEqual(duplicate, first)
            self.assertEqual(
                len(journal_path.read_text(encoding="utf-8").splitlines()), 1
            )
            audit_entries = [
                json.loads(line)
                for line in (Path(directory) / "contribution-ledger-updates.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
            ]
            self.assertEqual(len(audit_entries), 2)
            self.assertEqual(audit_entries[1]["validation_status"], "already_recorded")
            self.assertEqual(audit_entries[1]["manifest_id"], first["manifest_id"])

            prior = json.loads(journal_path.read_text(encoding="utf-8"))
            prior["manifest_id"] = "sha256:" + "b" * 64
            prior["entry_hash"] = canonical_json_hash(
                {key: value for key, value in prior.items() if key != "entry_hash"},
                prefix="sha256:",
            )
            journal_path.write_text(json.dumps(prior) + "\n", encoding="utf-8")
            appended = record_validated_contribution(
                self.minimal, self.root, journal_path
            )
            self.assertEqual(appended["prior_entry_hash"], prior["entry_hash"])
            self.assertEqual(
                len(journal_path.read_text(encoding="utf-8").splitlines()), 2
            )

    def test_concurrent_duplicate_submission_is_recorded_once(self) -> None:
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "contributions.jsonl"
            barrier = threading.Barrier(2)
            original_validate = validate_local_contribution_record
            errors: list[Exception] = []

            def synchronized_validate(
                document: object, local_root: Path
            ) -> dict[str, Any]:
                contribution = original_validate(document, local_root)
                barrier.wait()
                return contribution

            def record() -> None:
                try:
                    record_validated_contribution(self.minimal, self.root, journal_path)
                except (
                    Exception  # noqa: BLE001 - justification: Surface every worker failure in the main thread.
                ) as exc:  # pragma: no cover - justification: failure capture
                    errors.append(exc)

            with patch(
                "aethermesh_core.contribution_record.validate_local_contribution_record",
                side_effect=synchronized_validate,
            ):
                threads = [threading.Thread(target=record) for _ in range(2)]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join()

            self.assertEqual(errors, [])
            self.assertEqual(
                len(journal_path.read_text(encoding="utf-8").splitlines()), 1
            )

    def test_rejects_tampered_or_malformed_journal_before_appending(self) -> None:
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "contributions.jsonl"
            record_validated_contribution(self.minimal, self.root, journal_path)
            entry = json.loads(journal_path.read_text(encoding="utf-8"))
            entry["creator_node_id"] = "node.tampered"
            journal_path.write_text(json.dumps(entry) + "\n", encoding="utf-8")

            with self.assertRaisesRegex(ContributionRecordError, "invalid hash"):
                record_validated_contribution(self.minimal, self.root, journal_path)

            journal_path.write_text("not-json\n", encoding="utf-8")
            with self.assertRaisesRegex(ContributionRecordError, "malformed"):
                record_validated_contribution(self.minimal, self.root, journal_path)

    def test_recording_clock_must_be_timezone_aware(self) -> None:
        with TemporaryDirectory() as directory:
            journal_path = Path(directory) / "contributions.jsonl"
            with self.assertRaisesRegex(ContributionRecordError, "timezone-aware"):
                record_validated_contribution(
                    self.minimal,
                    self.root,
                    journal_path,
                    clock=lambda: datetime(2026, 7, 13, 12, 0, 2),  # noqa: DTZ001 - justification: Exercise rejection of naive clocks.
                )
            self.assertFalse(journal_path.exists())

    def test_local_receipt_reference_rejects_missing_or_synthetic_receipts(
        self,
    ) -> None:
        record = copy.deepcopy(self.failed)
        record["validation"]["validation_receipt_ref"] = (
            "examples/validation-receipts/missing.json"
        )
        record["validation"]["status_history"][-1]["validation_receipt_ref"] = (
            "examples/validation-receipts/missing.json"
        )
        record["manifest_links"]["validation_manifest_ref"] = (
            "examples/validation-receipts/missing.json"
        )
        with self.assertRaisesRegex(ContributionRecordError, "does not exist"):
            validate_local_contribution_record(record, self.root)

        record = copy.deepcopy(self.failed)
        record["validation_receipt_id"] = "local-validation-receipt-local-job-fake"
        with self.assertRaisesRegex(ContributionRecordError, "must match"):
            validate_contribution_record(record)

        with TemporaryDirectory() as directory:
            local_root = Path(directory)
            (local_root / "receipt.json").symlink_to(
                self.root / "examples/validation-receipts/local-echo-fail.json"
            )
            record = copy.deepcopy(self.failed)
            record["validation"]["validation_receipt_ref"] = "receipt.json"
            record["validation"]["status_history"][-1]["validation_receipt_ref"] = (
                "receipt.json"
            )
            record["manifest_links"]["validation_manifest_ref"] = "receipt.json"
            with self.assertRaisesRegex(ContributionRecordError, "escapes local root"):
                validate_local_contribution_record(record, local_root)

    def test_local_receipt_evidence_must_match_validation_and_manifest(self) -> None:
        cases = (
            (
                lambda record: record["manifest_links"].update(
                    validation_manifest_ref="examples/validation-receipts/local-echo-fail.json"
                ),
                "validation manifest reference",
            ),
            (
                lambda record: record["validation"].update(
                    status="invalid", failure_reason="synthetic failure"
                ),
                "state must match its latest history entry",
            ),
            (
                lambda record: (
                    record["validation"].update(validated_at="2026-07-13T12:00:02Z"),
                    record["validation"]["status_history"][-1].update(
                        validated_at="2026-07-13T12:00:02Z"
                    ),
                ),
                "validated_at does not match",
            ),
            (
                lambda record: (
                    record["validation"].update(validated_at=None),
                    record["validation"]["status_history"][-1].update(
                        validated_at=None
                    ),
                ),
                "validated_at is required",
            ),
            (
                lambda record: record["manifest_links"].update(work_manifest_ref=None),
                "work_manifest_ref is required",
            ),
            (
                lambda record: record["manifest_links"].update(
                    work_manifest_ref="examples/job-envelopes/complete-local-echo.json"
                ),
                "work manifest job_id does not match",
            ),
            (
                lambda record: record.update(
                    work_type="hash", job_type="hash", capability="work.hash"
                ),
                "work manifest job_type does not match",
            ),
        )
        for mutate, expected in cases:
            with self.subTest(expected=expected):
                record = copy.deepcopy(self.minimal)
                mutate(record)
                with self.assertRaisesRegex(ContributionRecordError, expected):
                    validate_local_contribution_record(record, self.root)

        with TemporaryDirectory() as directory:
            local_root = Path(directory)
            receipt_ref = self.minimal["validation"]["validation_receipt_ref"]
            manifest_ref = self.minimal["manifest_links"]["work_manifest_ref"]
            result_ref = self.minimal["source"]["local_source_path"]
            for reference in (receipt_ref, manifest_ref, result_ref):
                assert isinstance(reference, str)
                target = local_root / reference
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((self.root / reference).read_bytes())
            assert isinstance(manifest_ref, str)
            manifest_path = local_root / manifest_ref
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["creator_node_id"] = "node.other-creator"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ContributionRecordError, "creator_node_id"):
                validate_local_contribution_record(self.minimal, local_root)

            manifest["creator_node_id"] = self.minimal["creator_node_id"]
            manifest["created_at"] = "2026-07-12T12:00:01Z"
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(ContributionRecordError, "manifest_id"):
                record_validated_contribution(
                    self.minimal, local_root, local_root / "contributions.jsonl"
                )

        record = copy.deepcopy(self.failed)
        record["validation"]["failure_reason"] = "synthetic failure"
        record["validation"]["status_history"][-1]["failure_reason"] = (
            "synthetic failure"
        )
        with self.assertRaisesRegex(ContributionRecordError, "rejection_reason"):
            validate_local_contribution_record(record, self.root)

        record = copy.deepcopy(self.minimal)
        record["validation"]["validator_node_id"] = "node.other-validator"
        with self.assertRaisesRegex(ContributionRecordError, "latest history entry"):
            validate_contribution_record(record)

    def test_unvalidated_shape_keeps_optional_validation_fields_nullable(self) -> None:
        record = copy.deepcopy(self.minimal)
        record["validation"] = new_unvalidated_validation()
        self.assertIs(validate_contribution_record(record), record)
        with self.assertRaisesRegex(
            ContributionRecordError, "required for local evidence"
        ):
            validate_local_contribution_record(record, self.root)

    def test_local_receipt_updates_unvalidated_record_without_overwriting_evidence(
        self,
    ) -> None:
        record = copy.deepcopy(self.minimal)
        record["validation"] = new_unvalidated_validation()
        record["manifest_links"]["validation_manifest_ref"] = None
        updated = apply_local_validation_receipt(
            record, self.root, "examples/validation-receipts/local-echo-pass.json"
        )
        self.assertEqual(updated["validation"]["status"], "valid")
        self.assertEqual(
            updated["validation"]["validation_receipt_ref"],
            "examples/validation-receipts/local-echo-pass.json",
        )
        self.assertEqual(len(updated["validation"]["status_history"]), 2)
        self.assertEqual(updated["creator_node_id"], record["creator_node_id"])
        self.assertEqual(
            updated["manifest_links"]["work_manifest_ref"],
            record["manifest_links"]["work_manifest_ref"],
        )
        self.assertEqual(updated["lineage"], record["lineage"])
        self.assertEqual(updated["attribution"], record["attribution"])

    def test_failed_local_receipt_retains_invalid_contribution_and_reason(self) -> None:
        record = copy.deepcopy(self.failed)
        record["validation"] = new_unvalidated_validation()
        record["manifest_links"]["validation_manifest_ref"] = None

        updated = apply_local_validation_receipt(
            record, self.root, "examples/validation-receipts/local-echo-fail.json"
        )

        self.assertEqual(updated["validation"]["status"], "invalid")
        self.assertEqual(
            updated["validation"]["failure_reason"],
            "output did not match deterministic echo expectation",
        )
        self.assertEqual(updated["record_id"], record["record_id"])
        self.assertEqual(updated["attribution"], record["attribution"])

    def test_plain_schema_declares_the_same_required_shape(self) -> None:
        schema = json.loads(
            (self.root / "examples/schemas/contribution-record.schema.json").read_text(
                encoding="utf-8"
            )
        )

        self.assertEqual(set(schema["required"]), set(self.minimal))
        self.assertFalse(schema["additionalProperties"])
        self.assertEqual(schema["properties"]["schema_version"], {"const": 5})
        self.assertEqual(
            schema["properties"]["result_hash_algorithm"], {"const": "sha256"}
        )
        self.assertEqual(
            schema["properties"]["result_hash"],
            {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"},
        )
        self.assertEqual(
            schema["properties"]["job_type"]["enum"],
            sorted(PHASE_1_JOB_CAPABILITIES),
        )
        self.assertEqual(
            schema["properties"]["capability"]["enum"],
            sorted(PHASE_1_JOB_CAPABILITIES.values()),
        )
        schema_triples = {
            (
                option["properties"]["work_type"]["const"],
                option["properties"]["job_type"]["const"],
                option["properties"]["capability"]["const"],
            )
            for option in schema["oneOf"]
        }
        self.assertEqual(
            schema_triples,
            {
                (job_type, job_type, capability)
                for job_type, capability in PHASE_1_JOB_CAPABILITIES.items()
            },
        )
        self.assertIn("failure_reason", schema["properties"]["validation"]["required"])
        validation_history = schema["properties"]["validation"]["properties"][
            "status_history"
        ]
        self.assertEqual(
            validation_history["prefixItems"],
            [{"$ref": "#/$defs/unvalidatedState"}],
        )
        self.assertEqual(
            validation_history["items"], {"$ref": "#/$defs/validationState"}
        )
        validation_state_text = json.dumps(schema["$defs"]["validationState"])
        self.assertNotIn('"passed"', validation_state_text)
        self.assertNotIn('"failed"', validation_state_text)
        self.assertIn(
            "parent_contribution_ids", schema["properties"]["lineage"]["required"]
        )
        self.assertIn(
            "contributor_node_id", schema["properties"]["lineage"]["required"]
        )
        self.assertIn("creation_mode", schema["properties"]["attribution"]["required"])
        receipt_id_schema = schema["properties"]["validation_receipt_id"]
        self.assertTrue(
            any(
                re.fullmatch(option["pattern"], self.minimal["validation_receipt_id"])
                for option in receipt_id_schema["anyOf"]
            )
        )
        sha_receipt_id = "local-validation-receipt-sha256:" + "a" * 64
        self.assertTrue(
            any(
                re.fullmatch(option["pattern"], sha_receipt_id)
                for option in receipt_id_schema["anyOf"]
            )
        )
        schema_text = json.dumps(schema).lower()
        self.assertNotIn("reward_amount", schema_text)
        self.assertNotIn("consensus_status", schema_text)

    def test_missing_required_record_identity_and_timestamp_fields_fail(self) -> None:
        for field in (
            "record_id",
            "job_id",
            "validation_receipt_id",
            "result_hash_algorithm",
            "result_hash",
            "creator_node_id",
            "contributor_node_id",
            "created_at",
            "job_type",
            "capability",
        ):
            with self.subTest(field=field):
                record = copy.deepcopy(self.minimal)
                record.pop(field)
                with self.assertRaisesRegex(
                    ContributionRecordError, f"missing: {field}"
                ):
                    validate_contribution_record(record)

    def test_version_three_record_is_not_silently_reinterpreted(self) -> None:
        record = copy.deepcopy(self.minimal)
        record["schema_version"] = 3
        with self.assertRaisesRegex(ContributionRecordError, "must be integer 5"):
            validate_contribution_record(record)

    def test_each_phase_one_job_type_records_its_manifest_capability(self) -> None:
        runtime_capabilities = {
            work_type: identifier
            for identifier, _, work_type in LOCAL_CAPABILITY_DEFINITIONS
        }
        self.assertEqual(PHASE_1_JOB_CAPABILITIES, runtime_capabilities)

        for job_type, capability in PHASE_1_JOB_CAPABILITIES.items():
            with self.subTest(job_type=job_type):
                record = copy.deepcopy(self.minimal)
                record["work_type"] = job_type
                record["job_type"] = job_type
                record["capability"] = capability
                self.assertIs(validate_contribution_record(record), record)

    def test_missing_or_mismatched_job_type_and_capability_fail(self) -> None:
        for field in ("job_type", "capability"):
            with self.subTest(field=field):
                record = copy.deepcopy(self.minimal)
                record.pop(field)
                with self.assertRaisesRegex(
                    ContributionRecordError, f"missing: {field}"
                ):
                    validate_contribution_record(record)

        record = copy.deepcopy(self.minimal)
        record["job_type"] = "unsupported"
        with self.assertRaisesRegex(ContributionRecordError, "supported Phase 1"):
            validate_contribution_record(record)

        record = copy.deepcopy(self.minimal)
        record["capability"] = "work.hash"
        with self.assertRaisesRegex(ContributionRecordError, "must match"):
            validate_contribution_record(record)

        record = copy.deepcopy(self.minimal)
        record["work_type"] = "hash"
        with self.assertRaisesRegex(ContributionRecordError, "work_type must match"):
            validate_contribution_record(record)

    def test_failed_validation_requires_reason_without_losing_other_evidence(
        self,
    ) -> None:
        record = copy.deepcopy(self.failed)
        record["validation"]["failure_reason"] = None
        with self.assertRaisesRegex(ContributionRecordError, "requires failure_reason"):
            validate_contribution_record(record)

        record = copy.deepcopy(self.minimal)
        record["validation"]["failure_reason"] = "not applicable"
        with self.assertRaisesRegex(ContributionRecordError, "only invalid"):
            validate_contribution_record(record)

    def test_rejects_unknown_or_malformed_core_values(self) -> None:
        cases = (
            (lambda record: record.update(schema_version=True), "schema_version"),
            (lambda record: record.update(record_id="bad id"), "record_id"),
            (lambda record: record.update(job_id=""), "job_id"),
            (lambda record: record.update(job_id="not a local id"), "job_id"),
            (
                lambda record: record.update(created_at="2026-99-13T12:00:00Z"),
                "created_at",
            ),
            (lambda record: record.update(work_type=""), "work_type"),
            (
                lambda record: record.update(unreviewed_network_claim=True),
                "unsupported fields",
            ),
        )
        for mutate, expected in cases:
            with self.subTest(expected=expected):
                record = copy.deepcopy(self.minimal)
                mutate(record)
                with self.assertRaisesRegex(ContributionRecordError, expected):
                    validate_contribution_record(record)

    def test_rejects_missing_or_unsafe_source_and_manifest_references(self) -> None:
        record = copy.deepcopy(self.minimal)
        record["source"] = {"local_source_path": None, "artifact_ref": None}
        with self.assertRaisesRegex(ContributionRecordError, "requires"):
            validate_contribution_record(record)

        for section, field in (
            ("source", "local_source_path"),
            ("manifest_links", "work_manifest_ref"),
            ("validation", "validation_receipt_ref"),
        ):
            with self.subTest(section=section, field=field):
                record = copy.deepcopy(self.failed)
                record[section][field] = "/private/record.json"
                with self.assertRaisesRegex(ContributionRecordError, "safe local"):
                    validate_contribution_record(record)

    def test_rejects_invalid_validation_lineage_and_attribution_values(self) -> None:
        cases = (
            (
                lambda record: record["validation"].update(status="trusted"),
                "validation.status",
            ),
            (
                lambda record: record["validation"].update(validated_at="not-a-time"),
                "validated_at",
            ),
            (
                lambda record: record["lineage"].update(
                    parent_contribution_ids="parent"
                ),
                "parent_contribution_ids",
            ),
            (
                lambda record: record["lineage"].update(input_hashes=["sha256:bad"]),
                "input_hashes",
            ),
            (
                lambda record: record["lineage"].update(
                    contributor_node_id="node.other"
                ),
                "must match contributor_node_id",
            ),
            (
                lambda record: record["attribution"].update(author_kind="account"),
                "author_kind",
            ),
            (
                lambda record: record["attribution"].update(creation_mode="imported"),
                "creation_mode",
            ),
        )
        for mutate, expected in cases:
            with self.subTest(expected=expected):
                record = copy.deepcopy(self.failed)
                mutate(record)
                with self.assertRaisesRegex(ContributionRecordError, expected):
                    validate_contribution_record(record)

    def test_rejects_non_object_sections_and_unknown_nested_fields(self) -> None:
        record = copy.deepcopy(self.minimal)
        record["source"] = []
        with self.assertRaisesRegex(
            ContributionRecordError, "source must be an object"
        ):
            validate_contribution_record(record)

        record = copy.deepcopy(self.minimal)
        record["attribution"]["reward_amount"] = 1
        with self.assertRaisesRegex(
            ContributionRecordError, "attribution contains unsupported"
        ):
            validate_contribution_record(record)

    def test_local_validation_rejects_result_hash_mismatches(self) -> None:
        record = copy.deepcopy(self.minimal)
        record["result_hash"] = "sha256:" + "f" * 64
        with self.assertRaisesRegex(
            ContributionRecordError, "validation receipt result_hash does not match"
        ):
            validate_local_contribution_record(record, self.root)

        record = copy.deepcopy(self.minimal)
        record["result_hash_algorithm"] = "sha512"
        with self.assertRaisesRegex(ContributionRecordError, "must be sha256"):
            validate_contribution_record(record)

        result = json.loads(
            (self.root / self.minimal["source"]["local_source_path"]).read_text(
                encoding="utf-8"
            )
        )
        result["validation_receipt_id"] = "local-validation-receipt-other"
        with (
            patch(
                "aethermesh_core.contribution_record.validate_job_result_document",
                return_value=result,
            ),
            self.assertRaisesRegex(
                ContributionRecordError, "validation_receipt_id does not match"
            ),
        ):
            validate_local_contribution_record(self.minimal, self.root)

        with TemporaryDirectory() as directory:
            local_root = Path(directory)
            record = copy.deepcopy(self.minimal)
            references = (
                record["source"]["local_source_path"],
                record["validation"]["validation_receipt_ref"],
                record["manifest_links"]["work_manifest_ref"],
            )
            for reference in references:
                assert isinstance(reference, str)
                target = local_root / reference
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes((self.root / reference).read_bytes())
            result_ref = record["source"]["local_source_path"]
            assert isinstance(result_ref, str)
            result_path = local_root / result_ref
            result = json.loads(result_path.read_text(encoding="utf-8"))
            result["output_payload"]["inline_payload"] = "hello tampered mesh"
            result_path.write_text(json.dumps(result), encoding="utf-8")
            with self.assertRaisesRegex(
                ContributionRecordError,
                "hash does not match its canonical payload",
            ):
                validate_local_contribution_record(record, local_root)

    def _load(self, name: str) -> dict[str, Any]:
        return json.loads(
            (self.root / "examples/contributions" / name).read_text(encoding="utf-8")
        )

    def _load_job_envelope(self, name: str) -> dict[str, Any]:
        return json.loads(
            (self.root / "examples/job-envelopes" / name).read_text(encoding="utf-8")
        )


if __name__ == "__main__":
    unittest.main()
