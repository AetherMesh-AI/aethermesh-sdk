import copy
import json
import tempfile
import unittest
from functools import partial
from pathlib import Path
from typing import Any
from unittest.mock import patch

from aethermesh_core.expert_manifest import (
    RECEIPT_VERSION,
    ExpertManifestError,
    _receipt_matches_manifest,
    deterministic_non_model_artifact_placeholder,
    expert_is_usable,
    load_expert_manifest,
    validate_expert_manifest,
    validate_expert_output,
)

ROOT = Path(__file__).parents[1]
SAMPLE = ROOT / "examples/model-experts/echo-expert-v0/manifest.json"


class ExpertManifestTests(unittest.TestCase):
    def test_sample_parses_and_unvalidated_manifest_is_not_usable(self) -> None:
        document = load_expert_manifest(SAMPLE)

        self.assertEqual(document["version"], 6)
        self.assertEqual(document["manifest_id"], "local-echo-fixture-manifest-v0")
        self.assertEqual(document["expert_id"], "local-echo-fixture-v0")
        self.assertEqual(document["name"], "Local Echo Fixture Expert")
        self.assertTrue(document["creator_node_id"])
        self.assertEqual(document["created_at"], "2026-07-18T00:00:00Z")
        self.assertEqual(
            document["input_schema_ref"], "schemas/local-echo-input.schema.json"
        )
        self.assertEqual(
            document["output_schema_ref"], "schemas/local-echo-output.schema.json"
        )
        self.assertEqual(document["validation"]["receipt_path"], None)
        self.assertEqual(document["training_lineage"], [])
        self.assertEqual(document["validation_history"], [])
        self.assertEqual(
            document["capabilities"][0]["validation"]["status"], "unvalidated"
        )
        self.assertFalse(expert_is_usable(SAMPLE))

    def test_save_and_load_preserve_provenance_and_validation_history(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            saved = Path(temp_dir) / "manifest.json"
            original = load_expert_manifest(SAMPLE)
            self._write_schemas(Path(temp_dir), original)
            saved.write_text(json.dumps(original, sort_keys=True), encoding="utf-8")
            handoff = load_expert_manifest(saved)

        self.assertEqual(handoff["lineage"], original["lineage"])
        self.assertEqual(handoff["training_lineage"], original["training_lineage"])
        self.assertEqual(handoff["validation_history"], original["validation_history"])
        self.assertEqual(
            handoff["contribution_attribution"], original["contribution_attribution"]
        )
        self.assertEqual(handoff["creator_node_id"], original["creator_node_id"])
        self.assertEqual(handoff["created_at"], original["created_at"])
        self.assertEqual(handoff["manifest_id"], original["manifest_id"])
        self.assertEqual(handoff["capabilities"], original["capabilities"])

    def test_training_lineage_is_required_and_accepts_empty_v0_list(self) -> None:
        document = self._sample()
        self.assertEqual(document["training_lineage"], [])
        validate_expert_manifest(document)

        document.pop("training_lineage")
        document.pop("validation_history")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): training_lineage"
        ):
            validate_expert_manifest(document)

    def test_validation_history_is_required_and_accepts_empty_v0_list(self) -> None:
        document = self._sample()
        self.assertEqual(document["validation_history"], [])
        validate_expert_manifest(document)

        document.pop("validation_history")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): validation_history"
        ):
            validate_expert_manifest(document)

        for value in (None, "not-an-array"):
            with self.subTest(value=value):
                document = self._sample()
                document["validation_history"] = value
                with self.assertRaisesRegex(
                    ExpertManifestError, "validation_history must be a list"
                ):
                    validate_expert_manifest(document)

    def test_version_7_requires_license_for_external_artifacts_only(self) -> None:
        local_only = self._sample()
        local_only["version"] = 7
        local_only["external_artifacts"] = []
        validate_expert_manifest(local_only)

        third_party = copy.deepcopy(local_only)
        third_party["external_artifacts"] = [
            {"kind": "model", "reference": "upstream/example-model"}
        ]
        with self.assertRaisesRegex(
            ExpertManifestError,
            "license is required when external_artifacts are declared",
        ):
            validate_expert_manifest(third_party)

        third_party["license"] = "Apache-2.0"
        validate_expert_manifest(third_party)

        third_party["external_artifacts"][0]["kind"] = "unknown"
        with self.assertRaisesRegex(
            ExpertManifestError, "external_artifacts\\[0\\].kind must be one of"
        ):
            validate_expert_manifest(third_party)

    def test_version_7_license_metadata_preserves_manifest_provenance_and_receipt(
        self,
    ) -> None:
        document = self._sample()
        document["version"] = 7
        document["external_artifacts"] = [
            {"kind": "adapter", "reference": "upstream/example-adapter"}
        ]
        document["license"] = "MIT"
        validate_expert_manifest(document)

        for field in (
            "manifest_id",
            "creator_node_id",
            "lineage",
            "validation",
            "contribution_attribution",
        ):
            with self.subTest(field=field):
                self.assertEqual(document[field], self._sample()[field])

        receipt = {
            "receipt_version": RECEIPT_VERSION,
            "name": document["name"],
            "manifest_id": document["manifest_id"],
            "expert_id": document["expert_id"],
            "creator_node_id": document["creator_node_id"],
            "author": document["author"],
            "owner": document["owner"],
            "created_at": document["created_at"],
            "attribution_notes": document["attribution_notes"],
            "artifact_hash": document["artifact_hash"],
            "input_schema_ref": document["input_schema_ref"],
            "output_schema_ref": document["output_schema_ref"],
            "lineage": document["lineage"],
            "training_lineage": document["training_lineage"],
            "validation_history": document["validation_history"],
            "external_artifacts": document["external_artifacts"],
            "license": document["license"],
            "contribution_attribution": document["contribution_attribution"],
            "validated_at": document["validation"]["last_validated_at"],
            "validator_node_id": document["validation"]["validator_node_id"],
            "status": document["validation"]["status"],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "receipt.json"
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertTrue(_receipt_matches_manifest(receipt_path, document))

        receipt["license"] = "GPL-3.0-only"
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "receipt.json"
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
            self.assertFalse(_receipt_matches_manifest(receipt_path, document))

    def test_training_lineage_entries_require_verified_local_hashes(self) -> None:
        document = self._sample()
        document["training_lineage"] = [
            {
                "kind": "weights",
                "reference": "weights.bin",
                "sha256": "sha256:" + "0" * 64,
            }
        ]
        validate_expert_manifest(document)

        document["training_lineage"][0]["sha256"] = "sha256:unverified"
        with self.assertRaisesRegex(
            ExpertManifestError,
            "training_lineage\\[0\\].sha256 must be a lowercase sha256 content hash",
        ):
            validate_expert_manifest(document)

    def test_version_5_manifest_remains_readable_without_validation_history(
        self,
    ) -> None:
        document = self._sample()
        document["version"] = 5
        document.pop("validation_history")

        validate_expert_manifest(document)

    def test_capability_metadata_requires_complete_evidence_or_unvalidated_status(
        self,
    ) -> None:
        document = self._sample()
        document.pop("capabilities")
        with self.assertRaisesRegex(ExpertManifestError, "missing required field"):
            validate_expert_manifest(document)

        document = self._sample()
        capability = document["capabilities"][0]
        capability["validation"] = {
            "status": "passed",
            "local_test_name": "test_local_echo",
            "validation_receipt_id": "receipt-local-echo-v0",
            "validated_at": "2026-07-18T00:00:01Z",
            "result_summary": "fixture output matched exactly",
        }
        validate_expert_manifest(document)

        capability["validation"]["validation_receipt_id"] = None
        with self.assertRaisesRegex(
            ExpertManifestError,
            "validation_receipt_id must be a non-empty whitespace-free string",
        ):
            validate_expert_manifest(document)

        document = self._sample()
        document["capabilities"][0]["validation"]["status"] = []
        with self.assertRaisesRegex(ExpertManifestError, "validation.status must be"):
            validate_expert_manifest(document)

        document = self._sample()
        document["capabilities"][0]["validation"]["local_test_name"] = "test_local_echo"
        with self.assertRaisesRegex(ExpertManifestError, "when unvalidated"):
            validate_expert_manifest(document)

    def test_version_1_manifest_remains_readable_without_version_2_fields(self) -> None:
        document = self._sample()
        document["version"] = 1
        document.pop("manifest_id")
        document.pop("capabilities")
        document.pop("input_schema_ref")
        document.pop("output_schema_ref")
        document.pop("author")
        document.pop("owner")
        document.pop("attribution_notes")
        document.pop("training_lineage")
        document.pop("validation_history")
        document["validation"].update(
            {
                "status": "passed",
                "test_command": "python -m unittest",
                "expected_inputs_ref": "input.json",
                "receipt_path": "receipt.json",
                "last_validated_at": "2026-07-18T00:00:01Z",
                "validator_node_id": "node-validator",
            }
        )
        document["contribution_attribution"].update(
            {
                "validator_node_id": "node-validator",
                "receipt_refs": ["receipt.json"],
            }
        )

        validate_expert_manifest(document)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            self.assertEqual(load_expert_manifest(path)["version"], 1)
            receipt = Path(temp_dir) / "receipt.json"
            receipt.write_text(
                json.dumps(
                    {
                        "receipt_version": RECEIPT_VERSION,
                        "name": document["name"],
                        "expert_id": document["expert_id"],
                        "creator_node_id": document["creator_node_id"],
                        "created_at": document["created_at"],
                        "artifact_hash": document["artifact_hash"],
                        "validated_at": document["validation"]["last_validated_at"],
                        "validator_node_id": document["validation"][
                            "validator_node_id"
                        ],
                        "status": "passed",
                    }
                ),
                encoding="utf-8",
            )
            self.assertTrue(_receipt_matches_manifest(receipt, document))

    def test_version_2_receipt_remains_accepted_without_version_3_fields(self) -> None:
        document = self._sample()
        document["version"] = 2
        document.pop("output_schema_ref")
        document.pop("author")
        document.pop("owner")
        document.pop("attribution_notes")
        document.pop("training_lineage")
        document.pop("validation_history")
        document["validation"].update(
            {
                "status": "passed",
                "test_command": "python -m unittest",
                "expected_inputs_ref": "input.json",
                "receipt_path": "receipt.json",
                "last_validated_at": "2026-07-18T00:00:01Z",
                "validator_node_id": "node-validator",
            }
        )
        document["contribution_attribution"].update(
            {
                "validator_node_id": "node-validator",
                "receipt_refs": ["receipt.json"],
            }
        )
        receipt_document = {
            "receipt_version": RECEIPT_VERSION,
            "name": document["name"],
            "manifest_id": document["manifest_id"],
            "expert_id": document["expert_id"],
            "creator_node_id": document["creator_node_id"],
            "created_at": document["created_at"],
            "artifact_hash": document["artifact_hash"],
            "input_schema_ref": document["input_schema_ref"],
            "validated_at": document["validation"]["last_validated_at"],
            "validator_node_id": document["validation"]["validator_node_id"],
            "status": "passed",
        }

        validate_expert_manifest(document)
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt = Path(temp_dir) / "receipt.json"
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertTrue(_receipt_matches_manifest(receipt, document))

    def test_version_3_receipt_remains_accepted_without_version_4_fields(self) -> None:
        document = self._sample()
        document["version"] = 3
        for field in ("author", "owner", "attribution_notes"):
            document.pop(field)
        document.pop("training_lineage")
        document.pop("validation_history")
        document["validation"].update(
            {
                "status": "passed",
                "test_command": "python -m unittest",
                "expected_inputs_ref": "input.json",
                "receipt_path": "receipt.json",
                "last_validated_at": "2026-07-18T00:00:01Z",
                "validator_node_id": "node-validator",
            }
        )
        document["contribution_attribution"].update(
            {
                "validator_node_id": "node-validator",
                "receipt_refs": ["receipt.json"],
            }
        )
        receipt_document = {
            "receipt_version": RECEIPT_VERSION,
            "name": document["name"],
            "manifest_id": document["manifest_id"],
            "expert_id": document["expert_id"],
            "creator_node_id": document["creator_node_id"],
            "created_at": document["created_at"],
            "artifact_hash": document["artifact_hash"],
            "input_schema_ref": document["input_schema_ref"],
            "output_schema_ref": document["output_schema_ref"],
            "lineage": document["lineage"],
            "contribution_attribution": document["contribution_attribution"],
            "validated_at": document["validation"]["last_validated_at"],
            "validator_node_id": document["validation"]["validator_node_id"],
            "status": "passed",
        }

        validate_expert_manifest(document)
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt = Path(temp_dir) / "receipt.json"
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertTrue(_receipt_matches_manifest(receipt, document))

    def test_input_schema_reference_must_be_present_and_resolvable(self) -> None:
        document = self._sample()
        document.pop("input_schema_ref")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): input_schema_ref"
        ):
            validate_expert_manifest(document)

        for reference in (
            "",
            "../input.schema.json",
            "https://example.com/schema.json",
        ):
            with self.subTest(reference=reference):
                document = self._sample()
                document["input_schema_ref"] = reference
                with self.assertRaisesRegex(
                    ExpertManifestError, "input_schema_ref must be"
                ):
                    validate_expert_manifest(document)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            document = self._sample()
            document["input_schema_ref"] = "schemas/missing.schema.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(
                ExpertManifestError, "must name a readable local JSON Schema"
            ):
                load_expert_manifest(path)

    def test_input_schema_reference_must_describe_required_typed_inputs(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            schemas = directory / "schemas"
            schemas.mkdir()
            schema = schemas / "input.schema.json"
            schema.write_text(
                json.dumps(
                    {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["message"],
                        "properties": {"message": {"type": "string"}},
                    }
                ),
                encoding="utf-8",
            )
            document = self._sample()
            document["input_schema_ref"] = "schemas/input.schema.json"
            path = directory / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(
                ExpertManifestError, "must declare input validation constraints"
            ):
                load_expert_manifest(path)

            schema.write_text(
                json.dumps(
                    {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["message"],
                        "properties": {
                            "message": {
                                "type": "string",
                                "description": "Input documentation is not a constraint.",
                                "minItems": 1,
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ExpertManifestError, "must declare input validation constraints"
            ):
                load_expert_manifest(path)

            schema.write_text(
                json.dumps(
                    {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": "object",
                        "additionalProperties": False,
                        "required": ["message"],
                        "properties": {
                            "message": {
                                "type": "string",
                                "minLength": "not-an-integer",
                            }
                        },
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                ExpertManifestError, "must contain a valid JSON Schema"
            ):
                load_expert_manifest(path)

            for required, field_type, message in (
                ([{"not": "a field name"}], "string", "unique required input fields"),
                (["message"], "not-a-json-schema-type", "accepted types"),
            ):
                with self.subTest(required=required, field_type=field_type):
                    schema.write_text(
                        json.dumps(
                            {
                                "$schema": "https://json-schema.org/draft/2020-12/schema",
                                "type": "object",
                                "additionalProperties": False,
                                "required": required,
                                "properties": {
                                    "message": {"type": field_type, "minLength": 1}
                                },
                            }
                        ),
                        encoding="utf-8",
                    )
                    with self.assertRaisesRegex(ExpertManifestError, message):
                        load_expert_manifest(path)

    def test_output_schema_reference_must_be_present_and_resolvable(self) -> None:
        document = self._sample()
        document.pop("output_schema_ref")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): output_schema_ref"
        ):
            validate_expert_manifest(document)

        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            document = self._sample()
            document["output_schema_ref"] = "schemas/missing.schema.json"
            self._write_input_schema(Path(temp_dir), document)
            path.write_text(json.dumps(document), encoding="utf-8")
            with self.assertRaisesRegex(
                ExpertManifestError, "must name a readable local JSON Schema"
            ):
                load_expert_manifest(path)

    def test_output_schema_reference_must_describe_valid_typed_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            schemas = directory / "schemas"
            schemas.mkdir()
            schema = schemas / "output.schema.json"
            document = self._sample()
            document["output_schema_ref"] = "schemas/output.schema.json"
            self._write_input_schema(directory, document)
            path = directory / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")

            schema.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(ExpertManifestError, "draft 2020-12"):
                load_expert_manifest(path)

            schema.write_text(
                json.dumps(
                    {
                        "$schema": "https://json-schema.org/draft/2020-12/schema",
                        "type": ["string"],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ExpertManifestError, "JSON output type"):
                load_expert_manifest(path)

            for ref_keyword in ("$ref", "$dynamicRef"):
                with self.subTest(ref_keyword=ref_keyword):
                    schema.write_text(
                        json.dumps(
                            {
                                "$schema": (
                                    "https://json-schema.org/draft/2020-12/schema"
                                ),
                                "type": "string",
                                "examples": [],
                                "allOf": [
                                    {ref_keyword: "https://example.com/output.json"}
                                ],
                            }
                        ),
                        encoding="utf-8",
                    )
                    with self.assertRaisesRegex(
                        ExpertManifestError, "references must be local fragments"
                    ):
                        load_expert_manifest(path)

    def test_output_schema_validates_local_expert_output(self) -> None:
        validate_expert_output(SAMPLE, "echoed local value")
        with self.assertRaisesRegex(
            ExpertManifestError, "output does not satisfy output_schema_ref"
        ):
            validate_expert_output(SAMPLE, "")

    def test_output_schema_reports_an_unresolvable_local_fragment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            document = self._sample()
            self._write_schemas(directory, document)
            schema_path = directory / document["output_schema_ref"]
            schema = json.loads(schema_path.read_text(encoding="utf-8"))
            schema["$ref"] = "#/$defs/missing"
            schema_path.write_text(json.dumps(schema), encoding="utf-8")
            manifest_path = directory / "manifest.json"
            manifest_path.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(
                ExpertManifestError, "output does not satisfy output_schema_ref"
            ):
                validate_expert_output(manifest_path, "echoed local value")

    def test_failed_receipt_records_manifest_creator_schema_lineage_and_attribution(
        self,
    ) -> None:
        document = self._sample()
        validation = document["validation"]
        validation["status"] = "failed"
        receipt_document = {
            "receipt_version": RECEIPT_VERSION,
            "name": document["name"],
            "manifest_id": document["manifest_id"],
            "expert_id": document["expert_id"],
            "creator_node_id": document["creator_node_id"],
            "author": document["author"],
            "owner": document["owner"],
            "created_at": document["created_at"],
            "attribution_notes": document["attribution_notes"],
            "artifact_hash": document["artifact_hash"],
            "input_schema_ref": document["input_schema_ref"],
            "output_schema_ref": document["output_schema_ref"],
            "lineage": document["lineage"],
            "training_lineage": document["training_lineage"],
            "validation_history": document["validation_history"],
            "contribution_attribution": document["contribution_attribution"],
            "validated_at": validation["last_validated_at"],
            "validator_node_id": validation["validator_node_id"],
            "status": "failed",
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt = Path(temp_dir) / "receipt.json"
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertTrue(_receipt_matches_manifest(receipt, document))

            receipt_document["status"] = "passed"
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertFalse(_receipt_matches_manifest(receipt, document))

    def test_non_model_placeholder_is_repeatable_and_binds_explicit_identity(
        self,
    ) -> None:
        document = self._sample()
        document["artifact"] = {"reference": "local-echo-interface-v0", "sha256": None}
        document["artifact_hash"] = deterministic_non_model_artifact_placeholder(
            document
        )

        validate_expert_manifest(document)
        self.assertEqual(
            document["artifact_hash"],
            deterministic_non_model_artifact_placeholder(copy.deepcopy(document)),
        )

        changed = copy.deepcopy(document)
        changed["artifact"]["reference"] = "different-local-interface-v0"
        changed["artifact_hash"] = deterministic_non_model_artifact_placeholder(changed)
        validate_expert_manifest(changed)
        self.assertNotEqual(changed["artifact_hash"], document["artifact_hash"])

    def test_artifact_hash_must_match_concrete_content_hash(self) -> None:
        document = self._sample()
        document["artifact_hash"] = "sha256:" + "0" * 64

        with self.assertRaisesRegex(ExpertManifestError, "must match artifact.sha256"):
            validate_expert_manifest(document)

    def test_model_entry_cannot_use_non_model_placeholder(self) -> None:
        document = self._sample()
        document["model_id"] = "local-echo-model-v0"
        document["artifact"] = {"reference": "local-echo-interface-v0", "sha256": None}
        document["artifact_hash"] = deterministic_non_model_artifact_placeholder(
            document
        )

        with self.assertRaisesRegex(
            ExpertManifestError, "non-model expert placeholder"
        ):
            validate_expert_manifest(document)

    def test_concrete_artifact_content_hash_changes_only_with_content(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            artifact = Path(temp_dir) / "worker.py"
            artifact.write_text("print('one')\n", encoding="utf-8")
            first_hash = self._hash(artifact)
            self.assertEqual(first_hash, self._hash(artifact))

            artifact.write_text("print('two')\n", encoding="utf-8")
            self.assertNotEqual(first_hash, self._hash(artifact))

    def test_renaming_preserves_stable_identity_lineage_and_attribution(self) -> None:
        original = self._sample()
        renamed = copy.deepcopy(original)
        renamed["name"] = "Renamed Local Echo Expert"

        validate_expert_manifest(renamed)

        for field in (
            "version",
            "expert_id",
            "creator_node_id",
            "author",
            "owner",
            "created_at",
            "attribution_notes",
            "lineage",
            "validation",
            "contribution_attribution",
        ):
            with self.subTest(field=field):
                self.assertEqual(renamed[field], original[field])

    def test_required_fields_and_malformed_json_fail_clearly(self) -> None:
        with self.assertRaisesRegex(ExpertManifestError, "must be a JSON object"):
            validate_expert_manifest([])
        document = self._sample()
        document.pop("version")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): version"
        ):
            validate_expert_manifest(document)
        document = self._sample()
        document["unexpected"] = "field"
        with self.assertRaisesRegex(ExpertManifestError, "allowed fields"):
            validate_expert_manifest(document)
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "manifest.json"
            path.write_text("{broken", encoding="utf-8")
            with self.assertRaisesRegex(ExpertManifestError, "JSON is malformed"):
                load_expert_manifest(path)
            path.write_bytes(b"\xff")
            with self.assertRaisesRegex(ExpertManifestError, "invalid UTF-8"):
                load_expert_manifest(path)

    def test_missing_manifest_attribution_rejects_model_and_expert_before_use(
        self,
    ) -> None:
        for identity_field, identity in (
            ("expert_id", "local-attribution-expert-v0"),
            ("model_id", "local-attribution-model-v0"),
        ):
            with self.subTest(identity_field=identity_field):
                document = self._sample()
                document.pop("expert_id")
                document[identity_field] = identity
                document.pop("contribution_attribution")

                with tempfile.TemporaryDirectory() as temp_dir:
                    directory = Path(temp_dir)
                    self._write_schemas(directory, document)
                    manifest_path = directory / "manifest.json"
                    manifest_path.write_text(
                        json.dumps(document, sort_keys=True), encoding="utf-8"
                    )
                    records_before = {
                        path.relative_to(directory): path.read_bytes()
                        for path in directory.rglob("*")
                        if path.is_file()
                    }

                    for use_manifest in (
                        partial(load_expert_manifest, manifest_path),
                        partial(
                            validate_expert_output, manifest_path, "echoed local value"
                        ),
                    ):
                        with self.assertRaisesRegex(
                            ExpertManifestError,
                            "missing required field\\(s\\): contribution_attribution",
                        ):
                            use_manifest()

                    self.assertEqual(
                        {
                            path.relative_to(directory): path.read_bytes()
                            for path in directory.rglob("*")
                            if path.is_file()
                        },
                        records_before,
                    )

    def test_creator_node_id_is_required_before_local_manifest_use(self) -> None:
        missing = self._sample()
        missing.pop("creator_node_id")
        with self.assertRaisesRegex(
            ExpertManifestError,
            "missing required field\\(s\\): creator_node_id",
        ):
            validate_expert_manifest(missing)

        for value, message in (
            (None, "creator_node_id must be a non-empty whitespace-free string"),
            ("", "creator_node_id must be a non-empty whitespace-free string"),
            (" \t ", "creator_node_id must be a non-empty whitespace-free string"),
            ("placeholder", "creator_node_id must name a concrete creator node"),
            ("node-placeholder", "creator_node_id must name a concrete creator node"),
            ("unknown", "creator_node_id must name a concrete creator node"),
            ("unset", "creator_node_id must name a concrete creator node"),
        ):
            with self.subTest(value=value):
                document = self._sample()
                document["creator_node_id"] = value
                with self.assertRaisesRegex(ExpertManifestError, message):
                    validate_expert_manifest(document)

        valid = self._sample()
        validate_expert_manifest(valid)
        self.assertEqual(valid["creator_node_id"], "node-local-fixture")

    def test_schema_rejects_invalid_required_values(self) -> None:
        cases = [
            ("version", "1", "version must be 1, 2, 3, 4, 5, 6, or 7"),
            ("version", 8, "version must be 1, 2, 3, 4, 5, 6, or 7"),
            ("version", True, "version must be 1, 2, 3, 4, 5, 6, or 7"),
            (
                "expert_id",
                "",
                "requires at least one non-empty model_id or expert_id",
            ),
            ("created_at", "yesterday", "created_at must be"),
            ("creator_node_id", "", "creator_node_id must be"),
            ("creator_node_id", "   ", "creator_node_id must be"),
            ("author", "   ", "author must be a non-empty string"),
            ("owner", "", "owner must be a non-empty string"),
            ("attribution_notes", "\t", "attribution_notes must be a non-empty string"),
            ("name", "   ", "name must be a non-empty string"),
            ("supported_task_categories", [], "supported_task_categories must be"),
            ("runtime_requirements", [""], "runtime_requirements must be"),
        ]
        for field, value, message in cases:
            with self.subTest(field=field):
                document = self._sample()
                document[field] = value
                with self.assertRaisesRegex(ExpertManifestError, message):
                    validate_expert_manifest(document)

        document = self._sample()
        document.pop("name")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): name"
        ):
            validate_expert_manifest(document)

        document = self._sample()
        document.pop("creator_node_id")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): creator_node_id"
        ):
            validate_expert_manifest(document)

        for field in ("author", "owner", "attribution_notes"):
            with self.subTest(field=field):
                document = self._sample()
                document.pop(field)
                with self.assertRaisesRegex(
                    ExpertManifestError, f"missing required field\\(s\\): {field}"
                ):
                    validate_expert_manifest(document)

        document = self._sample()
        document.pop("created_at")
        with self.assertRaisesRegex(
            ExpertManifestError, "missing required field\\(s\\): created_at"
        ):
            validate_expert_manifest(document)

        document = self._sample()
        for timestamp in (
            "2026-99-99T99:99:99Z",
            "2026-07-18T00:00:00",
            "2026-07-18T00:00:00+00:00",
            "2026-07-18T00:00:00-04:00",
        ):
            with self.subTest(timestamp=timestamp):
                document["created_at"] = timestamp
                with self.assertRaisesRegex(ExpertManifestError, "created_at must be"):
                    validate_expert_manifest(document)

    def test_schema_rejects_incomplete_or_unsafe_nested_evidence(self) -> None:
        cases = [
            (
                "artifact",
                {"reference": "../artifact", "sha256": "sha256:" + "a" * 64},
                "artifact.reference",
            ),
            ("lineage", {}, "lineage must contain exactly"),
            ("validation", {"status": "unknown"}, "validation must contain exactly"),
            (
                "contribution_attribution",
                {},
                "contribution_attribution must contain exactly",
            ),
        ]
        for field, value, message in cases:
            with self.subTest(field=field):
                document = self._sample()
                document[field] = value
                with self.assertRaisesRegex(ExpertManifestError, message):
                    validate_expert_manifest(document)
        document = self._sample()
        document["validation"].update(
            {
                "status": "passed",
                "test_command": "test",
                "expected_inputs_ref": "input.json",
            }
        )
        with self.assertRaisesRegex(
            ExpertManifestError, "passed validation requires complete"
        ):
            validate_expert_manifest(document)

        document = self._sample()
        document["lineage"]["derived_artifact_refs"] = None
        with self.assertRaisesRegex(
            ExpertManifestError, "derived_artifact_refs must be"
        ):
            validate_expert_manifest(document)

    def test_validation_evidence_matches_contribution_attribution(self) -> None:
        document = self._sample()
        document["validation"]["validator_node_id"] = "node-validator"
        with self.assertRaisesRegex(
            ExpertManifestError, "validator_node_id must match"
        ):
            validate_expert_manifest(document)

        document = self._sample()
        document["validation"]["receipt_path"] = "receipt.json"
        with self.assertRaisesRegex(ExpertManifestError, "must appear in"):
            validate_expert_manifest(document)

    def test_nested_collections_and_status_fail_with_manifest_error(self) -> None:
        document = self._sample()
        document["validation"]["status"] = []
        with self.assertRaisesRegex(ExpertManifestError, "validation.status must be"):
            validate_expert_manifest(document)

        document = self._sample()
        document["contribution_attribution"]["receipt_refs"] = "abc"
        with self.assertRaisesRegex(
            ExpertManifestError, "contribution_attribution.receipt_refs must be"
        ):
            validate_expert_manifest(document)

    def test_usable_requires_matching_artifact_and_real_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir) / "expert"
            directory.mkdir()
            self._write_schemas(directory, load_expert_manifest(SAMPLE))
            artifact = directory / "artifact.txt"
            artifact.write_text("artifact", encoding="utf-8")
            receipt = directory / "receipt.json"
            document = self._sample()
            document["artifact"]["sha256"] = (
                "sha256:cfb2e16a101a0000000000000000000000000000000000000000000000000000"
            )
            document["artifact_hash"] = document["artifact"]["sha256"]
            validation = document["validation"]
            validation.update(
                {
                    "status": "passed",
                    "test_command": "python -m unittest",
                    "expected_inputs_ref": "input.json",
                    "receipt_path": "receipt.json",
                    "last_validated_at": "2026-07-18T00:00:01Z",
                    "validator_node_id": "node-validator",
                }
            )
            document["lineage"]["derived_artifact_refs"] = ["artifact.txt"]
            document["contribution_attribution"].update(
                {
                    "modifier_node_ids": ["node-modifier"],
                    "validator_node_id": "node-validator",
                    "receipt_refs": ["receipt.json"],
                }
            )
            path = directory / "manifest.json"
            path.write_text(json.dumps(document), encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            document["artifact"]["sha256"] = self._hash(artifact)
            document["artifact_hash"] = document["artifact"]["sha256"]
            path.write_text(json.dumps(document), encoding="utf-8")
            receipt_document = {
                "receipt_version": RECEIPT_VERSION,
                "name": document["name"],
                "manifest_id": document["manifest_id"],
                "expert_id": document["expert_id"],
                "creator_node_id": document["creator_node_id"],
                "author": document["author"],
                "owner": document["owner"],
                "created_at": document["created_at"],
                "attribution_notes": document["attribution_notes"],
                "artifact_hash": document["artifact_hash"],
                "input_schema_ref": document["input_schema_ref"],
                "output_schema_ref": document["output_schema_ref"],
                "lineage": document["lineage"],
                "training_lineage": document["training_lineage"],
                "validation_history": document["validation_history"],
                "contribution_attribution": document["contribution_attribution"],
                "validated_at": validation["last_validated_at"],
                "validator_node_id": validation["validator_node_id"],
                "status": "passed",
            }
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            with patch(
                "aethermesh_core.expert_manifest._sha256",
                side_effect=OSError("read failed"),
            ):
                self.assertFalse(expert_is_usable(path))
            self.assertTrue(expert_is_usable(path))
            self.assertEqual(receipt_document["name"], "Local Echo Fixture Expert")
            self.assertEqual(
                receipt_document["creator_node_id"], document["creator_node_id"]
            )
            self.assertEqual(
                receipt_document["input_schema_ref"], document["input_schema_ref"]
            )
            self.assertEqual(
                receipt_document["output_schema_ref"], document["output_schema_ref"]
            )
            self.assertEqual(receipt_document["lineage"], document["lineage"])
            self.assertEqual(
                receipt_document["training_lineage"], document["training_lineage"]
            )
            self.assertEqual(
                receipt_document["contribution_attribution"],
                document["contribution_attribution"],
            )
            self.assertEqual(receipt_document["created_at"], document["created_at"])

            receipt_document["created_at"] = "2026-07-18T00:00:02Z"
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt_document["created_at"] = document["created_at"]
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")

            receipt_document.pop("creator_node_id")
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt_document["creator_node_id"] = document["creator_node_id"]
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")

            receipt.write_text("not JSON", encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt.write_bytes(b"\xff")
            self.assertFalse(expert_is_usable(path))
            receipt.write_text("[]", encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt.write_text("{}", encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt_document["artifact_hash"] = "sha256:" + "0" * 64
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")
            self.assertFalse(expert_is_usable(path))
            receipt_document["artifact_hash"] = document["artifact_hash"]
            receipt.write_text(json.dumps(receipt_document), encoding="utf-8")

            outside = directory.parent / "outside.txt"
            outside.write_text("artifact", encoding="utf-8")
            artifact.unlink()
            artifact.symlink_to(outside)
            self.assertFalse(expert_is_usable(path))

    def test_read_error_does_not_leak_local_path(self) -> None:
        missing = Path(tempfile.gettempdir()) / "private-manifest-location.json"
        with self.assertRaisesRegex(
            ExpertManifestError, "could not read expert manifest"
        ) as raised:
            load_expert_manifest(missing)
        self.assertNotIn(str(missing), str(raised.exception))

    @staticmethod
    def _sample() -> dict[str, Any]:
        return copy.deepcopy(load_expert_manifest(SAMPLE))

    @staticmethod
    def _hash(path: Path) -> str:
        import hashlib

        return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()

    @staticmethod
    def _write_input_schema(directory: Path, document: dict[str, Any]) -> None:
        source = SAMPLE.parent / document["input_schema_ref"]
        destination = directory / document["input_schema_ref"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())

    @classmethod
    def _write_schemas(cls, directory: Path, document: dict[str, Any]) -> None:
        cls._write_input_schema(directory, document)
        source = SAMPLE.parent / document["output_schema_ref"]
        destination = directory / document["output_schema_ref"]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())


if __name__ == "__main__":
    unittest.main()
