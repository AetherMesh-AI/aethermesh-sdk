import copy
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any

from aethermesh_core.expert_manifest import (
    RECEIPT_VERSION,
    ExpertManifestError,
    expert_is_usable,
    load_expert_manifest,
    validate_expert_manifest,
)

ROOT = Path(__file__).parents[1]
SAMPLE = ROOT / "examples/model-experts/echo-expert-v0/manifest.json"


class ExpertManifestIdentityTests(unittest.TestCase):
    def test_accepts_model_id_expert_id_or_both(self) -> None:
        expert_only = self._sample()
        validate_expert_manifest(expert_only)

        model_only = self._sample()
        model_only.pop("expert_id")
        model_only["model_id"] = "local-echo-model-v0"
        validate_expert_manifest(model_only)

        both = self._sample()
        both["model_id"] = "local-echo-model-v0"
        validate_expert_manifest(both)

    def test_rejects_missing_blank_or_whitespace_only_identity(self) -> None:
        cases = [
            {},
            {"expert_id": ""},
            {"expert_id": "   "},
            {"model_id": ""},
            {"model_id": "\t"},
        ]
        for identities in cases:
            with self.subTest(identities=identities):
                document = self._sample()
                document.pop("expert_id")
                document.update(identities)
                with self.assertRaisesRegex(
                    ExpertManifestError,
                    "requires at least one non-empty model_id or expert_id",
                ):
                    validate_expert_manifest(document)

    def test_model_id_is_preserved_in_matching_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            directory = Path(temp_dir)
            artifact = directory / "artifact.txt"
            artifact.write_text("artifact", encoding="utf-8")
            document = self._sample()
            self._write_schemas(directory, document)
            document.pop("expert_id")
            document["model_id"] = "local-echo-model-v0"
            artifact_hash = (
                "sha256:" + hashlib.sha256(artifact.read_bytes()).hexdigest()
            )
            document["artifact"]["sha256"] = artifact_hash
            document["artifact_hash"] = artifact_hash
            document["lineage"]["derived_artifact_refs"] = ["artifact.txt"]
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
            manifest = directory / "manifest.json"
            manifest.write_text(json.dumps(document), encoding="utf-8")
            receipt = {
                "receipt_version": RECEIPT_VERSION,
                "name": document["name"],
                "manifest_id": document["manifest_id"],
                "model_id": document["model_id"],
                "creator_node_id": document["creator_node_id"],
                "author": document["author"],
                "owner": document["owner"],
                "created_at": document["created_at"],
                "attribution_notes": document["attribution_notes"],
                "artifact_hash": artifact_hash,
                "input_schema_ref": document["input_schema_ref"],
                "output_schema_ref": document["output_schema_ref"],
                "lineage": document["lineage"],
                "training_lineage": document["training_lineage"],
                "validation_history": document["validation_history"],
                "contribution_attribution": document["contribution_attribution"],
                "validated_at": document["validation"]["last_validated_at"],
                "validator_node_id": document["validation"]["validator_node_id"],
                "status": "passed",
            }
            (directory / "receipt.json").write_text(
                json.dumps(receipt), encoding="utf-8"
            )

            self.assertTrue(expert_is_usable(manifest))
            self.assertEqual(
                json.loads((directory / "receipt.json").read_text(encoding="utf-8"))[
                    "model_id"
                ],
                "local-echo-model-v0",
            )
            self.assertEqual(
                json.loads((directory / "receipt.json").read_text(encoding="utf-8"))[
                    "creator_node_id"
                ],
                document["creator_node_id"],
            )
            self.assertEqual(
                json.loads((directory / "receipt.json").read_text(encoding="utf-8"))[
                    "created_at"
                ],
                document["created_at"],
            )

    @staticmethod
    def _sample() -> dict[str, Any]:
        return copy.deepcopy(load_expert_manifest(SAMPLE))

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
