import unittest
from importlib import metadata
from unittest import mock

from aethermesh_core import __version__
from aethermesh_core.identity import (
    IdentityPersistenceError,
    _identity_document,
    _load_identity,
)
from aethermesh_core.models import NodeIdentity
from aethermesh_core.receipts import (
    ReceiptPersistenceError,
    build_receipt_document,
)
from aethermesh_core.version_metadata import (
    capture_version_metadata,
    validate_version_metadata,
    version_metadata_ref,
)


class VersionMetadataTests(unittest.TestCase):
    def test_capture_version_metadata_has_required_local_fields(self) -> None:
        metadata = capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")

        self.assertEqual(metadata["version_metadata_schema_version"], 1)
        self.assertEqual(metadata["manifest_schema_version"], 1)
        self.assertEqual(metadata["validation_schema_version"], 1)
        for field in (
            "node_software_version",
            "build_identifier",
            "runtime_name",
            "runtime_version",
            "operating_system",
            "architecture",
            "captured_at",
        ):
            self.assertIsInstance(metadata[field], str)
            self.assertNotEqual(metadata[field], "")
        self.assertEqual(validate_version_metadata(metadata), metadata)
        self.assertRegex(
            version_metadata_ref(metadata), r"^version-metadata-sha256:[0-9a-f]{64}$"
        )

    def test_capture_version_metadata_falls_back_to_package_version_constant(
        self,
    ) -> None:
        with mock.patch(
            "aethermesh_core.version_metadata.metadata.version",
            side_effect=metadata.PackageNotFoundError,
        ):
            captured = capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")

        self.assertEqual(captured["node_software_version"], __version__)

    def test_version_metadata_rejects_unknown_fields(self) -> None:
        captured = capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")
        captured["local_path"] = "/Users/example/build"

        with self.assertRaisesRegex(ValueError, "unsupported fields: local_path"):
            validate_version_metadata(captured)

    def test_capture_version_metadata_rejects_path_like_build_identifier(self) -> None:
        with (
            mock.patch.dict(
                "os.environ", {"AETHERMESH_BUILD_ID": "/Users/example/build"}
            ),
            self.assertRaisesRegex(ValueError, "build_identifier"),
        ):
            capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")

    def test_identity_manifest_validation_fails_for_missing_version_metadata(
        self,
    ) -> None:
        document = _identity_document(
            NodeIdentity(node_id="node-a"),
            created_at="2026-07-08T00:00:00+00:00",
        )
        assert isinstance(document["references"], dict)
        document["references"].pop("version_metadata")

        with self.assertRaisesRegex(
            IdentityPersistenceError,
            "version metadata must be a JSON object",
        ):
            _load_identity_document(document)

    def test_identity_manifest_validation_fails_for_malformed_version_metadata(
        self,
    ) -> None:
        document = _identity_document(
            NodeIdentity(node_id="node-a"),
            created_at="2026-07-08T00:00:00+00:00",
        )
        assert isinstance(document["references"], dict)
        document["references"]["version_metadata"] = {
            "version_metadata_schema_version": 1,
            "node_software_version": "0.2.0a0",
        }

        with self.assertRaisesRegex(
            IdentityPersistenceError,
            "version metadata missing required fields",
        ):
            _load_identity_document(document)

    def test_receipt_references_same_version_metadata_used_for_run(self) -> None:
        metadata = capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")
        metadata_ref = version_metadata_ref(metadata)

        document = build_receipt_document([], version_metadata=metadata)

        self.assertEqual(document["version_metadata"], metadata)
        self.assertEqual(document["version_metadata_ref"], metadata_ref)

    def test_receipt_validation_rejects_mismatched_metadata_ref(self) -> None:
        metadata = capture_version_metadata(captured_at="2026-07-08T00:00:00+00:00")
        document = build_receipt_document([], version_metadata=metadata)
        document["version_metadata_ref"] = "version-metadata-sha256:" + "0" * 64

        with self.assertRaisesRegex(
            ReceiptPersistenceError,
            "version_metadata_ref",
        ):
            build_receipt_document([], existing_document=document)


def _load_identity_document(document: dict[str, object]) -> None:
    import json
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as temp_dir:
        path = Path(temp_dir) / "identity.json"
        path.write_text(json.dumps(document), encoding="utf-8")
        _load_identity(path)


if __name__ == "__main__":
    unittest.main()
