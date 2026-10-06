import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from aethermesh_core import identity as identity_module
from aethermesh_core.identity import (
    HardwareIdentityInputs,
    IdentityPersistenceError,
    _backup_identity_referenced_artifacts,
    _bytes_to_gb,
    _canonical_root_json,
    _colon_value,
    _component_hashes,
    _csv_first_value,
    _default_goos,
    _extract_labeled_value,
    _extract_mac_addresses,
    _identity_document,
    _index_from_hash,
    _linux_gpu_device_id,
    _linux_gpu_model,
    _linux_gpu_vendor,
    _linux_memtotal_gb,
    _linux_physical_core_count,
    _load_identity_reset_receipts,
    _local_identity_ref_path,
    _max_csv_int,
    _max_gpu_vram_gb,
    _new_local_node_id,
    _node_name_from_hashes,
    _node_name_wordlist_dir,
    _node_name_wordlists,
    _normalize_mac_address,
    _physical_core_count_fallback,
    _physical_mac_addresses,
    _read_or_empty,
    _read_text_file,
    _round_installed_ram_gb,
    _run_command,
    _run_or_empty,
    _safe_int,
    _save_identity,
    _string_list_from_section,
    _unique_reset_artifact_path,
    _vram_value_to_gb,
    collect_hardware_identity_inputs,
    deterministic_machine_node_id,
    deterministic_machine_node_name,
    load_or_create_identity,
    parse_local_node_identity_document,
    reset_identity,
)
from aethermesh_core.models import NodeIdentity
from aethermesh_core.receipts import (
    load_receipt_document_if_exists,
    write_receipt_document,
)
from aethermesh_core.version_metadata import (
    capture_version_metadata,
    version_metadata_ref,
)


def _hardware(**overrides: object) -> HardwareIdentityInputs:
    values = {
        "cpu_architecture": " arm64 ",
        "cpu_vendor": " Apple ",
        "cpu_brand_or_chip_name": " Apple   M4 Max ",
        "physical_core_count": 16,
        "logical_thread_count": 16,
        "permanent_mac_addresses": ("aa:bb:cc:dd:ee:ff", " 11-22-33-44-55-66 "),
        "gpu_vendor": "Apple",
        "gpu_model_or_chip_name": "M4 Max GPU",
        "gpu_device_id_if_available": "0x1234",
        "gpu_count": 1,
        "max_gpu_vram_gb": 64,
        "total_installed_ram_gb": 128.2,
    }
    values.update(overrides)
    return HardwareIdentityInputs(**values)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _expected_node_id(hardware: HardwareIdentityInputs) -> str:
    hashes = _component_hashes(hardware)
    return _sha(_canonical_root_json(hashes))


def _test_version_metadata(
    captured_at: str = "2026-07-08T00:00:00+00:00",
) -> dict[str, object]:
    return capture_version_metadata(captured_at=captured_at)


class IdentityPersistenceTests(unittest.TestCase):
    def test_public_local_node_identity_example_parses(self) -> None:
        example_path = (
            Path(__file__).parents[1] / "examples" / "local-node-identity.json"
        )
        document = json.loads(example_path.read_text(encoding="utf-8"))

        identity = parse_local_node_identity_document(document)

        self.assertEqual(identity.node_id, "node-local-7f3a9c2e")
        self.assertEqual(identity.creator_node_id, "node-local-7f3a9c2e")
        self.assertEqual(identity.created_at, "2026-07-08T00:00:00Z")
        self.assertEqual(identity.identity_version, 1)
        self.assertEqual(identity.public_key, "ed25519-pub-local-example-7f3a9c2e")
        self.assertEqual(
            identity.manifest_ref,
            "examples/local-batch.json#node:node-local-7f3a9c2e",
        )
        self.assertEqual(
            identity.local_metadata,
            {"display_name": "Local prototype node", "environment": "dev-fixture"},
        )
        self.assertEqual(identity.to_document(), document)
        parse_local_node_identity_document(document | {"local_metadata": {"notes": []}})

    def test_creator_node_id_is_preserved_separately_from_derived_record_ids(
        self,
    ) -> None:
        identity = parse_local_node_identity_document(
            {
                "node_id": "node-local-worker-a",
                "creator_node_id": "node-local-creator-a",
                "created_at": "2026-07-08T00:00:00+00:00",
                "identity_version": 1,
                "public_key": "ed25519-pub-local-worker-a",
                "manifest_ref": "examples/local-batch.json#node:node-local-worker-a",
            }
        )
        generated_record_ids = {
            "work_id": "work-0001",
            "receipt_id": "receipt-0001",
            "lineage_id": "lineage-0001",
            "contribution_record_id": "contribution-0001",
        }

        self.assertEqual(identity.creator_node_id, "node-local-creator-a")
        self.assertNotIn(identity.creator_node_id, generated_record_ids.values())
        self.assertNotEqual(identity.creator_node_id, identity.node_id)
        self.assertEqual(
            identity.to_document(),
            {
                "node_id": "node-local-worker-a",
                "creator_node_id": "node-local-creator-a",
                "created_at": "2026-07-08T00:00:00+00:00",
                "identity_version": 1,
                "public_key": "ed25519-pub-local-worker-a",
                "manifest_ref": "examples/local-batch.json#node:node-local-worker-a",
            },
        )

    def test_public_local_node_identity_rejects_secret_material(self) -> None:
        document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "identity_version": 1,
            "public_key": "ed25519-pub-local-a",
            "manifest_ref": "examples/local-batch.json#node:node-local-a",
            "local_metadata": {"private_key_path": "keys/node-local-a.key"},
        }

        with self.assertRaisesRegex(
            IdentityPersistenceError,
            "must not contain private key material",
        ):
            parse_local_node_identity_document(document)

    def test_public_local_node_identity_rejects_secret_material_in_lists(self) -> None:
        document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "identity_version": 1,
            "public_key": "ed25519-pub-local-a",
            "manifest_ref": "examples/local-batch.json#node:node-local-a",
            "local_metadata": {"notes": [{"secret_seed": "do-not-copy"}]},
        }

        with self.assertRaisesRegex(
            IdentityPersistenceError,
            "must not contain private key material",
        ):
            parse_local_node_identity_document(document)

    def test_public_local_node_identity_rejects_secret_material_name_variants(
        self,
    ) -> None:
        base_document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "identity_version": 1,
            "public_key": "ed25519-pub-local-a",
            "manifest_ref": "examples/local-batch.json#node:node-local-a",
        }

        for secret_field in ("privateKey", "secret-key", "seed"):
            with (
                self.subTest(secret_field=secret_field),
                self.assertRaisesRegex(
                    IdentityPersistenceError,
                    "must not contain private key material",
                ),
            ):
                parse_local_node_identity_document(
                    base_document | {"local_metadata": {secret_field: "do-not-copy"}}
                )

    def test_public_local_node_identity_rejects_non_integer_version(self) -> None:
        base_document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "public_key": "ed25519-pub-local-a",
            "manifest_ref": "examples/local-batch.json#node:node-local-a",
        }

        for identity_version in (True, "1", 2):
            with (
                self.subTest(identity_version=identity_version),
                self.assertRaisesRegex(
                    IdentityPersistenceError,
                    "identity_version must be integer 1",
                ),
            ):
                parse_local_node_identity_document(
                    base_document | {"identity_version": identity_version}
                )

    def test_public_local_node_identity_rejects_nonlocal_manifest_ref(self) -> None:
        base_document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "identity_version": 1,
            "public_key": "ed25519-pub-local-a",
        }
        denied_refs = (
            "registry://nodes/node-local-a",
            "https://example.invalid/nodes/node-local-a",
            "/var/tmp/aethermesh/manifest.json",
            "~/.aethermesh/manifest.json",
            "../outside-repo/manifest.json",
            "C:\\Users\\example\\manifest.json",
            "manifests/local batch.json#node:node-local-a",
            "manifests/local-batch.json#",
            "#node:node-local-a",
            "manifests/local-batch.json#bad fragment",
        )

        for manifest_ref in denied_refs:
            with (
                self.subTest(manifest_ref=manifest_ref),
                self.assertRaisesRegex(
                    IdentityPersistenceError,
                    "manifest_ref must be a local file or fixture reference",
                ),
            ):
                parse_local_node_identity_document(
                    base_document | {"manifest_ref": manifest_ref}
                )

    def test_public_local_node_identity_rejects_secret_manifest_ref(self) -> None:
        base_document = {
            "node_id": "node-local-a",
            "creator_node_id": "node-local-a",
            "created_at": "2026-07-08T00:00:00Z",
            "identity_version": 1,
            "public_key": "ed25519-pub-local-a",
        }

        for manifest_ref in (
            "keys/private_key.json#node:node-local-a",
            "keys/private-key.json#node:node-local-a",
            "keys/seed.json#node:node-local-a",
        ):
            with (
                self.subTest(manifest_ref=manifest_ref),
                self.assertRaisesRegex(
                    IdentityPersistenceError,
                    "manifest_ref must not reference private key material",
                ),
            ):
                parse_local_node_identity_document(
                    base_document | {"manifest_ref": manifest_ref}
                )

    def test_node_name_wordlists_exist_and_are_valid(self) -> None:
        wordlists = _node_name_wordlists()
        self.assertEqual(set(wordlists), {"cpu", "mac", "gpu", "ram"})
        denied_public_name_fragments = {
            "behead",
            "bitch",
            "blood",
            "bomb",
            "boob",
            "brib",
            "cancer",
            "cocaine",
            "corpse",
            "cunt",
            "death",
            "destruct",
            "dick",
            "disease",
            "extermin",
            "extortion",
            "fecal",
            "fraud",
            "fuck",
            "haram",
            "hate",
            "heroin",
            "hitler",
            "jihad",
            "kidnap",
            "lucifer",
            "malice",
            "malware",
            "murder",
            "nazi",
            "obscen",
            "openai",
            "pedo",
            "plague",
            "porn",
            "rape",
            "rectal",
            "sabotag",
            "satan",
            "scam",
            "sex",
            "shit",
            "sinful",
            "slut",
            "spank",
            "suicide",
            "terror",
            "testicular",
            "tequila",
            "thug",
            "traitor",
            "trojan",
            "venmo",
            "virus",
            "vomit",
            "weapon",
            "weed",
            "whore",
        }
        for words in wordlists.values():
            self.assertEqual(len(words), 16384)
            self.assertEqual(len(set(words)), 16384)
            for word in words:
                self.assertRegex(word, r"^[a-z]+$")
                self.assertGreaterEqual(len(word), 3)
                self.assertFalse(
                    any(fragment in word for fragment in denied_public_name_fragments),
                    word,
                )

    def test_packaged_wordlist_dir_falls_back_to_install_prefix(self) -> None:
        with (
            patch.object(
                identity_module, "NODE_NAME_WORDLIST_DIR", Path("/definitely/missing")
            ),
            patch.object(sys, "prefix", "/tmp/aethermesh-prefix"),
        ):
            self.assertEqual(
                _node_name_wordlist_dir(),
                Path("/tmp/aethermesh-prefix") / "wordlists" / "node-names",
            )

    def test_deterministic_node_name_uses_component_hash_words_and_node_tag(
        self,
    ) -> None:
        hardware = _hardware()
        hashes = _component_hashes(hardware)
        node_id = deterministic_machine_node_id(hardware_inputs=hardware)
        node_name = deterministic_machine_node_name(hardware_inputs=hardware)
        wordlists = _node_name_wordlists()

        self.assertRegex(node_name, r"^[a-z]+-[a-z]+-[a-z]+-[a-z]+_[a-f0-9]{6}$")
        self.assertEqual(node_name.rsplit("_", 1)[1], node_id[:6])
        self.assertEqual(
            node_name,
            "-".join(
                [
                    wordlists["cpu"][_index_from_hash(hashes.cpu_hash)],
                    wordlists["mac"][_index_from_hash(hashes.mac_hash)],
                    wordlists["gpu"][_index_from_hash(hashes.gpu_hash)],
                    wordlists["ram"][_index_from_hash(hashes.ram_hash)],
                ]
            )
            + f"_{node_id[:6]}",
        )
        self.assertEqual(
            node_name, deterministic_machine_node_name(hardware_inputs=hardware)
        )
        self.assertEqual(node_id, _expected_node_id(hardware))
        self.assertFalse(node_id.startswith("local-"))

    def test_changing_component_hashes_changes_matching_node_name_word(self) -> None:
        baseline_hashes = _component_hashes(_hardware())
        baseline_id = deterministic_machine_node_id(hardware_inputs=_hardware())
        baseline_words = (
            _node_name_from_hashes(baseline_hashes, baseline_id)
            .split("_", 1)[0]
            .split("-")
        )

        scenarios = [
            ("cpu", _hardware(cpu_brand_or_chip_name="Apple M3 Ultra"), 0),
            (
                "mac",
                _hardware(
                    permanent_mac_addresses=("AA:BB:CC:DD:EE:FF", "77:88:99:AA:BB:CC")
                ),
                1,
            ),
            ("gpu", _hardware(gpu_model_or_chip_name="M4 Ultra GPU"), 2),
            ("ram", _hardware(total_installed_ram_gb=96), 3),
        ]
        for _label, hardware, changed_index in scenarios:
            changed_hashes = _component_hashes(hardware)
            changed_id = deterministic_machine_node_id(hardware_inputs=hardware)
            changed_words = (
                _node_name_from_hashes(changed_hashes, changed_id)
                .split("_", 1)[0]
                .split("-")
            )
            self.assertNotEqual(
                changed_words[changed_index], baseline_words[changed_index]
            )
            for stable_index in set(range(4)) - {changed_index}:
                self.assertEqual(
                    changed_words[stable_index], baseline_words[stable_index]
                )

    def test_changing_node_id_changes_only_node_name_suffix(self) -> None:
        hashes = _component_hashes(_hardware())
        first = _node_name_from_hashes(hashes, "a" * 64)
        second = _node_name_from_hashes(hashes, "b" * 64)

        self.assertEqual(first.split("_", 1)[0], second.split("_", 1)[0])
        self.assertTrue(first.endswith("_aaaaaa"))
        self.assertTrue(second.endswith("_bbbbbb"))

    def test_identity_document_records_creator_timestamp_and_provenance(self) -> None:
        version_metadata = _test_version_metadata()
        self.assertEqual(
            _identity_document(
                NodeIdentity(node_id="local-node", node_name="test-node_123456"),
                created_at="2026-07-08T00:00:00+00:00",
            ),
            {
                "version": 1,
                "node": {
                    "node_id": "local-node",
                    "node_name": "test-node_123456",
                    "creator_node_id": "local-node",
                    "created_at": "2026-07-08T00:00:00+00:00",
                },
                "provenance": {
                    "created_by": "aethermesh_core.identity.load_or_create_identity",
                    "source": "local-first-initialization",
                    "creation_event": "identity_manifest_created",
                    "load_behavior": "reuse_existing_identity_without_overwrite",
                    "authority": "local-only-no-network-consensus",
                },
                "references": {
                    "manifest_refs": [],
                    "validation_receipt_refs": [],
                    "version_metadata": version_metadata,
                },
                "lineage": {
                    "parent_node_ids": [],
                    "lineage_links": [],
                },
                "contribution_attribution": {
                    "creator_node_id": "local-node",
                    "attribution_node_id": "local-node",
                    "contribution_refs": [],
                },
            },
        )

        self.assertEqual(
            _identity_document(
                NodeIdentity(node_id="local-node"),
                created_at="2026-07-08T00:00:00+00:00",
            ),
            {
                "version": 1,
                "node": {
                    "node_id": "local-node",
                    "creator_node_id": "local-node",
                    "created_at": "2026-07-08T00:00:00+00:00",
                },
                "provenance": {
                    "created_by": "aethermesh_core.identity.load_or_create_identity",
                    "source": "local-first-initialization",
                    "creation_event": "identity_manifest_created",
                    "load_behavior": "reuse_existing_identity_without_overwrite",
                    "authority": "local-only-no-network-consensus",
                },
                "references": {
                    "manifest_refs": [],
                    "validation_receipt_refs": [],
                    "version_metadata": version_metadata,
                },
                "lineage": {
                    "parent_node_ids": [],
                    "lineage_links": [],
                },
                "contribution_attribution": {
                    "creator_node_id": "local-node",
                    "attribution_node_id": "local-node",
                    "contribution_refs": [],
                },
            },
        )

    def test_missing_identity_file_is_created_with_deterministic_hardware_node_id(
        self,
    ) -> None:
        hardware = _hardware()
        expected_node_id = _expected_node_id(hardware)
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "deep" / "nested" / "local-node.json"

            with patch(
                "aethermesh_core.identity._new_local_node_id",
                side_effect=AssertionError("random node id must not be used"),
            ):
                identity = load_or_create_identity(
                    identity_path, hardware_inputs=hardware
                )
            persisted = json.loads(identity_path.read_text(encoding="utf-8"))
            second = load_or_create_identity(
                identity_path,
                hardware_inputs=_hardware(cpu_brand_or_chip_name="Different CPU"),
            )
            persisted_after_second_load = json.loads(
                identity_path.read_text(encoding="utf-8")
            )

        self.assertEqual(identity.node_id, expected_node_id)
        self.assertEqual(second.node_id, identity.node_id)
        self.assertEqual(second.node_name, identity.node_name)
        self.assertEqual(persisted_after_second_load, persisted)
        self.assertEqual(
            identity.node_name,
            deterministic_machine_node_name(
                hardware_inputs=hardware, node_id=expected_node_id
            ),
        )
        self.assertRegex(identity.node_id, r"^[0-9a-f]{64}$")
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(
            persisted["node"],
            {
                "node_id": identity.node_id,
                "node_name": identity.node_name,
                "creator_node_id": identity.node_id,
                "created_at": persisted["node"]["created_at"],
            },
        )
        self.assertEqual(persisted["node"]["creator_node_id"], identity.node_id)
        self.assertRegex(
            persisted["node"]["created_at"],
            r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00$",
        )
        created_at = datetime.fromisoformat(persisted["node"]["created_at"])
        self.assertEqual(created_at.tzinfo, UTC)
        self.assertEqual(
            persisted_after_second_load["node"]["created_at"],
            persisted["node"]["created_at"],
        )
        self.assertEqual(
            persisted["provenance"],
            {
                "created_by": "aethermesh_core.identity.load_or_create_identity",
                "source": "local-first-initialization",
                "creation_event": "identity_manifest_created",
                "load_behavior": "reuse_existing_identity_without_overwrite",
                "authority": "local-only-no-network-consensus",
            },
        )
        self.assertEqual(persisted["references"]["manifest_refs"], [])
        self.assertEqual(persisted["references"]["validation_receipt_refs"], [])
        self.assertEqual(
            persisted["references"]["version_metadata"]["captured_at"],
            persisted["node"]["created_at"],
        )
        self.assertEqual(
            persisted_after_second_load["references"], persisted["references"]
        )
        self.assertEqual(
            persisted["lineage"],
            {"parent_node_ids": [], "lineage_links": []},
        )
        self.assertEqual(
            persisted["contribution_attribution"],
            {
                "creator_node_id": identity.node_id,
                "attribution_node_id": identity.node_id,
                "contribution_refs": [],
            },
        )

    def test_missing_identity_file_can_use_explicit_node_id_factory_override(
        self,
    ) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"

            identity = load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )

        self.assertEqual(identity.node_id, "a" * 64)
        self.assertEqual(
            identity.node_name,
            deterministic_machine_node_name(hardware_inputs=hardware, node_id="a" * 64),
        )
        self.assertNotEqual(identity.node_id, _expected_node_id(hardware))

    def test_identity_creation_refuses_to_overwrite_existing_file_without_reset(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            _save_identity(identity_path, NodeIdentity(node_id="a" * 64))
            original = json.loads(identity_path.read_text(encoding="utf-8"))

            with self.assertRaisesRegex(IdentityPersistenceError, "explicit reset"):
                _save_identity(identity_path, NodeIdentity(node_id="b" * 64))

            after_attempt = json.loads(identity_path.read_text(encoding="utf-8"))

        self.assertEqual(after_attempt, original)

    def test_identity_creation_race_reuses_existing_winner(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"

            def racing_atomic_create(path: Path, document: dict[str, object]) -> None:
                self.assertEqual(path, identity_path)
                node_document = document["node"]
                if not isinstance(node_document, dict):
                    self.fail("identity document node must be an object")
                self.assertEqual(node_document.get("node_id"), "b" * 64)
                identity_path.write_text(
                    json.dumps(_identity_document(NodeIdentity(node_id="a" * 64))),
                    encoding="utf-8",
                )
                raise FileExistsError("identity won by another initializer")

            with patch(
                "aethermesh_core.identity.atomic_create_json",
                side_effect=racing_atomic_create,
            ):
                loaded = load_or_create_identity(
                    identity_path,
                    node_id_factory=lambda: "b" * 64,
                )

        self.assertEqual(loaded.node_id, "a" * 64)

    def test_identity_creation_removes_successful_atomic_create_temp_file(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"

            _save_identity(identity_path, NodeIdentity(node_id="a" * 64))
            temp_files = [
                path
                for path in Path(temp_dir).iterdir()
                if path.name != identity_path.name
            ]

        self.assertEqual(temp_files, [])

    def test_identity_creation_temp_file_failure_does_not_leave_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"

            with (
                patch(
                    "aethermesh_core.json_io.tempfile.NamedTemporaryFile",
                    side_effect=OSError("no temp space"),
                ),
                self.assertRaisesRegex(IdentityPersistenceError, "no temp space"),
            ):
                _save_identity(identity_path, NodeIdentity(node_id="a" * 64))

            self.assertFalse(identity_path.exists())

    def test_explicit_identity_reset_quarantines_old_identity_and_records_receipt(
        self,
    ) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            quarantine_dir = Path(temp_dir) / "quarantine"
            first = load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )
            original_document = json.loads(identity_path.read_text(encoding="utf-8"))
            original_document["references"]["manifest_refs"] = [
                "manifests/local-batch.json#node:aaaaaaaaaaaa"
            ]
            original_document["references"]["validation_receipt_refs"] = [
                "receipts/receipt-0001.json"
            ]
            original_document["lineage"]["parent_node_ids"] = ["creator-root"]
            original_document["lineage"]["lineage_links"] = [
                "lineage/local-node-link.json"
            ]
            original_document["contribution_attribution"]["contribution_refs"] = [
                "contributions/contribution-0001.json"
            ]
            identity_path.write_text(json.dumps(original_document), encoding="utf-8")
            referenced_files = {
                "manifests/local-batch.json": {"kind": "manifest"},
                "receipts/receipt-0001.json": {"kind": "receipt"},
                "lineage/local-node-link.json": {"kind": "lineage"},
                "contributions/contribution-0001.json": {"kind": "contribution"},
            }
            for relative_path, document in referenced_files.items():
                artifact_path = Path(temp_dir) / relative_path
                artifact_path.parent.mkdir(parents=True, exist_ok=True)
                artifact_path.write_text(json.dumps(document), encoding="utf-8")

            result = reset_identity(
                identity_path,
                reason="operator requested recovery",
                quarantine_dir=quarantine_dir,
                hardware_inputs=hardware,
                node_id_factory=lambda: "b" * 64,
            )
            reset_document = json.loads(identity_path.read_text(encoding="utf-8"))
            backup_document = json.loads(
                Path(result.backup_path).read_text(encoding="utf-8")
            )
            receipt_document = json.loads(
                Path(result.audit_receipt_path).read_text(encoding="utf-8")
            )
            copied_reference_documents = {
                backup_file.name: json.loads(backup_file.read_text(encoding="utf-8"))
                for backup_file in quarantine_dir.glob("*/*.json")
            }
            reloaded = load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "c" * 64,
            )

        self.assertEqual(first.node_id, "a" * 64)
        self.assertEqual(result.previous_node_id, "a" * 64)
        self.assertEqual(result.new_node_id, "b" * 64)
        self.assertEqual(
            result.to_dict(),
            {
                "previous_node_id": "a" * 64,
                "new_node_id": "b" * 64,
                "backup_path": result.backup_path,
                "audit_receipt_path": result.audit_receipt_path,
                "warning": result.warning,
            },
        )
        self.assertIn("lineage and contribution attribution continuity", result.warning)
        self.assertEqual(backup_document, original_document)
        self.assertEqual(reloaded.node_id, "b" * 64)
        self.assertEqual(reset_document["node"]["node_id"], "b" * 64)
        self.assertEqual(reset_document["node"]["creator_node_id"], "a" * 64)
        self.assertEqual(reset_document["references"]["manifest_refs"], [])
        self.assertEqual(reset_document["references"]["validation_receipt_refs"], [])
        self.assertEqual(
            reset_document["lineage"], {"parent_node_ids": [], "lineage_links": []}
        )
        self.assertEqual(
            reset_document["contribution_attribution"],
            {
                "creator_node_id": "a" * 64,
                "attribution_node_id": "b" * 64,
                "contribution_refs": [],
            },
        )
        self.assertEqual(receipt_document["version"], 1)
        self.assertEqual(receipt_document["receipt_type"], "identity_reset_audit")
        self.assertEqual(len(receipt_document["reset_receipts"]), 1)
        receipt = receipt_document["reset_receipts"][0]
        self.assertEqual(receipt["event"], "identity_reset")
        self.assertEqual(receipt["previous_node_id"], "a" * 64)
        self.assertEqual(receipt["new_node_id"], "b" * 64)
        self.assertEqual(receipt["previous_creator_node_id"], "a" * 64)
        self.assertEqual(receipt["new_creator_node_id"], "a" * 64)
        self.assertFalse(receipt["full_local_identity_rotation"])
        self.assertEqual(
            receipt["files_backed_up"],
            [
                Path(result.backup_path).name,
                "local-batch.json",
                "receipt-0001.json",
                "local-node-link.json",
                "contribution-0001.json",
            ],
        )
        self.assertEqual(
            copied_reference_documents,
            {
                "local-batch.json": {"kind": "manifest"},
                "receipt-0001.json": {"kind": "receipt"},
                "local-node-link.json": {"kind": "lineage"},
                "contribution-0001.json": {"kind": "contribution"},
            },
        )
        self.assertEqual(
            receipt["backed_up_identity_sections"],
            [
                "node",
                "references.manifest_refs",
                "references.validation_receipt_refs",
                "lineage",
                "contribution_attribution",
            ],
        )
        self.assertEqual(receipt["reason"], "operator requested recovery")
        self.assertEqual(receipt["identity_path"], identity_path.name)
        self.assertEqual(
            receipt["quarantined_identity_path"], Path(result.backup_path).name
        )
        self.assertNotIn(temp_dir, receipt["identity_path"])
        self.assertNotIn(temp_dir, receipt["quarantined_identity_path"])
        self.assertEqual(
            receipt["active_identity_binding"],
            {
                "manifest_node_id": "b" * 64,
                "validation_receipt_node_id": "b" * 64,
                "lineage_node_id": "b" * 64,
                "contribution_attribution_node_id": "b" * 64,
                "creator_node_id": "a" * 64,
            },
        )

    def test_identity_reset_can_explicitly_rotate_creator_identity(self) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )

            result = reset_identity(
                identity_path,
                rotate_creator_identity=True,
                hardware_inputs=hardware,
                node_id_factory=lambda: "b" * 64,
            )
            reset_document = json.loads(identity_path.read_text(encoding="utf-8"))
            receipt_document = json.loads(
                Path(result.audit_receipt_path).read_text(encoding="utf-8")
            )

        self.assertEqual(reset_document["node"]["creator_node_id"], "b" * 64)
        self.assertEqual(
            reset_document["contribution_attribution"]["creator_node_id"], "b" * 64
        )
        receipt = receipt_document["reset_receipts"][0]
        self.assertTrue(receipt["full_local_identity_rotation"])
        self.assertEqual(receipt["previous_creator_node_id"], "a" * 64)
        self.assertEqual(receipt["new_creator_node_id"], "b" * 64)

    def test_identity_reset_defaults_to_deterministic_hardware_node_id(self) -> None:
        hardware = _hardware()
        expected_node_id = _expected_node_id(hardware)
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )

            with patch(
                "aethermesh_core.identity._new_local_node_id",
                side_effect=AssertionError("random node id must not be used"),
            ):
                result = reset_identity(identity_path, hardware_inputs=hardware)
            reset_document = json.loads(identity_path.read_text(encoding="utf-8"))

        self.assertEqual(result.previous_node_id, "a" * 64)
        self.assertEqual(result.new_node_id, expected_node_id)
        self.assertEqual(reset_document["node"]["node_id"], expected_node_id)
        self.assertEqual(
            reset_document["node"]["node_name"],
            deterministic_machine_node_name(
                hardware_inputs=hardware, node_id=expected_node_id
            ),
        )
        self.assertEqual(reset_document["node"]["creator_node_id"], "a" * 64)

    def test_identity_reset_reference_backup_ignores_non_local_or_missing_refs(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            backup_root = Path(temp_dir) / "quarantine"
            document = {
                "references": {
                    "manifest_refs": [
                        "missing.json",
                        "https://example.invalid/manifest.json",
                        "../outside.json",
                    ],
                    "validation_receipt_refs": "not-a-list",
                },
                "lineage": "not-a-section",
                "contribution_attribution": {"contribution_refs": [""]},
            }

            copied = _backup_identity_referenced_artifacts(
                document,
                identity_path=identity_path,
                backup_root=backup_root,
            )

        self.assertEqual(copied, [])
        self.assertEqual(
            _string_list_from_section(document, "lineage", "lineage_links"), []
        )
        self.assertEqual(
            _string_list_from_section(
                document, "references", "validation_receipt_refs"
            ),
            [],
        )
        self.assertIsNone(
            _local_identity_ref_path(
                "https://example.invalid/manifest.json", identity_path=identity_path
            )
        )
        self.assertIsNone(
            _local_identity_ref_path("../outside.json", identity_path=identity_path)
        )

    def test_identity_reset_reference_backup_copy_failure_is_reported(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            artifact_path = Path(temp_dir) / "manifests" / "local-batch.json"
            artifact_path.parent.mkdir(parents=True, exist_ok=True)
            artifact_path.write_text("{}", encoding="utf-8")
            document = {
                "references": {
                    "manifest_refs": ["manifests/local-batch.json"],
                    "validation_receipt_refs": [],
                },
                "lineage": {"lineage_links": []},
                "contribution_attribution": {"contribution_refs": []},
            }

            with (
                patch(
                    "aethermesh_core.identity.shutil.copy2",
                    side_effect=OSError("disk full"),
                ),
                self.assertRaisesRegex(
                    IdentityPersistenceError, "referenced identity artifact"
                ),
            ):
                _backup_identity_referenced_artifacts(
                    document,
                    identity_path=identity_path,
                    backup_root=Path(temp_dir) / "quarantine",
                )

    def test_identity_reset_requires_existing_identity_without_creating_one(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"

            with self.assertRaisesRegex(IdentityPersistenceError, "existing identity"):
                reset_identity(identity_path, node_id_factory=lambda: "b" * 64)

            self.assertFalse(identity_path.exists())

    def test_identity_reset_artifact_paths_avoid_collisions(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            (root / "identity-reset.json").write_text("{}", encoding="utf-8")

            path = _unique_reset_artifact_path(root, "identity-reset", ".json")

        self.assertEqual(path.name, "identity-reset-1.json")

    def test_identity_reset_receipt_loader_rejects_invalid_existing_receipts(
        self,
    ) -> None:
        invalid_documents = [
            ("not json", "malformed"),
            (json.dumps([]), "must be an object"),
            (json.dumps({"version": 2}), "version 1"),
            (json.dumps({"version": 1, "receipt_type": "other"}), "receipt_type"),
            (
                json.dumps({"version": 1, "receipt_type": "identity_reset_audit"}),
                "reset_receipts",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "identity-reset-receipts.json"
            for contents, message in invalid_documents:
                with self.subTest(message=message):
                    receipt_path.write_text(contents, encoding="utf-8")

                    with self.assertRaisesRegex(IdentityPersistenceError, message):
                        _load_identity_reset_receipts(receipt_path)

    def test_identity_reset_receipt_loader_accepts_valid_existing_receipt(self) -> None:
        document = {
            "version": 1,
            "receipt_type": "identity_reset_audit",
            "reset_receipts": [{"event": "identity_reset"}],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "identity-reset-receipts.json"
            receipt_path.write_text(json.dumps(document), encoding="utf-8")

            loaded = _load_identity_reset_receipts(receipt_path)

        self.assertEqual(loaded, document)

    def test_identity_reset_invalid_audit_receipt_does_not_replace_identity(
        self,
    ) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            receipt_path = Path(temp_dir) / "identity-reset-receipts.json"
            load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )
            original = json.loads(identity_path.read_text(encoding="utf-8"))
            receipt_path.write_text("not-json", encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "malformed"):
                reset_identity(
                    identity_path,
                    audit_receipt_path=receipt_path,
                    hardware_inputs=hardware,
                    node_id_factory=lambda: "b" * 64,
                )

            after_reset_attempt = json.loads(identity_path.read_text(encoding="utf-8"))

        self.assertEqual(after_reset_attempt, original)

    def test_identity_reset_receipt_write_failure_restores_previous_identity(
        self,
    ) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            load_or_create_identity(
                identity_path,
                hardware_inputs=hardware,
                node_id_factory=lambda: "a" * 64,
            )
            original = json.loads(identity_path.read_text(encoding="utf-8"))

            with (
                patch(
                    "aethermesh_core.identity.atomic_write_json",
                    side_effect=[None, OSError("receipt disk full")],
                ),
                self.assertRaisesRegex(IdentityPersistenceError, "audit receipt"),
            ):
                reset_identity(
                    identity_path,
                    hardware_inputs=hardware,
                    node_id_factory=lambda: "b" * 64,
                )

            after_reset_attempt = json.loads(identity_path.read_text(encoding="utf-8"))

        self.assertEqual(after_reset_attempt, original)

    def test_identity_created_at_must_be_utc(self) -> None:
        document = _identity_document(
            NodeIdentity(node_id="local-node"),
            created_at="2026-07-08T00:00:00-04:00",
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            identity_path.write_text(json.dumps(document), encoding="utf-8")

            with self.assertRaisesRegex(
                IdentityPersistenceError,
                "created_at must be a UTC timestamp",
            ):
                load_or_create_identity(identity_path)

    def test_new_local_node_id_uses_collision_resistant_hex_randomness(self) -> None:
        first = _new_local_node_id()
        second = _new_local_node_id()

        self.assertRegex(first, r"^[0-9a-f]{64}$")
        self.assertRegex(second, r"^[0-9a-f]{64}$")
        self.assertNotEqual(first, second)

    def test_same_hardware_inputs_always_produce_same_node_id(self) -> None:
        first = deterministic_machine_node_id(hardware_inputs=_hardware())
        second = deterministic_machine_node_id(hardware_inputs=_hardware())

        self.assertEqual(first, second)
        self.assertEqual(first, _expected_node_id(_hardware()))
        self.assertRegex(first, r"^[0-9a-f]{64}$")

    def test_changing_cpu_info_changes_cpu_hash_and_node_id(self) -> None:
        baseline = _component_hashes(_hardware())
        changed = _component_hashes(_hardware(cpu_brand_or_chip_name="Apple M3 Ultra"))

        self.assertNotEqual(changed.cpu_hash, baseline.cpu_hash)
        self.assertEqual(changed.mac_hash, baseline.mac_hash)
        self.assertNotEqual(
            deterministic_machine_node_id(
                hardware_inputs=_hardware(cpu_brand_or_chip_name="Apple M3 Ultra")
            ),
            deterministic_machine_node_id(hardware_inputs=_hardware()),
        )

    def test_changing_mac_set_changes_mac_hash_and_node_id(self) -> None:
        baseline = _component_hashes(_hardware())
        changed = _component_hashes(
            _hardware(
                permanent_mac_addresses=("AA:BB:CC:DD:EE:FF", "77:88:99:AA:BB:CC")
            )
        )

        self.assertNotEqual(changed.mac_hash, baseline.mac_hash)
        self.assertEqual(changed.cpu_hash, baseline.cpu_hash)
        self.assertNotEqual(
            deterministic_machine_node_id(
                hardware_inputs=_hardware(
                    permanent_mac_addresses=("AA:BB:CC:DD:EE:FF", "77:88:99:AA:BB:CC")
                )
            ),
            deterministic_machine_node_id(hardware_inputs=_hardware()),
        )

    def test_changing_gpu_info_changes_gpu_hash_and_node_id(self) -> None:
        baseline = _component_hashes(_hardware())
        changed = _component_hashes(_hardware(gpu_model_or_chip_name="M4 Ultra GPU"))

        self.assertNotEqual(changed.gpu_hash, baseline.gpu_hash)
        self.assertEqual(changed.ram_hash, baseline.ram_hash)
        self.assertNotEqual(
            deterministic_machine_node_id(
                hardware_inputs=_hardware(gpu_model_or_chip_name="M4 Ultra GPU")
            ),
            deterministic_machine_node_id(hardware_inputs=_hardware()),
        )

    def test_blank_hardware_fields_use_explicit_placeholders(self) -> None:
        hashes = _component_hashes(
            _hardware(
                cpu_architecture="  arm64  ",
                cpu_vendor="   ",
                cpu_brand_or_chip_name=" Apple    M2   Pro ",
                physical_core_count=" 10 ",
                logical_thread_count="10",
                gpu_vendor=" Apple   (0x106b) ",
                gpu_model_or_chip_name=" Apple    M2   Pro ",
                gpu_device_id_if_available=" ",
                gpu_count=" 1 ",
                max_gpu_vram_gb="",
                total_installed_ram_gb=" 16.1 ",
            )
        )

        self.assertEqual(
            hashes.cpu_input,
            "ARM64|UNKNOWN_CPU_VENDOR|APPLE M2 PRO|10|10",
        )
        self.assertEqual(
            hashes.gpu_input,
            "APPLE (0X106B)|APPLE M2 PRO|UNKNOWN_GPU_DEVICE_ID|1|UNKNOWN_GPU_VRAM",
        )
        self.assertEqual(hashes.ram_input, "16")
        self.assertNotIn("||", hashes.cpu_input)
        self.assertNotIn("||", hashes.gpu_input)

    def test_mac_addresses_normalize_uppercase_separator_free_and_sorted(self) -> None:
        hashes = _component_hashes(
            _hardware(
                permanent_mac_addresses=(
                    " 20:a5:cb:c8:f1:d6 ",
                    "20-A5-CB-CD-15-72",
                    "",
                    "  ",
                )
            )
        )

        self.assertEqual(hashes.mac_input, "20A5CBC8F1D6|20A5CBCD1572")

    def test_changing_gpu_vram_changes_gpu_hash_and_node_id(self) -> None:
        baseline = _component_hashes(_hardware(max_gpu_vram_gb=16))
        changed = _component_hashes(_hardware(max_gpu_vram_gb=32))

        self.assertNotEqual(changed.gpu_hash, baseline.gpu_hash)
        self.assertEqual(changed.cpu_hash, baseline.cpu_hash)
        self.assertNotEqual(
            deterministic_machine_node_id(
                hardware_inputs=_hardware(max_gpu_vram_gb=32)
            ),
            deterministic_machine_node_id(
                hardware_inputs=_hardware(max_gpu_vram_gb=16)
            ),
        )

    def test_missing_gpu_uses_no_gpu_detected_hash(self) -> None:
        hashes = _component_hashes(
            _hardware(
                gpu_vendor="",
                gpu_model_or_chip_name="",
                gpu_device_id_if_available="",
                gpu_count=0,
                max_gpu_vram_gb="",
            )
        )

        self.assertEqual(hashes.gpu_input, "NO_GPU_DETECTED")
        self.assertEqual(hashes.gpu_hash, _sha("NO_GPU_DETECTED"))

    def test_changing_ram_changes_ram_hash_and_node_id(self) -> None:
        baseline = _component_hashes(_hardware())
        changed = _component_hashes(_hardware(total_installed_ram_gb=64.4))

        self.assertNotEqual(changed.ram_hash, baseline.ram_hash)
        self.assertNotEqual(
            deterministic_machine_node_id(
                hardware_inputs=_hardware(total_installed_ram_gb=64.4)
            ),
            deterministic_machine_node_id(hardware_inputs=_hardware()),
        )

    def test_changing_account_id_does_not_change_node_id(self) -> None:
        baseline = deterministic_machine_node_id(
            hardware_inputs=_hardware(), account_id="account-a"
        )
        changed = deterministic_machine_node_id(
            hardware_inputs=_hardware(), account_id="account-b"
        )

        self.assertEqual(changed, baseline)

    def test_generated_node_id_has_no_local_prefix(self) -> None:
        node_id = deterministic_machine_node_id(hardware_inputs=_hardware())

        self.assertFalse(node_id.startswith("local-"))
        self.assertFalse(node_id.startswith("local-hash"))

    def test_debug_output_prints_full_formula_inputs_and_hashes(self) -> None:
        output = StringIO()
        node_id = deterministic_machine_node_id(
            hardware_inputs=_hardware(), debug=True, output=output
        )

        debug_text = output.getvalue()
        self.assertIn("[DEBUG] CPU_INPUT = ARM64|APPLE|APPLE M4 MAX|16|16", debug_text)
        self.assertIn("[DEBUG] CPU_HASH = ", debug_text)
        self.assertIn("[DEBUG] MAC_INPUT = 112233445566|AABBCCDDEEFF", debug_text)
        self.assertIn("[DEBUG] MAC_HASH = ", debug_text)
        self.assertIn("[DEBUG] GPU_INPUT = APPLE|M4 MAX GPU|0X1234|1|64", debug_text)
        self.assertIn("[DEBUG] GPU_HASH = ", debug_text)
        self.assertIn("[DEBUG] RAM_INPUT = 128", debug_text)
        self.assertIn("[DEBUG] RAM_HASH = ", debug_text)
        self.assertIn("[DEBUG] ROOT_JSON = ", debug_text)
        self.assertIn(f"[DEBUG] NODE_ID = {node_id}", debug_text)
        self.assertIn("[DEBUG] NODE_NAME = ", debug_text)

    def test_hardware_inputs_are_collected_without_account_or_install_identifiers(
        self,
    ) -> None:
        commands: list[tuple[str, tuple[str, ...]]] = []

        def run_command(name: str, *args: str) -> str:
            commands.append((name, args))
            if name == "sysctl" and args == ("-n", "machdep.cpu.vendor"):
                return "Apple"
            if name == "sysctl" and args == ("-n", "machdep.cpu.brand_string"):
                return "Apple M4 Max"
            if name == "sysctl" and args == ("-n", "hw.physicalcpu"):
                return "16"
            if name == "sysctl" and args == ("-n", "hw.logicalcpu"):
                return "16"
            if name == "sysctl" and args == ("-n", "hw.memsize"):
                return str(128 * 1024**3)
            if name == "networksetup":
                return "Hardware Port: Wi-Fi\nEthernet Address: aa:bb:cc:dd:ee:ff"
            if name == "system_profiler":
                return "Chipset Model: Apple M4 Max\nVendor: Apple\nDevice ID: 0x1234\nVRAM (Total): 64 GB"
            raise OSError(name)

        node_id = deterministic_machine_node_id(
            goos="darwin",
            run_command=run_command,
            read_file=lambda path: (_ for _ in ()).throw(FileNotFoundError(path)),
            account_id="ignored-account",
        )

        self.assertRegex(node_id, r"^[0-9a-f]{64}$")
        flattened_commands = " ".join(
            " ".join((name, *args)) for name, args in commands
        )
        self.assertNotIn("machine-id", flattened_commands)
        self.assertNotIn("IOPlatformUUID", flattened_commands)
        self.assertNotIn("hostname", flattened_commands.lower())

    def test_physical_mac_filter_keeps_only_ethernet_and_wifi_hardware(self) -> None:
        darwin_networksetup = """
Hardware Port: Wi-Fi
Device: en0
Ethernet Address: aa:bb:cc:dd:ee:01

Hardware Port: Ethernet
Device: en7
Ethernet Address: aa:bb:cc:dd:ee:02

Hardware Port: Thunderbolt Bridge
Device: bridge0
Ethernet Address: aa:bb:cc:dd:ee:03

Hardware Port: Ethernet Adapter (en6)
Device: en6
Ethernet Address: aa:bb:cc:dd:ee:05

Hardware Port: USB 10/100/1000 LAN
Device: en10
Ethernet Address: aa:bb:cc:dd:ee:06

Hardware Port: Bluetooth PAN
Device: en8
Ethernet Address: aa:bb:cc:dd:ee:04
"""
        self.assertEqual(
            _physical_mac_addresses(darwin_networksetup, source="darwin-networksetup"),
            ["aa:bb:cc:dd:ee:01", "aa:bb:cc:dd:ee:02", "aa:bb:cc:dd:ee:06"],
        )

        linux_ip = (
            "2: eth0: <BROADCAST> link/ether aa:bb:cc:dd:ee:11 brd ff:ff:ff:ff:ff:ff\n"
            "3: wlan0: <BROADCAST> link/ether aa:bb:cc:dd:ee:12 brd ff:ff:ff:ff:ff:ff\n"
            "4: docker0: <BROADCAST> link/ether aa:bb:cc:dd:ee:13 brd ff:ff:ff:ff:ff:ff\n"
            "5: vethabc@if4: <BROADCAST> link/ether aa:bb:cc:dd:ee:14 brd ff:ff:ff:ff:ff:ff\n"
            "6: bridge0: <BROADCAST> link/ether aa:bb:cc:dd:ee:15 brd ff:ff:ff:ff:ff:ff"
        )
        self.assertEqual(
            _physical_mac_addresses(linux_ip, source="linux-ip-link"),
            ["aa:bb:cc:dd:ee:11", "aa:bb:cc:dd:ee:12"],
        )

        windows_getmac = (
            '"Connection Name","Network Adapter","Physical Address","Transport Name"\n'
            '"Ethernet","Intel Ethernet","AA-BB-CC-DD-EE-21","tcpip"\n'
            '"Wi-Fi","Intel Wi-Fi 6","AA-BB-CC-DD-EE-22","tcpip"\n'
            '"Bluetooth Network Connection","Bluetooth Device","AA-BB-CC-DD-EE-23","tcpip"\n'
            '"vEthernet (Default Switch)","Hyper-V Virtual Ethernet","AA-BB-CC-DD-EE-24","tcpip"\n'
        )
        self.assertEqual(
            _physical_mac_addresses(windows_getmac, source="windows-getmac"),
            ["AA-BB-CC-DD-EE-21", "AA-BB-CC-DD-EE-22"],
        )
        self.assertEqual(_physical_mac_addresses("", source="unknown-source"), [])
        self.assertEqual(
            _physical_mac_addresses(
                "Hardware Port: Wi-Fi\nEthernet Address: ff:ff:ff:ff:ff:ff\n",
                source="darwin-networksetup",
            ),
            [],
        )
        self.assertEqual(
            _physical_mac_addresses(
                "garbage without link fields", source="linux-ip-link"
            ),
            [],
        )
        self.assertEqual(
            _physical_mac_addresses(
                "4: docker0: <BROADCAST> link/ether aa:bb:cc:dd:ee:13 brd ff:ff:ff:ff:ff:ff",
                source="linux-ip-link",
            ),
            [],
        )

    def test_hardware_collectors_cover_linux_windows_and_unknown_os(self) -> None:
        linux_files = {
            "/proc/cpuinfo": "vendor_id: GenuineIntel\nmodel name: Xeon\n",
            "/proc/meminfo": "MemTotal: 67108864 kB\n",
        }

        def linux_command(name: str, *args: str) -> str:
            if name == "lscpu":
                return "CPU(s): 12\nCore(s) per socket: 6\nSocket(s): 1\n"
            if name == "lspci":
                return "00:02.0 VGA compatible controller [0300]: Intel Arc [8086:56a0]\n\tRegion 0: Memory at 00000000 (64-bit, prefetchable) [size=8G]"
            if name == "ip":
                return "2: eth0: <BROADCAST> link/ether aa:bb:cc:dd:ee:ff brd ff:ff:ff:ff:ff:ff"
            raise OSError(name)

        linux = collect_hardware_identity_inputs(
            goos="linux",
            read_file=lambda path: linux_files[path],
            run_command=linux_command,
        )
        self.assertEqual(linux.cpu_vendor, "GenuineIntel")
        self.assertEqual(linux.cpu_brand_or_chip_name, "Xeon")
        self.assertEqual(linux.physical_core_count, 6)
        self.assertEqual(linux.permanent_mac_addresses, ("aa:bb:cc:dd:ee:ff",))
        self.assertEqual(linux.gpu_vendor, "Intel")
        self.assertEqual(linux.gpu_device_id_if_available, "8086:56a0")
        self.assertEqual(linux.gpu_count, 1)
        self.assertEqual(linux.max_gpu_vram_gb, 8)
        self.assertEqual(linux.total_installed_ram_gb, 64)

        def windows_command(name: str, *args: str) -> str:
            command = " ".join((name, *args))
            if "cpu get" in command:
                return (
                    "Node,Manufacturer,Name,NumberOfCores,NumberOfLogicalProcessors\n"
                    "pc,AMD,Ryzen 9,12,24\n"
                )
            if "win32_VideoController" in command:
                return (
                    "Node,AdapterCompatibility,Name,PNPDeviceID,AdapterRAM\n"
                    "pc,NVIDIA,RTX 4090,PCI\\VEN_10DE&DEV_2684,25769803776\n"
                )
            if "memorychip" in command:
                return "Capacity\n34359738368\n34359738368\n"
            if name == "getmac":
                return '"Ethernet","AA-BB-CC-DD-EE-FF"\n'
            raise OSError(name)

        windows = collect_hardware_identity_inputs(
            goos="windows",
            read_file=lambda path: "unused",
            run_command=windows_command,
        )
        self.assertEqual(windows.cpu_vendor, "AMD")
        self.assertEqual(windows.cpu_brand_or_chip_name, "Ryzen 9")
        self.assertEqual(windows.physical_core_count, "12")
        self.assertEqual(windows.logical_thread_count, "24")
        self.assertEqual(windows.gpu_vendor, "NVIDIA")
        self.assertEqual(windows.gpu_model_or_chip_name, "RTX 4090")
        self.assertEqual(windows.gpu_count, 1)
        self.assertEqual(windows.max_gpu_vram_gb, 24)
        self.assertEqual(windows.total_installed_ram_gb, 64)

        unknown = collect_hardware_identity_inputs(
            goos="plan9",
            read_file=lambda path: "unused",
            run_command=lambda name, *args: "unused",
        )
        self.assertEqual(unknown.gpu_count, 0)
        self.assertEqual(unknown.permanent_mac_addresses, ())

    def test_hardware_parser_edge_cases_are_stable(self) -> None:
        self.assertEqual(_normalize_mac_address("00:00:00:00:00:00"), "")
        self.assertEqual(_normalize_mac_address("not-a-mac"), "")
        self.assertEqual(_extract_mac_addresses("ff:ff:ff:ff:ff:ff"), [])
        self.assertEqual(_round_installed_ram_gb("not-a-number"), 0)
        self.assertEqual(_bytes_to_gb(0), 0)
        self.assertEqual(_linux_memtotal_gb(""), 0)
        self.assertEqual(_colon_value("", "Missing"), "")
        self.assertEqual(_extract_labeled_value("", "Missing"), "")
        self.assertEqual(_max_gpu_vram_gb(""), 0)
        self.assertEqual(_max_gpu_vram_gb("VRAM (Total): 512 MB\nVRAM: 2 GB"), 2)
        self.assertEqual(_max_gpu_vram_gb("size=1048576 KB\nsize=1 TB"), 1024)
        self.assertEqual(_vram_value_to_gb("not-a-number", "GB"), 0)
        self.assertEqual(_vram_value_to_gb("2", "XB"), 2)
        self.assertEqual(_max_csv_int("", "AdapterRAM"), 0)
        self.assertEqual(_max_csv_int("Node,Name\npc,value", "AdapterRAM"), 0)
        self.assertEqual(_max_csv_int("Node,AdapterRAM\npc", "AdapterRAM"), 0)
        self.assertEqual(
            _max_csv_int("Node,AdapterRAM\npc,1024\npc,2048", "AdapterRAM"), 2048
        )
        self.assertEqual(_linux_gpu_model(""), "")
        self.assertEqual(_linux_gpu_vendor(""), "")
        self.assertEqual(_linux_gpu_device_id(""), "")
        self.assertEqual(
            _linux_gpu_device_id("00:02.0 VGA compatible controller: Intel Arc"),
            "",
        )
        self.assertEqual(
            _linux_gpu_model("00:02.0 VGA compatible controller: Intel Arc"),
            "Intel Arc",
        )
        self.assertEqual(_csv_first_value("", "Name"), "")
        self.assertEqual(_csv_first_value("Node,Name\npc,value", "Missing"), "")
        self.assertEqual(_csv_first_value("Node,Name\npc", "Name"), "")
        self.assertEqual(_safe_int("not-an-int"), 0)
        self.assertEqual(
            _linux_physical_core_count(
                "physical id: 0\ncore id: 0\nphysical id: 0\ncore id: 1\n", ""
            ),
            2,
        )
        self.assertEqual(
            _linux_physical_core_count("", "Core(s) per socket: 8\nSocket(s): 2\n"),
            16,
        )
        self.assertIsInstance(_linux_physical_core_count("", ""), int)
        with patch("aethermesh_core.identity.os.cpu_count", return_value=None):
            self.assertEqual(_physical_core_count_fallback(), 0)

    def test_existing_identity_file_reuses_node_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original_document = {
                "version": 1,
                "node": {
                    "node_id": "legacy-node-id",
                    "node_name": "legacy-node-name_legacy",
                    "creator_node_id": "original-creator-node",
                    "created_at": "2026-07-08T00:00:00+00:00",
                },
                "provenance": {
                    "created_by": "older-local-tool",
                    "source": "local-first-initialization",
                    "creation_event": "identity_manifest_created",
                    "load_behavior": "reuse_existing_identity_without_overwrite",
                    "authority": "local-only-no-network-consensus",
                },
                "references": {
                    "manifest_refs": ["manifests/local-batch.json#node:legacy-node-id"],
                    "validation_receipt_refs": ["receipts/receipt-0001.json"],
                    "version_metadata": _test_version_metadata(),
                },
                "lineage": {
                    "parent_node_ids": ["original-creator-node"],
                    "lineage_links": ["lineage/local-node-link.json"],
                },
                "contribution_attribution": {
                    "creator_node_id": "original-creator-node",
                    "attribution_node_id": "legacy-node-id",
                    "contribution_refs": ["contributions/contribution-0001.json"],
                },
            }
            identity_path.write_text(
                json.dumps(original_document),
                encoding="utf-8",
            )

            identity = load_or_create_identity(
                identity_path, hardware_inputs=_hardware()
            )
            reloaded_document = json.loads(identity_path.read_text(encoding="utf-8"))

        self.assertEqual(identity.node_id, "legacy-node-id")
        self.assertEqual(identity.node_name, "legacy-node-name_legacy")
        self.assertEqual(reloaded_document, original_document)

    def test_malformed_identity_json_names_file_path_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity_path = root / "local-node.json"
            manifest_path = root / "manifests" / "local-batch.json"
            manifest_path.parent.mkdir()
            manifest_path.write_text('{"kind":"manifest"}', encoding="utf-8")
            original_identity = '{"version": 1,'
            identity_path.write_text(original_identity, encoding="utf-8")

            with self.assertRaises(IdentityPersistenceError) as context:
                load_or_create_identity(identity_path)

            message = str(context.exception)
            self.assertIn(f"identity file {identity_path.name}", message)
            self.assertNotIn(str(identity_path.parent), message)
            self.assertIn("malformed", message)
            self.assertEqual(
                identity_path.read_text(encoding="utf-8"), original_identity
            )
            self.assertEqual(
                manifest_path.read_text(encoding="utf-8"), '{"kind":"manifest"}'
            )

    def test_invalid_identity_fields_name_file_and_do_not_modify_artifacts(
        self,
    ) -> None:
        invalid_documents = [
            ("missing creator", ("node", "creator_node_id"), "node.creator_node_id"),
            ("missing node", ("node", "node_id"), "node.node_id"),
            ("empty node", ("node", "node_id", ""), "node.node_id"),
            (
                "typed creator",
                ("node", "creator_node_id", ["creator"]),
                "node.creator_node_id",
            ),
            ("typed lineage", ("lineage", "lineage_links", "bad"), "lineage_links"),
        ]
        for _case_name, mutation, expected_field in invalid_documents:
            with (
                self.subTest(expected_field=expected_field),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                root = Path(temp_dir)
                identity_path = root / "local-node.json"
                artifact_contents = {
                    root / "manifests" / "local-batch.json": json.dumps(
                        {"nodes": [{"node_id": "legacy-node-id"}]}
                    ),
                    root / "receipts" / "receipt-0001.json": json.dumps(
                        {"receipts": [{"node_id": "legacy-node-id"}]}
                    ),
                    root / "lineage" / "local-node-link.json": json.dumps(
                        {"node_id": "legacy-node-id"}
                    ),
                    root / "contributions" / "contribution-0001.json": json.dumps(
                        {
                            "node_id": "legacy-node-id",
                            "creator_node_id": "original-creator-node",
                        }
                    ),
                }
                for artifact_path, contents in artifact_contents.items():
                    artifact_path.parent.mkdir(parents=True, exist_ok=True)
                    artifact_path.write_text(contents, encoding="utf-8")
                document = _identity_document(
                    NodeIdentity(node_id="legacy-node-id"),
                    created_at="2026-07-08T00:00:00+00:00",
                    creator_node_id="original-creator-node",
                )
                document["references"] = {
                    "manifest_refs": ["manifests/local-batch.json#node:legacy-node-id"],
                    "validation_receipt_refs": ["receipts/receipt-0001.json"],
                    "version_metadata": _test_version_metadata(),
                }
                document["lineage"] = {
                    "parent_node_ids": ["original-creator-node"],
                    "lineage_links": ["lineage/local-node-link.json"],
                }
                document["contribution_attribution"] = {
                    "creator_node_id": "original-creator-node",
                    "attribution_node_id": "legacy-node-id",
                    "contribution_refs": ["contributions/contribution-0001.json"],
                }
                section_name = mutation[0]
                field_name = mutation[1]
                section = document[section_name]
                assert isinstance(section, dict)
                if len(mutation) == 2:
                    section.pop(field_name)
                else:
                    section[field_name] = mutation[2]
                original_identity = json.dumps(document)
                identity_path.write_text(original_identity, encoding="utf-8")

                with self.assertRaises(IdentityPersistenceError) as context:
                    load_or_create_identity(identity_path)

                message = str(context.exception)
                self.assertIn(f"identity file {identity_path.name}", message)
                self.assertNotIn(str(identity_path.parent), message)
                self.assertIn(expected_field, message)
                self.assertEqual(
                    identity_path.read_text(encoding="utf-8"), original_identity
                )
                for artifact_path, contents in artifact_contents.items():
                    self.assertEqual(
                        artifact_path.read_text(encoding="utf-8"), contents
                    )

    def test_identity_validation_accepts_matching_local_references(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            identity_path = root / "local-node.json"
            (root / "manifests").mkdir()
            (root / "receipts").mkdir()
            (root / "lineage").mkdir()
            (root / "contributions").mkdir()
            (root / "manifests" / "local-batch.json").write_text(
                json.dumps({"nodes": [{"node_id": "legacy-node-id"}]}),
                encoding="utf-8",
            )
            (root / "receipts" / "receipt-0001.json").write_text(
                json.dumps({"receipts": [{"node_id": "legacy-node-id"}]}),
                encoding="utf-8",
            )
            (root / "lineage" / "local-node-link.json").write_text(
                json.dumps({"node_id": "legacy-node-id"}),
                encoding="utf-8",
            )
            (root / "contributions" / "contribution-0001.json").write_text(
                json.dumps(
                    {
                        "node_id": "legacy-node-id",
                        "creator_node_id": "original-creator-node",
                    }
                ),
                encoding="utf-8",
            )
            identity_path.write_text(
                json.dumps(
                    _identity_document(
                        NodeIdentity(
                            node_id="legacy-node-id",
                            node_name="legacy-node-name_legacy",
                        ),
                        created_at="2026-07-08T00:00:00+00:00",
                        creator_node_id="original-creator-node",
                    )
                    | {
                        "references": {
                            "manifest_refs": [
                                "manifests/local-batch.json#node:legacy-node-id"
                            ],
                            "validation_receipt_refs": ["receipts/receipt-0001.json"],
                            "version_metadata": _test_version_metadata(),
                        },
                        "lineage": {
                            "parent_node_ids": ["original-creator-node"],
                            "lineage_links": ["lineage/local-node-link.json"],
                        },
                        "contribution_attribution": {
                            "creator_node_id": "original-creator-node",
                            "attribution_node_id": "legacy-node-id",
                            "contribution_refs": [
                                "contributions/contribution-0001.json"
                            ],
                        },
                    }
                ),
                encoding="utf-8",
            )

            with self.assertLogs("aethermesh_core.identity", level="INFO") as logs:
                identity = load_or_create_identity(identity_path)

        self.assertEqual(identity.node_id, "legacy-node-id")
        self.assertTrue(
            any(
                "identity validation passed for local-node.json" in line
                for line in logs.output
            )
        )

    def test_identity_validation_rejects_mismatched_local_references_without_overwrite(
        self,
    ) -> None:
        mismatched_documents = [
            (
                "manifest_refs",
                {"nodes": [{"node_id": "other-node"}]},
                "references.manifest_refs",
            ),
            (
                "validation_receipt_refs",
                {"receipts": [{"node_id": "other-node"}]},
                "references.validation_receipt_refs",
            ),
            ("lineage_links", {"node_id": "other-node"}, "lineage.lineage_links"),
            (
                "contribution_refs",
                {"node_id": "legacy-node-id", "creator_node_id": "other-creator"},
                "contribution_attribution.contribution_refs",
            ),
        ]
        for ref_field, artifact, message in mismatched_documents:
            with (
                self.subTest(ref_field=ref_field),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                root = Path(temp_dir)
                identity_path = root / "local-node.json"
                artifact_path = root / "refs" / f"{ref_field}.json"
                artifact_path.parent.mkdir()
                artifact_path.write_text(json.dumps(artifact), encoding="utf-8")
                document = _identity_document(
                    NodeIdentity(node_id="legacy-node-id"),
                    created_at="2026-07-08T00:00:00+00:00",
                    creator_node_id="original-creator-node",
                )
                references = document["references"]
                lineage = document["lineage"]
                contribution_attribution = document["contribution_attribution"]
                assert isinstance(references, dict)
                assert isinstance(lineage, dict)
                assert isinstance(contribution_attribution, dict)
                if ref_field == "manifest_refs":
                    references["manifest_refs"] = [f"refs/{ref_field}.json"]
                elif ref_field == "validation_receipt_refs":
                    references["validation_receipt_refs"] = [f"refs/{ref_field}.json"]
                elif ref_field == "lineage_links":
                    lineage["lineage_links"] = [f"refs/{ref_field}.json"]
                else:
                    contribution_attribution["contribution_refs"] = [
                        f"refs/{ref_field}.json"
                    ]
                original = json.dumps(document)
                identity_path.write_text(original, encoding="utf-8")

                with self.assertRaisesRegex(IdentityPersistenceError, message):
                    load_or_create_identity(identity_path)

                self.assertEqual(identity_path.read_text(encoding="utf-8"), original)

    def test_identity_reference_metadata_match_helpers_cover_supported_shapes(
        self,
    ) -> None:
        self.assertFalse(
            identity_module._manifest_document_matches_node([], "legacy-node-id")
        )
        self.assertTrue(
            identity_module._manifest_document_matches_node(
                {"nodes": ["legacy-node-id"]}, "legacy-node-id"
            )
        )
        self.assertTrue(
            identity_module._manifest_document_matches_node(
                {"node": {"node_id": "legacy-node-id"}}, "legacy-node-id"
            )
        )
        self.assertFalse(
            identity_module._receipt_document_matches_node([], "legacy-node-id")
        )
        self.assertFalse(
            identity_module._receipt_document_matches_node(
                {"node_id": "other-node"}, "legacy-node-id"
            )
        )
        self.assertTrue(
            identity_module._identity_artifact_has_identity_metadata(
                [{"parent_node_ids": ["legacy-node-id"]}]
            )
        )
        self.assertFalse(
            identity_module._identity_artifact_has_identity_metadata(["not-metadata"])
        )
        self.assertTrue(
            identity_module._identity_artifact_mentions_node(
                {"nested": ["legacy-node-id"]}, "legacy-node-id"
            )
        )
        self.assertFalse(
            identity_module._identity_artifact_mentions_node(
                "other-node", "legacy-node-id"
            )
        )

    def test_identity_validation_rejects_malformed_references_before_load(self) -> None:
        for manifest_ref in ("../outside.json", "refs/local.json#bad fragment"):
            with (
                self.subTest(manifest_ref=manifest_ref),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                identity_path = Path(temp_dir) / "local-node.json"
                document = _identity_document(
                    NodeIdentity(node_id="legacy-node-id"),
                    created_at="2026-07-08T00:00:00+00:00",
                )
                references = document["references"]
                assert isinstance(references, dict)
                references["manifest_refs"] = [manifest_ref]
                original = json.dumps(document)
                identity_path.write_text(original, encoding="utf-8")

                with (
                    self.assertLogs(
                        "aethermesh_core.identity", level="WARNING"
                    ) as logs,
                    self.assertRaisesRegex(
                        IdentityPersistenceError, "malformed local reference"
                    ),
                ):
                    load_or_create_identity(identity_path)

                self.assertEqual(identity_path.read_text(encoding="utf-8"), original)
                self.assertTrue(
                    any(
                        "identity validation failed for local-node.json" in line
                        for line in logs.output
                    )
                )
                self.assertFalse(
                    any(str(identity_path.parent) in line for line in logs.output)
                )

    def test_malformed_attribution_critical_identity_data_fails_closed(
        self,
    ) -> None:
        case_names = (
            "missing creator node id",
            "invalid node id format",
            "corrupted manifest reference",
            "malformed validation receipt",
            "missing lineage fields",
            "missing contribution attribution",
        )
        for case_name in case_names:
            with (
                self.subTest(case_name=case_name),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                root = Path(temp_dir)
                identity_path = root / "local-node.json"
                document = _identity_document(
                    NodeIdentity(node_id="legacy-node-id"),
                    created_at="2026-07-08T00:00:00+00:00",
                    creator_node_id="original-creator-node",
                )
                referenced_artifacts: dict[str, str] = {}
                if case_name == "missing creator node id":
                    node = document["node"]
                    assert isinstance(node, dict)
                    node.pop("creator_node_id")
                    expected_message = "node.creator_node_id"
                elif case_name == "invalid node id format":
                    node = document["node"]
                    assert isinstance(node, dict)
                    node["node_id"] = "bad node id"
                    expected_message = "node.node_id"
                elif case_name == "corrupted manifest reference":
                    references = document["references"]
                    assert isinstance(references, dict)
                    references["manifest_refs"] = ["../private/manifest.json"]
                    expected_message = "references.manifest_refs"
                elif case_name == "malformed validation receipt":
                    references = document["references"]
                    assert isinstance(references, dict)
                    references["validation_receipt_refs"] = [
                        "receipts/receipt-0001.json"
                    ]
                    referenced_artifacts = {"receipts/receipt-0001.json": "{not-json"}
                    expected_message = "referenced local artifact is malformed"
                elif case_name == "missing lineage fields":
                    document["lineage"] = {}
                    expected_message = "parent_node_ids"
                else:
                    document.pop("contribution_attribution")
                    expected_message = "contribution_attribution"
                for relative_path, contents in referenced_artifacts.items():
                    artifact_path = root / relative_path
                    artifact_path.parent.mkdir(parents=True, exist_ok=True)
                    artifact_path.write_text(contents, encoding="utf-8")
                original_identity = json.dumps(document)
                identity_path.write_text(original_identity, encoding="utf-8")

                with self.assertRaises(IdentityPersistenceError) as context:
                    load_or_create_identity(
                        identity_path,
                        hardware_inputs=_hardware(),
                        node_id_factory=lambda: self.fail(
                            "malformed identity must not generate a replacement"
                        ),
                    )

                message = str(context.exception)
                self.assertIn(f"identity file {identity_path.name}", message)
                self.assertNotIn(str(identity_path.parent), message)
                self.assertIn(expected_message, message)
                self.assertEqual(
                    identity_path.read_text(encoding="utf-8"), original_identity
                )
                for relative_path, contents in referenced_artifacts.items():
                    self.assertEqual(
                        (root / relative_path).read_text(encoding="utf-8"),
                        contents,
                    )

    def test_identity_validation_rejects_invalid_node_id_format_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original = json.dumps(
                _identity_document(
                    NodeIdentity(node_id="bad node id"),
                    created_at="2026-07-08T00:00:00+00:00",
                )
            )
            identity_path.write_text(original, encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "node.node_id"):
                load_or_create_identity(identity_path)

            self.assertEqual(identity_path.read_text(encoding="utf-8"), original)

    def test_malformed_identity_json_raises_clear_error_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            identity_path.write_text("{not json", encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "malformed"):
                load_or_create_identity(identity_path)

            self.assertEqual(identity_path.read_text(encoding="utf-8"), "{not json")

    def test_unsupported_identity_version_raises_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original = json.dumps({"version": 2, "node": {"node_id": "future-node"}})
            identity_path.write_text(original, encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "version 1"):
                load_or_create_identity(identity_path)

            self.assertEqual(identity_path.read_text(encoding="utf-8"), original)

    def test_invalid_identity_node_id_raises_clear_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            identity_path.write_text(
                json.dumps({"version": 1, "node": {"node_id": ""}}),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(IdentityPersistenceError, "node.node_id"):
                load_or_create_identity(identity_path)

    def test_missing_creator_identity_data_raises_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original = json.dumps({"version": 1, "node": {"node_id": "legacy-node"}})
            identity_path.write_text(original, encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "creator_node_id"):
                load_or_create_identity(identity_path, hardware_inputs=_hardware())

            self.assertEqual(identity_path.read_text(encoding="utf-8"), original)

    def test_missing_identity_reference_data_raises_without_overwrite(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original = json.dumps(
                {
                    "version": 1,
                    "node": {
                        "node_id": "legacy-node",
                        "creator_node_id": "legacy-node",
                        "created_at": "2026-07-08T00:00:00+00:00",
                    },
                    "provenance": {
                        "created_by": "older-local-tool",
                        "source": "local-first-initialization",
                        "creation_event": "identity_manifest_created",
                        "load_behavior": "reuse_existing_identity_without_overwrite",
                        "authority": "local-only-no-network-consensus",
                    },
                }
            )
            identity_path.write_text(original, encoding="utf-8")

            with self.assertRaisesRegex(IdentityPersistenceError, "references"):
                load_or_create_identity(identity_path, hardware_inputs=_hardware())

            self.assertEqual(identity_path.read_text(encoding="utf-8"), original)

    def test_identity_receipt_reference_uses_stable_node_id(self) -> None:
        hardware = _hardware()
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            first_identity = load_or_create_identity(
                identity_path, hardware_inputs=hardware
            )
            second_identity = load_or_create_identity(
                identity_path, hardware_inputs=hardware
            )

        metadata = _test_version_metadata()
        metadata_ref = version_metadata_ref(metadata)
        receipt = {
            "version": 1,
            "run_source": "run-local-flow",
            "version_metadata": metadata,
            "version_metadata_ref": metadata_ref,
            "receipts": [
                {
                    "job_id": "job-1",
                    "job_type": "echo",
                    "node_id": second_identity.node_id,
                    "assignment_message_id": "msg-0001",
                    "correlation_id": "job-1",
                    "result_message_id": "msg-0002",
                    "validation_message_id": "msg-0003",
                    "contribution_message_id": "msg-0004",
                    "result_status": "completed",
                    "result_hash": "a" * 64,
                    "validation": {"valid": True, "reason": "ok"},
                    "version_metadata_ref": metadata_ref,
                    "credited_units": 1,
                    "output_summary": {"value": "hello"},
                }
            ],
        }

        with tempfile.TemporaryDirectory() as temp_dir:
            receipt_path = Path(temp_dir) / "receipts.json"
            write_receipt_document(receipt_path, receipt)
            validated_receipt = load_receipt_document_if_exists(receipt_path)

        self.assertEqual(first_identity.node_id, second_identity.node_id)
        self.assertEqual(validated_receipt, receipt)
        self.assertEqual(receipt["receipts"][0]["node_id"], first_identity.node_id)

    def test_identity_error_messages_are_stable(self) -> None:
        cases = [
            ([], "identity JSON must be an object"),
            (
                {"version": 2, "node": {"node_id": "future-node"}},
                "identity JSON must contain version 1",
            ),
            (
                {"version": 1, "node": []},
                "identity JSON field 'node' must be an object",
            ),
            (
                {"version": 1, "node": {"node_id": ""}},
                "identity JSON field 'node.node_id' must be a non-empty string",
            ),
        ]
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            for document, expected_message in cases:
                with self.subTest(expected_message=expected_message):
                    identity_path.write_text(json.dumps(document), encoding="utf-8")
                    with self.assertRaises(IdentityPersistenceError) as cm:
                        load_or_create_identity(identity_path)
                    message = str(cm.exception)
                    self.assertIn(f"identity file {identity_path.name}", message)
                    self.assertNotIn(str(identity_path.parent), message)
                    self.assertIn(expected_message, message)

    def test_helper_edges_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            value_path = Path(temp_dir) / "value.txt"
            value_path.write_text(" value\n", encoding="utf-8")

            self.assertEqual(_read_text_file(str(value_path)), "value")

        self.assertEqual(_read_or_empty(None, "/missing"), "")
        self.assertEqual(
            _read_or_empty(
                lambda path: (_ for _ in ()).throw(FileNotFoundError(path)),
                "/missing",
            ),
            "",
        )
        self.assertEqual(_run_or_empty(None, "ignored"), "")
        self.assertEqual(
            _run_or_empty(
                lambda name, *args: (_ for _ in ()).throw(
                    subprocess.CalledProcessError(1, name)
                ),
                "false",
            ),
            "",
        )

    def test_default_goos_maps_common_python_platforms(self) -> None:
        cases = [
            ("linux", "linux"),
            ("linux2", "linux"),
            ("darwin", "darwin"),
            ("win32", "windows"),
            ("freebsd13", "freebsd13"),
        ]
        for platform_name, expected in cases:
            with (
                self.subTest(platform_name=platform_name),
                patch("aethermesh_core.identity.sys.platform", platform_name),
            ):
                self.assertEqual(_default_goos(), expected)

    def test_default_command_runner_trims_stdout(self) -> None:
        self.assertEqual(
            _run_command(sys.executable, "-c", "print(' command-value ')"),
            "command-value",
        )

    def test_debug_output_defaults_to_stdout(self) -> None:
        output = StringIO()
        with redirect_stdout(output):
            deterministic_machine_node_id(hardware_inputs=_hardware(), debug=True)

        self.assertIn("[DEBUG] NODE_ID = ", output.getvalue())


if __name__ == "__main__":
    unittest.main()
