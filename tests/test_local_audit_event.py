import json
import tempfile
import unittest
from pathlib import Path

from aethermesh_core.local_audit_event import (
    AUDIT_EVENT_TYPES,
    AUDIT_REDACTED_VALUE,
    _SECRET_IN_TEXT,
    LocalAuditEventError,
    append_local_audit_event,
    sanitize_local_audit_event,
    validate_local_audit_event,
)


class LocalAuditEventTests(unittest.TestCase):
    def test_secret_redaction_prevents_separator_whitespace_backtracking(self) -> None:
        self.assertEqual(_SECRET_IN_TEXT.pattern.count(r"\s*+"), 2)

    def test_sanitizes_secret_values_in_unstructured_audit_context(self) -> None:
        secrets = {
            "api_key": "api-live-" + "9Fj2qL",
            "private_token": "token-live-4Md8pQ",
            "password": "password-live-6Hs3wK",
            "bearer_token": "bearer-live-" + "1Rt7xV",
            "basic_credential": "dXNlci1maXh0dXJl" + "OnBhc3N3b3Jk",
            "seed_phrase": "ember river cloud lantern meadow",
            "manifest_secret": "manifest-secret-5Np2cZ",
        }
        event = {
            **_referenced_event(),
            "api_key": secrets["api_key"],
            "hashes": {
                "result_hash": "sha256:example",
                "metadata": {"private_token": secrets["private_token"]},
                "stringified_payload": (
                    '{"authorization":"Bearer ' + secrets["bearer_token"] + '"}'
                ),
            },
            "lineage_parent_ids": [
                "work-parent-001",
                {"seed_phrase": secrets["seed_phrase"]},
            ],
            "signatures": {"password": secrets["password"]},
            "error_summary": (
                "submission failed: Bearer "
                + secrets["bearer_token"]
                + "; upstream rejected Basic "
                + secrets["basic_credential"]
                + "; api_key="
                + secrets["api_key"]
                + "; seed phrase="
                + secrets["seed_phrase"]
                + '; manifest_secret="'
                + secrets["manifest_secret"]
                + '"'
            ),
        }

        sanitized = sanitize_local_audit_event(event)
        rendered = json.dumps(sanitized, sort_keys=True)

        _assert_secret_values_absent(self, rendered, secrets)
        self.assertEqual(sanitized["api_key"], AUDIT_REDACTED_VALUE)
        self.assertEqual(
            sanitized["hashes"]["metadata"]["private_token"], AUDIT_REDACTED_VALUE
        )
        self.assertEqual(
            sanitized["lineage_parent_ids"][1]["seed_phrase"], AUDIT_REDACTED_VALUE
        )
        self.assertEqual(sanitized["signatures"]["password"], AUDIT_REDACTED_VALUE)
        self.assertIn(AUDIT_REDACTED_VALUE, sanitized["error_summary"])

    def test_sanitizes_secrets_private_content_and_absolute_paths(self) -> None:
        event = {
            **_referenced_event(),
            "related_file_paths": ["/Users/private/node/manifests/work-001.json"],
            "hashes": {
                "result_hash": "sha256:example",
                "apiKey": "private-api-key",
                "service_api_key": "private-service-api-key",
                "access_token": "private-token",
                "environment": "HOME=/Users/private",
                "environment_variables": "HOME=/Users/private",
            },
            "signatures": {
                "privateKey": "private-key",
                "signingPrivateKey": "private-signing-key",
                "authorization": "Bearer private-token",
                "credential": "private-credential",
                "password": "private-password",
                "secret": "private-secret",
                "seed": "private-seed",
            },
        }

        sanitized = sanitize_local_audit_event(event)

        self.assertEqual(sanitized["related_file_paths"], ["local-path/work-001.json"])
        self.assertEqual(sanitized["hashes"]["result_hash"], "sha256:example")
        for field in (
            "apiKey",
            "service_api_key",
            "access_token",
            "environment",
            "environment_variables",
        ):
            self.assertEqual(sanitized["hashes"][field], AUDIT_REDACTED_VALUE)
        for value in sanitized["signatures"].values():
            self.assertEqual(value, AUDIT_REDACTED_VALUE)

    def test_appended_sanitized_event_keeps_audit_attribution(self) -> None:
        event = {
            **_referenced_event(),
            "related_file_paths": ["/home/private/aethermesh/work-001.json"],
            "signatures": {"token": "must-not-be-written"},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            written = append_local_audit_event(path, event)
            output = path.read_text(encoding="utf-8")

        self.assertNotIn("must-not-be-written", output)
        self.assertNotIn("/home/private", output)
        self.assertEqual(written["creator_node_id"], "creator-local-a")
        self.assertEqual(written["manifest_id"], "manifest-work-001")
        self.assertEqual(written["validation_receipt_id"], "receipt-work-001")
        self.assertEqual(written["lineage_parent_ids"], ["work-parent-001"])
        self.assertEqual(
            written["contribution_attribution_ids"],
            ["local-contribution-work-001"],
        )

    def test_persisted_event_redacts_error_summary_without_losing_evidence(
        self,
    ) -> None:
        secrets = {
            "api_key": "api-live-" + "7Bq3mN",
            "private_token": "token-live-2Kp8sD",
            "password": "password-live-5Wc1rT",
            "bearer_token": "bearer-live-" + "8Zv4hJ",
            "seed_phrase": "orchid maple silver comet harbor",
            "manifest_secret": "manifest-secret-6Xa9fL",
        }
        event = {
            **_referenced_event(),
            "hashes": {"manifest_digest": "sha256:manifest-safe"},
            "signatures": {"private_token": secrets["private_token"]},
            "error_summary": (
                "validation failed: Bearer "
                + secrets["bearer_token"]
                + "; api_key="
                + secrets["api_key"]
                + "; password="
                + secrets["password"]
                + '; seed phrase="'
                + secrets["seed_phrase"]
                + '"; manifest_secret="'
                + secrets["manifest_secret"]
                + '"'
            ),
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "events.jsonl"
            append_local_audit_event(path, event)
            persisted = path.read_text(encoding="utf-8")

        _assert_secret_values_absent(self, persisted, secrets)
        parsed = json.loads(persisted)
        self.assertIn(AUDIT_REDACTED_VALUE, persisted)
        self.assertEqual(parsed["creator_node_id"], "creator-local-a")
        self.assertEqual(parsed["manifest_id"], "manifest-work-001")
        self.assertEqual(parsed["validation_receipt_id"], "receipt-work-001")
        self.assertEqual(parsed["lineage_parent_ids"], ["work-parent-001"])
        self.assertEqual(
            parsed["contribution_attribution_ids"],
            ["local-contribution-work-001"],
        )

    def test_sanitizes_embedded_local_path_in_short_summary(self) -> None:
        event = {
            **_referenced_event(),
            "error_summary": (
                "could not read path:/Users/private/node/key.pem or "
                "file:///home/private/node/backup.pem"
            ),
        }

        sanitized = sanitize_local_audit_event(event)

        self.assertEqual(
            sanitized["error_summary"],
            "could not read path:local-path/key.pem or local-path/backup.pem",
        )

    def test_validates_required_fields_and_allows_missing_optional_fields(self) -> None:
        event = _minimal_event()

        self.assertEqual(validate_local_audit_event(event), event)
        self.assertEqual(len(AUDIT_EVENT_TYPES), 14)
        self.assertIsNone(event["creator_node_id"])

    def test_appends_parseable_jsonl_events_with_local_references(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            path = Path(temp_dir) / "audit" / "events.jsonl"
            first = append_local_audit_event(path, _minimal_event())
            second = append_local_audit_event(path, _referenced_event())
            parsed = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
            ]

        self.assertEqual(parsed, [first, second])
        self.assertEqual(second["manifest_id"], "manifest-work-001")
        self.assertEqual(second["validation_receipt_id"], "receipt-work-001")
        self.assertEqual(second["lineage_parent_ids"], ["work-parent-001"])
        self.assertEqual(
            second["contribution_attribution_ids"], ["local-contribution-work-001"]
        )

    def test_rejects_invalid_required_fields_and_unknown_event_type(self) -> None:
        cases = [
            (
                {
                    key: value
                    for key, value in _minimal_event().items()
                    if key != "event_id"
                },
                "missing required",
            ),
            ({**_minimal_event(), "schema_version": 1}, "schema_version"),
            ({**_minimal_event(), "schema_version": True}, "schema_version"),
            ({**_minimal_event(), "timestamp": "not-utc"}, "timestamp"),
            ({**_minimal_event(), "event_sequence": 0}, "event_sequence"),
            ({**_minimal_event(), "event_sequence": True}, "event_sequence"),
            ({**_minimal_event(), "event_type": "unknown"}, "event_type"),
            ({**_minimal_event(), "event_type": []}, "event_type"),
            ({**_minimal_event(), "creator_node_id": ""}, "creator_node_id"),
            ({**_minimal_event(), "unexpected": True}, "unsupported fields"),
            ({**_minimal_event(), 1: "invalid"}, "field names must be strings"),
        ]
        for event, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(LocalAuditEventError, message),
            ):
                validate_local_audit_event(event)

    def test_rejects_invalid_optional_references(self) -> None:
        cases = [
            ({"manifest_id": ""}, "manifest_id"),
            ({"lineage_parent_ids": [""]}, "lineage_parent_ids"),
            ({"contribution_attribution_ids": "bad"}, "contribution_attribution_ids"),
            ({"related_file_paths": ["../outside.json"]}, "safe relative"),
            ({"related_file_paths": ["C:\\outside.json"]}, "safe relative"),
            ({"related_file_paths": ["https://example.test/a"]}, "safe relative"),
            ({"manifest_ref": "/private/node.json"}, "safe relative"),
            ({"validation_receipt_ref": "../receipt.json"}, "safe relative"),
            ({"lineage_refs": ["~/lineage.json"]}, "safe relative"),
            (
                {"contribution_attribution_refs": ["C:\\contribution.json"]},
                "safe relative",
            ),
            ({"hashes": {"": "sha256:value"}}, "hashes"),
            ({"signatures": {"receipt": ""}}, "signatures"),
        ]
        for optional_fields, message in cases:
            with (
                self.subTest(message=message),
                self.assertRaisesRegex(LocalAuditEventError, message),
            ):
                validate_local_audit_event({**_minimal_event(), **optional_fields})

    def test_contribution_update_requires_a_supported_outcome(self) -> None:
        event = {
            **_minimal_event(),
            "event_type": "contribution_record_updated",
            "validation_status": "recorded",
        }
        self.assertEqual(validate_local_audit_event(event), event)
        with self.assertRaisesRegex(LocalAuditEventError, "missing validation_status"):
            validate_local_audit_event(
                {
                    key: value
                    for key, value in event.items()
                    if key != "validation_status"
                }
            )
        with self.assertRaisesRegex(LocalAuditEventError, "unsupported"):
            validate_local_audit_event({**event, "validation_status": "rewarded"})

    def test_job_submitted_requires_compact_submission_evidence(self) -> None:
        event = {
            **_minimal_event(),
            "event_type": "job_submitted",
            "actor_node_id": "local-node-a",
            "creator_node_id": "creator-node-a",
            "job_id": "local-job-a",
            "local_node_id": "local-node-a",
            "manifest_ref": "data/job-submissions/local-job-a.json",
            "hashes": {"manifest_hash": "sha256:manifest"},
            "lineage_refs": ["data/lineage/parent.json"],
            "validation_expectation": "deterministic-local",
            "contribution_attribution": {
                "job_id": "local-job-a",
                "creator_node_id": "creator-node-a",
                "metadata_hash": "sha256:metadata",
            },
            "attribution_metadata_hash": "sha256:metadata",
        }

        self.assertEqual(validate_local_audit_event(event), event)
        with self.assertRaisesRegex(LocalAuditEventError, "local_node_id"):
            validate_local_audit_event({**event, "local_node_id": "other-node"})

    def test_execution_started_requires_compact_execution_provenance(self) -> None:
        event = {
            **_minimal_event(),
            "event_type": "job.execution.started",
            "actor_node_id": "worker-node-a",
            "creator_node_id": "creator-node-a",
            "job_id": "local-job-a",
            "executing_node_id": "worker-node-a",
            "manifest_id": "local-job-a",
            "manifest_ref": "data/job-submissions/local-job-a.json",
            "hashes": {
                "manifest_hash": "sha256:manifest",
                "input_payload_hash": "sha256:input",
            },
            "lineage_refs": ["data/lineage/parent.json"],
            "contribution_attribution": {
                "job_id": "local-job-a",
                "creator_node_id": "creator-node-a",
                "executing_node_id": "worker-node-a",
                "metadata_hash": "sha256:metadata",
            },
            "attribution_metadata_hash": "sha256:metadata",
        }

        self.assertEqual(validate_local_audit_event(event), event)
        with self.assertRaisesRegex(LocalAuditEventError, "manifest_id"):
            validate_local_audit_event(
                {key: value for key, value in event.items() if key != "manifest_id"}
            )
        with self.assertRaisesRegex(LocalAuditEventError, "must match actor_node_id"):
            validate_local_audit_event({**event, "executing_node_id": "other-node"})


def _minimal_event() -> dict[str, object]:
    return {
        "schema_version": 2,
        "event_id": "audit-init-001",
        "timestamp": "2026-07-15T06:00:00Z",
        "event_type": "node_initialized",
        "actor_node_id": "local-node-a",
        "creator_node_id": None,
        "local_run_id": "run-001",
        "event_sequence": 1,
    }


def _referenced_event() -> dict[str, object]:
    return {
        **_minimal_event(),
        "event_id": "audit-validation-001",
        "event_type": "validation_result",
        "actor_node_id": "validator-local-a",
        "creator_node_id": "creator-local-a",
        "manifest_id": "manifest-work-001",
        "work_id": "work-001",
        "validation_receipt_id": "receipt-work-001",
        "lineage_parent_ids": ["work-parent-001"],
        "contribution_attribution_ids": ["local-contribution-work-001"],
        "related_file_paths": ["data/manifests/work-001.json"],
        "hashes": {"result_hash": "sha256:example"},
        "signatures": {"receipt_signature": "existing-local-signature"},
    }


def _assert_secret_values_absent(
    test_case: unittest.TestCase, rendered: str, secrets: dict[str, str]
) -> None:
    for label, secret in secrets.items():
        if secret in rendered:
            test_case.fail(f"{label} leaked into audit output")
