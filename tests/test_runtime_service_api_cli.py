import asyncio
import builtins
import json
import os
import sys
import tempfile
import time
import types
import unittest
from copy import deepcopy
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx
from fastapi import FastAPI
from typer.testing import CliRunner

from aethermesh_core import app_cli
from aethermesh_core.api import _lifespan, create_app
from aethermesh_core.identity import deterministic_machine_node_id
from aethermesh_core.job_result_schema import validate_job_result_document
from aethermesh_core.local_audit_event import (
    append_local_audit_event as real_append_audit_event,
)
from aethermesh_core.local_json_helpers import canonical_json_hash
from aethermesh_core.models import JobResult
from aethermesh_core.release_update import ReleaseUpdateError
from aethermesh_core.result_hash import canonical_result_document_hash
from aethermesh_core.runner import LocalRunner, run_local_job
from aethermesh_core.runtime_service import (
    NodeRuntimeService,
    RuntimeServiceError,
    _config_api_host,
    _config_api_port,
    _config_capability_resource_hints,
    _config_enabled_work_types,
    _config_identity_path,
    _config_identity_persistence_enabled,
    _config_node_id,
    _config_node_name,
    _default_home,
    _duration_ms,
    _is_utc_timestamp,
    _is_utc_timestamp_before_or_at,
    _is_utc_timestamp_before_or_at_timestamp,
    _memory_total_bytes,
    _merge_config,
    _output_payload_record,
    _package_version,
    _pid_is_alive,
    _result_summary,
    _validate_stored_output_payload,
)


async def _fetch_api_payloads(api_app: FastAPI) -> dict[str, Any]:
    transport = httpx.ASGITransport(app=api_app)
    async with httpx.AsyncClient(
        transport=transport, base_url="http://testserver"
    ) as client:
        return {
            "health": (await client.get("/health")).json(),
            "status": (await client.get("/api/status")).json(),
            "status_alias": (await client.get("/status")).json(),
            "version_alias": (await client.get("/version")).json(),
            "node": (await client.get("/api/node")).json(),
            "node_alias": (await client.get("/node")).json(),
            "peers": (await client.get("/api/peers")).json(),
            "peers_alias": (await client.get("/peers")).json(),
            "jobs": (await client.get("/api/jobs")).json(),
            "capabilities": (await client.get("/api/capabilities")).json(),
            "capabilities_alias": (await client.get("/capabilities")).json(),
            "package": (await client.get("/api/package")).json(),
            "network": (await client.get("/api/network")).json(),
            "logs": (await client.get("/api/logs")).json(),
            "logs_alias": (await client.get("/logs")).json(),
            "events": (await client.get("/api/events")).json(),
            "shutdown": (await client.post("/shutdown")).json(),
            "restart": (await client.post("/restart")).json(),
            "root": (await client.get("/")).json(),
        }


def _valid_local_work_fixture() -> tuple[dict[str, Any], dict[str, Any]]:
    """Return a stable, valid local work request and its deterministic output."""

    return (
        {
            "schema_version": 1,
            "job_id": "local-job-0123456789abcdef0123456789abcdef",
            "job_type": "echo",
            "requested_capability": {"identifier": "work.echo"},
            "input_payload": {
                "payload_type": "json",
                "content": {"message": "valid local work"},
            },
            "creator_node_id": "creator-local-fixture",
            "requested_validation_mode": "deterministic-local",
            "lineage_parent_refs": ["data/lineage/fixture-parent.json"],
            "attribution_metadata": {"fixture": "valid-local-work"},
        },
        {"output": "valid local work"},
    )


class RuntimeServiceTests(unittest.TestCase):
    def test_deterministic_local_work_repeats_with_stable_provenance(self) -> None:
        request, expected_output = _valid_local_work_fixture()
        request_before_execution = deepcopy(request)

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = self._execute_fixed_deterministic_fixture(root / "first", request)
            second = self._execute_fixed_deterministic_fixture(root / "second", request)

        self.assertEqual(first["manifest"], second["manifest"])
        self.assertEqual(first["result"], second["result"])
        self.assertRegex(first["receipt"]["validated_at"], r"Z$")
        self.assertRegex(second["receipt"]["validated_at"], r"Z$")
        self.assertEqual(
            {
                key: value
                for key, value in first["receipt"].items()
                if key != "validated_at"
            },
            {
                key: value
                for key, value in second["receipt"].items()
                if key != "validated_at"
            },
        )
        self.assertEqual(
            {
                key: value
                for key, value in first["receipt_document"].items()
                if key != "validated_at"
            },
            {
                key: value
                for key, value in second["receipt_document"].items()
                if key != "validated_at"
            },
        )
        self.assertEqual(first["completed"], second["completed"])
        self.assertEqual(first["result"]["summary"], expected_output["output"])
        self.assertEqual(
            canonical_result_document_hash(first["result"]),
            canonical_result_document_hash(second["result"]),
        )
        self.assertEqual(
            first["result"]["result_hash"],
            canonical_result_document_hash(first["result"]),
        )
        self.assertEqual(
            first["receipt_document"]["result_hash"],
            first["result"]["result_hash"],
        )
        self.assertEqual(
            first["receipt"]["result_hash"], first["result"]["result_hash"]
        )
        self.assertEqual(first["result"]["creator_node_id"], request["creator_node_id"])
        self.assertEqual(
            first["completed"]["contribution_attribution"],
            {
                "job_id": request["job_id"],
                "creator_node_id": request["creator_node_id"],
                "metadata": request["attribution_metadata"],
                "worker_node_id": "worker-local-fixture",
                "executor_node_id": "worker-local-fixture",
                "validated_contribution_units": 1,
            },
        )
        self.assertEqual(
            first["receipt"]["manifest_ref"], first["submission"]["manifest_ref"]
        )
        self.assertEqual(
            first["receipt"]["lineage_parent_ids"], request["lineage_parent_refs"]
        )
        self.assertEqual(
            first["receipt"]["contribution_attribution"],
            first["completed"]["contribution_attribution"],
        )
        self.assertEqual(request, request_before_execution)

    def test_deterministic_local_work_input_change_changes_result_hash(self) -> None:
        request, _expected_output = _valid_local_work_fixture()
        changed_request = deepcopy(request)
        changed_request["input_payload"]["content"]["message"] = "changed local work"

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            original = self._execute_fixed_deterministic_fixture(
                root / "original", request
            )
            changed = self._execute_fixed_deterministic_fixture(
                root / "changed", changed_request
            )

        self.assertNotEqual(original["result"]["summary"], changed["result"]["summary"])
        self.assertNotEqual(
            canonical_result_document_hash(original["result"]),
            canonical_result_document_hash(changed["result"]),
        )
        self.assertNotEqual(
            original["receipt_document"]["output_hash"],
            changed["receipt_document"]["output_hash"],
        )

    def _execute_fixed_deterministic_fixture(
        self, root: Path, request: dict[str, Any]
    ) -> dict[str, Any]:
        with (
            patch(
                "aethermesh_core.runtime_service.time.time", return_value=1_720_000_000
            ),
            patch(
                "aethermesh_core.runtime_service._utc_timestamp",
                side_effect=[
                    "2024-07-03T09:46:40.000000Z",
                    "2024-07-03T09:46:40.000000Z",
                    "2024-07-03T09:46:40.000000Z",
                ],
            ),
        ):
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            completed = service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )

        return {
            "submission": submission,
            "completed": completed,
            "manifest": json.loads((root / submission["manifest_ref"]).read_text()),
            "result": json.loads((root / completed["result"]["ref"]).read_text()),
            "receipt_document": json.loads(
                (
                    root
                    / "data"
                    / "job-validation-receipts"
                    / f"{submission['job_id']}.json"
                ).read_text()
            ),
            "receipt": service.get_local_validation_receipt(
                work_id=submission["job_id"]
            ),
        }

    def test_valid_local_work_fixture_completes_with_receipted_provenance(self) -> None:
        request, expected_output = _valid_local_work_fixture()
        request_before_execution = json.loads(json.dumps(request))

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            manifest_path = root / submission["manifest_ref"]
            manifest_before_execution = manifest_path.read_bytes()

            completed = service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            result = json.loads(
                (root / completed["result"]["ref"]).read_text(encoding="utf-8")
            )
            receipt = service.get_local_validation_receipt(work_id=submission["job_id"])

            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(completed["result"]["summary"], expected_output["output"])
            self.assertEqual(result["status"], "succeeded")
            self.assertEqual(result["summary"], expected_output["output"])
            self.assertEqual(
                result["output_payload"],
                {
                    "inline_payload": expected_output["output"],
                    "payload_ref": None,
                    "payload_digest": None,
                },
            )
            self.assertTrue(completed["validation"]["passed"])
            self.assertEqual(receipt["status"], "accepted")
            self.assertIsNone(receipt["rejection_reason"])
            self.assertEqual(
                receipt["next_local_action"],
                "retain this local receipt, manifest, and result artifact for replay",
            )
            self.assertEqual(receipt["validation_status"], "passed")
            self.assertTrue(receipt["validation"]["valid"])
            self.assertEqual(
                receipt["validator_software"]["validator_name"],
                "deterministic_local_result_check",
            )
            self.assertEqual(receipt["validator_software"]["receipt_schema_version"], 6)
            self.assertEqual(
                receipt["validation_method"],
                {
                    "kind": "deterministic_local_result_check",
                    "description": (
                        "Ran the deterministic local echo validator against the assigned "
                        "job and executor result. The validator checks completion, work "
                        "identity, contribution units, payload validity, and expected "
                        "output in order; outcome: ok."
                    ),
                    "manifest_ref": submission["manifest_ref"],
                    "creator_node_id": request["creator_node_id"],
                    "work_id": submission["job_id"],
                    "lineage_parent_refs": request["lineage_parent_refs"],
                    "contribution_attribution": completed["contribution_attribution"],
                },
            )
            self.assertEqual(receipt["job_id"], submission["job_id"])
            self.assertEqual(receipt["work_id"], submission["job_id"])
            self.assertEqual(result["job_id"], request["job_id"])
            self.assertEqual(result["capability"], "work.echo")
            self.assertEqual(
                result["manifest_id"],
                canonical_json_hash(
                    json.loads(manifest_before_execution), prefix="sha256:"
                ),
            )
            self.assertEqual(result["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                result["lineage"]["parent_job_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                completed["contribution_attribution"],
                {
                    "job_id": request["job_id"],
                    "creator_node_id": request["creator_node_id"],
                    "metadata": request["attribution_metadata"],
                    "worker_node_id": "worker-local-fixture",
                    "executor_node_id": "worker-local-fixture",
                    "validated_contribution_units": 1,
                },
            )
            self.assertEqual(receipt["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                receipt["contribution_attribution"]["job_id"], receipt["job_id"]
            )
            self.assertEqual(
                service.get_local_validation_receipt(work_id=submission["job_id"])[
                    "validation_receipt_id"
                ],
                receipt["validation_receipt_id"],
            )
            self.assertEqual(receipt["validation_receipt_id"], receipt["receipt_id"])
            self.assertEqual(receipt["capability"], result["capability"])
            self.assertEqual(
                receipt["lineage_parent_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                receipt["contribution_attribution"],
                completed["contribution_attribution"],
            )
            self.assertEqual(manifest_path.read_bytes(), manifest_before_execution)
            self.assertEqual(request, request_before_execution)

    def test_successful_output_payload_validation_rejects_missing_data(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            missing_payload = {
                "inline_payload": None,
                "payload_ref": None,
                "payload_digest": None,
            }
            with self.assertRaisesRegex(
                RuntimeServiceError, "no output payload evidence"
            ):
                _validate_stored_output_payload(
                    missing_payload,
                    root=Path(temp_dir),
                    job_id="local-job-0123456789abcdef0123456789abcdef",
                    expected_output_hash="sha256:" + "0" * 64,
                    require_payload=True,
                )
            _validate_stored_output_payload(
                missing_payload,
                root=Path(temp_dir),
                job_id="local-job-0123456789abcdef0123456789abcdef",
                expected_output_hash="sha256:" + "0" * 64,
                require_payload=False,
            )

    def test_output_payload_rejects_non_json_output(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            self.assertRaisesRegex(RuntimeServiceError, "JSON-compatible"),
        ):
            _output_payload_record(
                {"unsupported": object()},
                job_id="local-job-0123456789abcdef0123456789abcdef",
                root=Path(temp_dir),
                store=True,
            )

    def test_large_local_output_uses_retrievable_digest_addressed_payload_artifact(
        self,
    ) -> None:
        request, _ = _valid_local_work_fixture()
        request["input_payload"]["content"]["message"] = "x" * 4097
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            completed = service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            result = json.loads(
                (root / completed["result"]["ref"]).read_text(encoding="utf-8")
            )
            output_payload = result["output_payload"]
            expected_ref = f"data/job-output-payloads/{submission['job_id']}.json"
            self.assertIsNone(output_payload["inline_payload"])
            self.assertEqual(output_payload["payload_ref"], expected_ref)
            self.assertRegex(output_payload["payload_digest"], r"^sha256:[0-9a-f]{64}$")
            payload_artifact_path = root / expected_ref
            payload_artifact = json.loads(
                payload_artifact_path.read_text(encoding="utf-8")
            )
            self.assertEqual(payload_artifact, {"payload": "x" * 4097})
            self.assertEqual(
                output_payload["payload_digest"],
                canonical_json_hash(payload_artifact, prefix="sha256:"),
            )
            receipt = service.get_local_validation_receipt(work_id=submission["job_id"])
            self.assertEqual(receipt["manifest_ref"], submission["manifest_ref"])
            self.assertEqual(
                receipt["lineage_parent_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                receipt["contribution_attribution"],
                completed["contribution_attribution"],
            )
            payload_artifact_path.unlink()
            with self.assertRaisesRegex(RuntimeServiceError, "not locally retrievable"):
                service.get_local_validation_receipt(work_id=submission["job_id"])

    def test_local_validation_receipt_rejects_missing_capability(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt.pop("capability")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError,
                "capability does not match manifest result",
            ):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_local_validation_receipt_rejects_missing_validation_method(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt.pop("validation_method")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError, "has no validation method"
            ):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_local_validation_receipt_rejects_missing_next_local_action(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt.pop("next_local_action")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError, "has no next local action"
            ):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_local_validation_receipt_rejects_missing_validator_software(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt.pop("validator_software")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError, "invalid validator software metadata"
            ):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_local_validation_receipt_rejects_old_stored_version(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["version"] = 4
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(RuntimeServiceError, "unsupported version"):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_local_validation_receipt_rejects_missing_receipt_id(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt.pop("validation_receipt_id")
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            with self.assertRaisesRegex(RuntimeServiceError, "invalid receipt ID"):
                NodeRuntimeService.from_home(root).get_local_validation_receipt(
                    work_id=evidence["submission"]["job_id"]
                )

    def test_validation_receipt_ids_are_stable_unique_and_traceable(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            first = self._execute_fixed_deterministic_fixture(root, request)
            service = NodeRuntimeService.from_home(root)
            second_request = json.loads(json.dumps(request))
            second_request["job_id"] = "local-job-11111111111111111111111111111111"
            second_request["input_payload"]["content"]["message"] = "second receipt"
            second = {"submission": service.submit_local_job(second_request)}
            service.execute_submitted_local_job(
                second["submission"]["job_id"], "worker-local-fixture"
            )
            first_receipt = service.get_local_validation_receipt(
                work_id=first["submission"]["job_id"]
            )
            reread_first = service.get_local_validation_receipt(
                receipt_id=first_receipt["validation_receipt_id"]
            )
            second_receipt = service.get_local_validation_receipt(
                work_id=second["submission"]["job_id"]
            )

            self.assertEqual(
                reread_first["validation_receipt_id"],
                first_receipt["validation_receipt_id"],
            )
            self.assertNotEqual(
                first_receipt["validation_receipt_id"],
                second_receipt["validation_receipt_id"],
            )
            for receipt in (first_receipt, second_receipt):
                self.assertEqual(
                    receipt["validation_receipt_id"], receipt["receipt_id"]
                )
                self.assertTrue(receipt["creator_node_id"])
                self.assertTrue(receipt["manifest_ref"])
                self.assertIsInstance(receipt["lineage_parent_ids"], list)
                self.assertIn(receipt["status"], {"accepted", "rejected"})
                self.assertIn(receipt["validation_status"], {"passed", "failed"})
                self.assertIsInstance(receipt["contribution_attribution"], dict)

    def test_local_job_submission_persists_provenance_without_execution(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))

            response = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": ["data/prior-job.json"],
                    "attribution_metadata": {"project": "prototype"},
                }
            )

            self.assertRegex(response["job_id"], r"^local-job-[a-f0-9]{32}$")
            self.assertEqual(response["status"], "queued")
            self.assertEqual(
                response["next_validation_expectation"],
                "pending_requested_local_validation",
            )
            self.assertEqual(response["network_mode"], "local-only-no-p2p")
            manifest_path = Path(temp_dir) / response["manifest_ref"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(manifest["job"]["job_id"], response["job_id"])
            self.assertIsInstance(manifest["submitted_at"], int)
            self.assertEqual(manifest["initial_state"], "created")
            self.assertEqual(
                manifest["job"]["input_payload"],
                {"payload_type": "json", "content": {"message": "hello"}},
            )
            self.assertRegex(
                manifest["job"]["input_payload_hash"], r"^sha256:[0-9a-f]{64}$"
            )
            self.assertEqual(
                manifest["lineage"]["parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(manifest["lineage"]["job_id"], response["job_id"])
            self.assertEqual(
                manifest["contribution_attribution"],
                {
                    "job_id": response["job_id"],
                    "creator_node_id": "creator-local-a",
                    "metadata": {"project": "prototype"},
                },
            )
            second = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "second"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": ["data/prior-job.json"],
                    "attribution_metadata": {"project": "prototype"},
                }
            )
            self.assertNotEqual(response["job_id"], second["job_id"])
            self.assertFalse((Path(temp_dir) / "data" / "receipts").exists())

    def test_local_job_submission_appends_complete_audit_events_before_queueing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "private"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }

            first = service.submit_local_job(request)
            second = service.submit_local_job(request)
            audit_path = root / "data" / "audit" / "job-submissions.jsonl"
            events = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(len(events), 2)
            self.assertNotEqual(events[0]["event_id"], events[1]["event_id"])
            for event, submission in zip(events, (first, second), strict=True):
                self.assertEqual(event["event_type"], "job_submitted")
                self.assertTrue(event["local_node_id"])
                self.assertEqual(event["creator_node_id"], "creator-local-a")
                self.assertEqual(event["job_id"], submission["job_id"])
                self.assertEqual(event["manifest_ref"], submission["manifest_ref"])
                self.assertEqual(event["lineage_refs"], ["data/prior-job.json"])
                self.assertEqual(event["validation_expectation"], "deterministic-local")
                self.assertEqual(
                    event["contribution_attribution"]["creator_node_id"],
                    "creator-local-a",
                )
                self.assertIn("metadata_hash", event["contribution_attribution"])
                self.assertNotIn("private", json.dumps(event))

    def test_local_job_is_not_accepted_when_submission_audit_write_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            job_id = "local-job-0123456789abcdef0123456789abcdef"
            request = {
                "schema_version": 1,
                "job_id": job_id,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": [],
                "attribution_metadata": {},
            }

            with (
                patch(
                    "aethermesh_core.runtime_service.append_local_audit_event",
                    side_effect=OSError("read-only audit log"),
                ),
                self.assertRaisesRegex(
                    RuntimeServiceError,
                    "could not write local job submission audit event",
                ),
            ):
                service.submit_local_job(request)

            self.assertFalse((root / "data" / "job-status" / f"{job_id}.json").exists())
            self.assertFalse(
                (root / "data" / "job-submissions" / f"{job_id}.json").exists()
            )

    def test_local_job_manifest_is_removed_when_audit_identity_is_unavailable(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            job_id = "local-job-0123456789abcdef0123456789abcdef"
            request = {
                "schema_version": 1,
                "job_id": job_id,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": [],
                "attribution_metadata": {},
            }

            with (
                patch.object(
                    service,
                    "_local_node_id_for_submission",
                    side_effect=RuntimeServiceError("identity unavailable"),
                ),
                self.assertRaisesRegex(
                    RuntimeServiceError,
                    "could not write local job submission audit event",
                ),
            ):
                service.submit_local_job(request)

            self.assertFalse((root / "data" / "job-status" / f"{job_id}.json").exists())
            self.assertFalse(
                (root / "data" / "job-submissions" / f"{job_id}.json").exists()
            )

    def test_execution_finish_audits_terminal_provenance_once(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "done"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            successful = service.submit_local_job(request)
            completed = service.execute_submitted_local_job(
                successful["job_id"], "worker-local-a"
            )
            failed = service.submit_local_job(
                {
                    **request,
                    "job_type": "text_stats",
                    "requested_capability": {"identifier": "work.text_stats"},
                }
            )
            failed_status = service.execute_submitted_local_job(
                failed["job_id"], "worker-local-b"
            )
            audit_path = root / "data" / "audit" / "job-executions.jsonl"
            before_status_checks = audit_path.read_text(encoding="utf-8")
            service.get_local_job_status(successful["job_id"])
            service.get_local_job_status(failed["job_id"])
            self.assertEqual(
                audit_path.read_text(encoding="utf-8"), before_status_checks
            )

            finish_events = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
                if json.loads(line)["event_type"] == "job.execution.finished"
            ]
            self.assertEqual(len(finish_events), 2)
            success_event, failure_event = finish_events
            self.assertEqual(success_event["terminal_status"], "completed")
            self.assertEqual(success_event["job_id"], successful["job_id"])
            self.assertEqual(
                success_event["execution_id"],
                f"local-execution-{successful['job_id']}",
            )
            self.assertEqual(success_event["creator_node_id"], "creator-local-a")
            self.assertEqual(success_event["worker_node_id"], "worker-local-a")
            self.assertEqual(success_event["manifest_ref"], successful["manifest_ref"])
            self.assertEqual(success_event["lineage_refs"], ["data/prior-job.json"])
            self.assertEqual(
                success_event["validation_receipt_ref"],
                completed["validation"]["receipt_ref"],
            )
            self.assertEqual(success_event["validator_node_id"], "worker-local-a")
            self.assertEqual(
                success_event["contribution_attribution"],
                {
                    "job_id": successful["job_id"],
                    "creator_node_id": "creator-local-a",
                    "worker_node_id": "worker-local-a",
                    "validator_node_id": "worker-local-a",
                    "metadata_hash": success_event["contribution_attribution"][
                        "metadata_hash"
                    ],
                },
            )
            self.assertEqual(
                success_event["contribution_attribution_ids"],
                [f"local-contribution-{successful['job_id']}"],
            )
            self.assertEqual(
                success_event["output_artifact_refs"], [completed["result"]["ref"]]
            )
            self.assertIsNone(success_event["error_summary"])
            self.assertEqual(failure_event["terminal_status"], "failed")
            self.assertEqual(failure_event["job_id"], failed["job_id"])
            self.assertEqual(failure_event["worker_node_id"], "worker-local-b")
            self.assertEqual(
                failure_event["validation_receipt_ref"],
                failed_status["validation"]["receipt_ref"],
            )
            self.assertEqual(
                failure_event["output_artifact_refs"],
                [failed_status["result"]["ref"]],
            )
            self.assertEqual(failure_event["error_summary"], failed_status["error"])

            cancel_root = root / "cancelled-runtime"
            cancel_service = NodeRuntimeService.from_home(cancel_root)
            cancelled = cancel_service.submit_local_job(request)
            cancel_service.cancel_submitted_local_job(cancelled["job_id"])
            cancellation_events = [
                json.loads(line)
                for line in (cancel_root / "data" / "audit" / "job-executions.jsonl")
                .read_text(encoding="utf-8")
                .splitlines()
                if json.loads(line)["event_type"] == "job.execution.finished"
            ]
            self.assertEqual(len(cancellation_events), 1)
            self.assertEqual(cancellation_events[0]["terminal_status"], "cancelled")
            self.assertIsNone(cancellation_events[0]["validation_receipt_ref"])

    def test_validation_receipt_creation_audit_is_append_only_and_truthful(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "expected"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-receipt.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            accepted = service.submit_local_job(request)
            service.execute_submitted_local_job(accepted["job_id"], "worker-local-a")
            rejected = service.submit_local_job(
                {
                    **request,
                    "job_type": "text_stats",
                    "requested_capability": {"identifier": "work.text_stats"},
                }
            )
            service.execute_submitted_local_job(rejected["job_id"], "worker-local-b")

            audit_path = root / "data" / "audit" / "validation-receipt-creations.jsonl"
            events = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]

            self.assertEqual(len(events), 2)
            self.assertEqual(
                [event["validation_result"] for event in events],
                ["accepted", "rejected"],
            )
            for event, submission, worker in zip(
                events,
                (accepted, rejected),
                ("worker-local-a", "worker-local-b"),
                strict=True,
            ):
                self.assertEqual(event["event_type"], "validation_receipt_created")
                self.assertEqual(event["creator_node_id"], "creator-local-a")
                self.assertEqual(event["actor_node_id"], worker)
                self.assertEqual(event["validator_node_id"], worker)
                self.assertEqual(event["manifest_ref"], submission["manifest_ref"])
                self.assertEqual(
                    event["validation_receipt_ref"],
                    f"data/job-validation-receipts/{submission['job_id']}.json",
                )
                self.assertEqual(
                    event["lineage_refs"],
                    [submission["manifest_ref"], "data/prior-receipt.json"],
                )
                self.assertEqual(
                    event["contribution_attribution"]["creator_node_id"],
                    "creator-local-a",
                )

    def test_validation_receipt_creation_fails_when_audit_append_fails(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "schema_version": 1,
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {"payload_type": "json", "content": {}},
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )

            def fail_receipt_audit(path: Path, event: dict[str, Any]) -> dict[str, Any]:
                if path.name == "validation-receipt-creations.jsonl":
                    raise OSError("read-only audit log")
                return real_append_audit_event(path, event)

            with (
                patch(
                    "aethermesh_core.runtime_service.append_local_audit_event",
                    side_effect=fail_receipt_audit,
                ),
                self.assertRaisesRegex(
                    RuntimeServiceError,
                    "could not write local validation receipt creation audit event",
                ),
            ):
                service.execute_submitted_local_job(
                    submission["job_id"], "worker-local-a"
                )

            self.assertTrue(
                (
                    root
                    / "data"
                    / "job-validation-receipts"
                    / f"{submission['job_id']}.json"
                ).is_file()
            )
            self.assertFalse(
                (
                    root / "data" / "audit" / "validation-receipt-creations.jsonl"
                ).exists()
            )

    def test_execution_start_audits_precede_work_and_preserve_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "private execution payload"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            first = service.submit_local_job(request)
            second = service.submit_local_job(request)
            audit_path = root / "data" / "audit" / "job-executions.jsonl"
            runner = run_local_job

            def run_after_start(*args: Any, **kwargs: Any) -> JobResult:
                events = [
                    json.loads(line)
                    for line in audit_path.read_text(encoding="utf-8").splitlines()
                ]
                self.assertEqual(len(events), 1)
                self.assertFalse(
                    (root / "data" / "job-results" / f"{first['job_id']}.json").exists()
                )
                return runner(*args, **kwargs)

            with patch(
                "aethermesh_core.runtime_service.run_local_job", run_after_start
            ):
                service.execute_submitted_local_job(first["job_id"], "worker-local-a")
            service.execute_submitted_local_job(second["job_id"], "worker-local-a")

            events = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(events), 4)
            started_events = [
                event
                for event in events
                if event["event_type"] == "job.execution.started"
            ]
            self.assertEqual(len(started_events), 2)
            self.assertNotEqual(
                started_events[0]["event_id"], started_events[1]["event_id"]
            )
            for event, submission in zip(started_events, (first, second), strict=True):
                self.assertEqual(event["event_type"], "job.execution.started")
                self.assertEqual(event["job_id"], submission["job_id"])
                self.assertEqual(event["creator_node_id"], "creator-local-a")
                self.assertEqual(event["executing_node_id"], "worker-local-a")
                self.assertEqual(event["manifest_id"], submission["job_id"])
                self.assertEqual(event["manifest_ref"], submission["manifest_ref"])
                self.assertEqual(event["lineage_refs"], ["data/prior-job.json"])
                self.assertEqual(
                    event["contribution_attribution"]["creator_node_id"],
                    "creator-local-a",
                )
                self.assertIn("input_payload_hash", event["hashes"])
                self.assertNotIn("private execution payload", json.dumps(event))

    def test_execution_start_audit_failure_keeps_job_queued_and_blocks_work(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "schema_version": 1,
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {"payload_type": "json", "content": {}},
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            with (
                patch(
                    "aethermesh_core.runtime_service.append_local_audit_event",
                    side_effect=OSError("read-only audit log"),
                ),
                patch("aethermesh_core.runtime_service.run_local_job") as runner,
                self.assertRaisesRegex(
                    RuntimeServiceError,
                    "could not write local job execution start audit event",
                ),
            ):
                service.execute_submitted_local_job(
                    submission["job_id"], "worker-local-a"
                )
            runner.assert_not_called()
            self.assertEqual(
                service.get_local_job_status(submission["job_id"])["status"], "queued"
            )

    def test_execution_start_requires_manifest_creator_identifier(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "schema_version": 1,
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {"payload_type": "json", "content": {}},
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            manifest_path = root / submission["manifest_ref"]
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest["creator_node_id"] = ""
            manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError, "creator_node_id is required"
            ):
                service.execute_submitted_local_job(
                    submission["job_id"], "worker-local-a"
                )
            self.assertFalse(
                (root / "data" / "audit" / "job-executions.jsonl").exists()
            )
            self.assertEqual(
                service.get_local_job_status(submission["job_id"])["status"], "queued"
            )

    def test_supplied_local_job_id_retries_idempotently_and_rejects_conflicts(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            job_id = "local-job-0123456789abcdef0123456789abcdef"
            request = {
                "schema_version": 1,
                "job_id": job_id,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "durable"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            submission = service.submit_local_job(request)
            manifest_path = root / submission["manifest_ref"]
            status_path = service._job_status_path(job_id)
            manifest_before = manifest_path.read_bytes()
            self.assertRegex(
                json.loads(manifest_before)["submission_fingerprint"],
                r"^sha256:[0-9a-f]{64}$",
            )

            restarted = NodeRuntimeService.from_home(root)
            discovered = restarted.get_local_job_status(job_id)
            self.assertEqual(discovered["job_id"], job_id)
            self.assertEqual(discovered["status"], "queued")
            self.assertEqual(discovered["creator_node_id"], "creator-local-a")
            self.assertEqual(discovered["manifest_ref"], submission["manifest_ref"])
            self.assertEqual(
                discovered["lineage"]["parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                discovered["contribution_attribution"]["metadata"],
                {"project": "prototype"},
            )

            completed = restarted.execute_submitted_local_job(job_id, "worker-local-a")
            receipt_path = root / completed["validation"]["receipt_ref"]
            self.assertTrue(receipt_path.is_file())
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertEqual(json.loads(receipt_path.read_text())["job_id"], job_id)
            status_before_retry = status_path.read_bytes()
            receipt_before_retry = receipt_path.read_bytes()
            retry = restarted.submit_local_job(request)
            self.assertTrue(retry["idempotent_retry"])
            self.assertEqual(retry["status"], "succeeded")
            self.assertEqual(retry["creator_node_id"], "creator-local-a")
            self.assertEqual(
                retry["validation"]["receipt_ref"],
                completed["validation"]["receipt_ref"],
            )
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertEqual(status_path.read_bytes(), status_before_retry)
            self.assertEqual(receipt_path.read_bytes(), receipt_before_retry)

            with self.assertRaisesRegex(RuntimeServiceError, "different content"):
                restarted.submit_local_job(
                    {
                        **request,
                        "input_payload": {
                            "payload_type": "json",
                            "content": {"message": "must not overwrite"},
                        },
                    }
                )
            self.assertEqual(manifest_path.read_bytes(), manifest_before)
            self.assertEqual(status_path.read_bytes(), status_before_retry)
            self.assertEqual(receipt_path.read_bytes(), receipt_before_retry)
            with self.assertRaisesRegex(RuntimeServiceError, "job_id"):
                restarted.submit_local_job({**request, "job_id": "local-job-invalid"})

            completed_status = restarted.get_local_job_status(job_id)
            self.assertEqual(completed_status["lineage"]["job_id"], job_id)
            self.assertEqual(
                completed_status["contribution_attribution"]["job_id"], job_id
            )
            state_audit_path = root / completed_status["state_audit_refs"][0]
            self.assertTrue(
                all(
                    json.loads(line)["job_id"] == job_id
                    for line in state_audit_path.read_text().splitlines()
                )
            )
            self.assertIn(job_id, "\n".join(restarted.recent_logs()["events"]))

    def test_local_job_submission_resolves_manifest_creation_race(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "schema_version": 1,
                "job_id": "local-job-fedcba9876543210fedcba9876543210",
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "race-safe"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": [],
                "attribution_metadata": {},
            }
            existing_state = {"job_id": request["job_id"], "idempotent_retry": True}
            with (
                patch(
                    "aethermesh_core.runtime_service.atomic_create_json",
                    side_effect=FileExistsError,
                ),
                patch.object(
                    service,
                    "_existing_local_submission",
                    return_value=existing_state,
                ),
            ):
                self.assertEqual(service.submit_local_job(request), existing_state)

    def test_local_job_states_are_auditable_and_terminal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            submission = service.submit_local_job(request)
            job_id = submission["job_id"]
            manifest_path = root / submission["manifest_ref"]
            manifest_before = manifest_path.read_text(encoding="utf-8")

            with self.assertRaisesRegex(
                RuntimeServiceError, "invalid local job state transition"
            ):
                service._transition_local_job_state(job_id, "succeeded")
            completed = service.execute_submitted_local_job(job_id, "worker-local-a")
            self.assertEqual(completed["status"], "succeeded")
            self.assertEqual(manifest_path.read_text(encoding="utf-8"), manifest_before)
            self.assertEqual(completed["creator_node_id"], "creator-local-a")
            self.assertEqual(
                completed["lineage"]["parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                completed["contribution_attribution"]["creator_node_id"],
                "creator-local-a",
            )
            self.assertEqual(
                set(completed["timestamps"]),
                {"created_at", "queued_at", "running_at", "succeeded_at"},
            )
            audit_path = root / completed["state_audit_refs"][0]
            audit_states = [
                json.loads(line)["state"]
                for line in audit_path.read_text().splitlines()
            ]
            self.assertEqual(audit_states, ["queued", "running", "succeeded"])
            self.assertEqual(len(completed["state_audit_refs"]), 3)

            with self.assertRaisesRegex(RuntimeServiceError, "not queued"):
                service.execute_submitted_local_job(job_id, "worker-local-a")
            with self.assertRaisesRegex(RuntimeServiceError, "terminal local job"):
                service.cancel_submitted_local_job(job_id)

            canceled = service.submit_local_job(request)
            self.assertEqual(
                service.cancel_submitted_local_job(canceled["job_id"])["status"],
                "canceled",
            )

            missing_status = service.submit_local_job(request)
            service._job_status_path(missing_status["job_id"]).unlink()
            self.assertEqual(
                service.inspect_local_audit_events(
                    manifest_id=missing_status["job_id"]
                )["total_matching"],
                1,
            )

    def test_executor_start_timestamp_is_receipted_and_validation_backed(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "timestamped"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            first = service.submit_local_job(request)
            second = service.submit_local_job(request)
            execution_timestamps = [
                "2026-07-12T20:36:43.000001Z",
                "2026-07-12T20:36:43.000002Z",
                "2026-07-12T20:36:43.000003Z",
                "2026-07-12T20:36:43.000004Z",
                "2026-07-12T20:36:43.000005Z",
                "2026-07-12T20:36:43.000006Z",
            ]
            with (
                patch(
                    "aethermesh_core.runtime_service._utc_timestamp",
                    side_effect=execution_timestamps,
                ),
                patch(
                    "aethermesh_core.runtime_service._utc_timestamp_from_unix_seconds",
                    return_value="2026-07-11T20:36:43.000000Z",
                ),
            ):
                service.execute_submitted_local_job(first["job_id"], "worker-local-a")
                service.execute_submitted_local_job(second["job_id"], "worker-local-a")

            for (
                submission,
                expected_started_at,
                expected_finished_at,
                expected_reported_at,
            ) in zip(
                (first, second),
                execution_timestamps[::3],
                execution_timestamps[1::3],
                execution_timestamps[2::3],
                strict=True,
            ):
                job_id = submission["job_id"]
                manifest = json.loads(
                    (root / submission["manifest_ref"]).read_text(encoding="utf-8")
                )
                result = json.loads(
                    (root / "data" / "job-results" / f"{job_id}.json").read_text(
                        encoding="utf-8"
                    )
                )
                receipt_path = (
                    root / "data" / "job-validation-receipts" / f"{job_id}.json"
                )
                receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
                self.assertEqual(result["started_at"], expected_started_at)
                self.assertEqual(
                    receipt["execution"]["executor_started_at"], expected_started_at
                )
                self.assertEqual(result["finished_at"], expected_finished_at)
                self.assertEqual(
                    receipt["execution"]["executor_finished_at"], expected_finished_at
                )
                self.assertEqual(
                    receipt["execution"]["duration_ms"],
                    _duration_ms(expected_started_at, expected_finished_at),
                )
                self.assertLessEqual(expected_started_at, expected_finished_at)
                self.assertEqual(result["reported_at"], expected_reported_at)
                self.assertLessEqual(expected_finished_at, expected_reported_at)
                self.assertRegex(
                    expected_started_at,
                    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$",
                )
                self.assertEqual(receipt["job_id"], manifest["job"]["job_id"])
                self.assertEqual(receipt["manifest_ref"], submission["manifest_ref"])
                self.assertEqual(
                    receipt["creator_node_id"], manifest["creator_node_id"]
                )
                self.assertEqual(
                    receipt["lineage_parent_refs"], manifest["lineage"]["parent_refs"]
                )
                self.assertEqual(
                    receipt["contribution_attribution"],
                    {
                        **manifest["contribution_attribution"],
                        "worker_node_id": "worker-local-a",
                        "executor_node_id": "worker-local-a",
                        "validated_contribution_units": 1,
                    },
                )
                self.assertEqual(
                    service.get_local_validation_receipt(work_id=job_id)[
                        "executor_started_at"
                    ],
                    expected_started_at,
                )
                self.assertEqual(
                    service.get_local_validation_receipt(work_id=job_id)[
                        "executor_finished_at"
                    ],
                    expected_finished_at,
                )
                self.assertEqual(
                    service.get_local_validation_receipt(work_id=job_id)["duration_ms"],
                    _duration_ms(expected_started_at, expected_finished_at),
                )
                self.assertEqual(
                    json.loads(receipt_path.read_text(encoding="utf-8"))["execution"][
                        "executor_finished_at"
                    ],
                    expected_finished_at,
                )

            self.assertNotEqual(*execution_timestamps[::3])
            self.assertEqual(
                _duration_ms(execution_timestamps[0], execution_timestamps[1]), 0
            )
            self.assertEqual(_result_summary({"output": "value"}), '{"output":"value"}')
            self.assertEqual(_result_summary("x" * 513), "x" * 512)
            self.assertFalse(_is_utc_timestamp(None))
            self.assertFalse(_is_utc_timestamp("not-a-timestamp"))
            self.assertTrue(
                _is_utc_timestamp_before_or_at(execution_timestamps[0], 1783888603)
            )
            self.assertFalse(
                _is_utc_timestamp_before_or_at(execution_timestamps[0], None)
            )
            self.assertFalse(_is_utc_timestamp_before_or_at(None, 1783888603))
            self.assertTrue(
                _is_utc_timestamp_before_or_at_timestamp(
                    execution_timestamps[0], execution_timestamps[1]
                )
            )
            self.assertFalse(
                _is_utc_timestamp_before_or_at_timestamp(
                    execution_timestamps[1], execution_timestamps[0]
                )
            )
            self.assertFalse(
                _is_utc_timestamp_before_or_at_timestamp(None, execution_timestamps[1])
            )
            self.assertFalse(
                _is_utc_timestamp_before_or_at_timestamp(execution_timestamps[0], None)
            )
            second_receipt_path = (
                root / "data" / "job-validation-receipts" / f"{second['job_id']}.json"
            )
            second_receipt = json.loads(second_receipt_path.read_text(encoding="utf-8"))
            second_receipt["execution"]["duration_ms"] = 1
            second_receipt_path.write_text(json.dumps(second_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "invalid executor timing evidence"
            ):
                service.get_local_validation_receipt(work_id=second["job_id"])

            second_receipt["execution"].pop("duration_ms")
            second_receipt_path.write_text(json.dumps(second_receipt), encoding="utf-8")
            legacy_receipt_view = service.get_local_validation_receipt(
                work_id=second["job_id"]
            )
            self.assertNotIn("duration_ms", legacy_receipt_view)

            second_receipt["execution"]["executor_started_at"] = "not-a-timestamp"
            second_receipt_path.write_text(json.dumps(second_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "invalid executor timing evidence"
            ):
                service.get_local_validation_receipt(work_id=second["job_id"])

            future_started_at = "2999-07-12T20:36:43.000002Z"
            second_receipt["execution"]["executor_started_at"] = future_started_at
            second_receipt_path.write_text(json.dumps(second_receipt), encoding="utf-8")
            second_result_path = (
                root / "data" / "job-results" / f"{second['job_id']}.json"
            )
            second_result = json.loads(second_result_path.read_text(encoding="utf-8"))
            second_result["started_at"] = future_started_at
            second_result_path.write_text(json.dumps(second_result), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "job result record violates its schema"
            ):
                service.get_local_validation_receipt(work_id=second["job_id"])

    def test_local_job_requester_identity_preserves_request_origin(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            node_requested = service.submit_local_job(
                {**request, "requester_identity": {"requesting_node_id": "node-a"}}
            )
            local_requested = service.submit_local_job(
                {
                    **request,
                    "requester_identity": {"local_requester_identity": "developer-cli"},
                }
            )
            absent_requested = service.submit_local_job(request)
            unknown_requested = service.submit_local_job(
                {**request, "requester_identity": {"status": "unknown"}}
            )
            with self.assertRaisesRegex(RuntimeServiceError, "requester_identity"):
                service.submit_local_job(
                    {**request, "requester_identity": ["not-an-identity-object"]}
                )
            completed = service.execute_submitted_local_job(
                local_requested["job_id"], "worker-local-a"
            )
            node_manifest = json.loads(
                (Path(temp_dir) / node_requested["manifest_ref"]).read_text(
                    encoding="utf-8"
                )
            )
            receipt = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-validation-receipts"
                    / f"{local_requested['job_id']}.json"
                ).read_text(encoding="utf-8")
            )

            self.assertEqual(
                node_manifest["requester_identity"], {"requesting_node_id": "node-a"}
            )
            self.assertEqual(
                completed["requester_identity"],
                {"local_requester_identity": "developer-cli"},
            )
            self.assertEqual(
                receipt["requester_identity"],
                {"local_requester_identity": "developer-cli"},
            )
            self.assertIsNone(
                service.get_local_job_status(absent_requested["job_id"])[
                    "requester_identity"
                ]
            )
            self.assertEqual(
                service.get_local_job_status(unknown_requested["job_id"])[
                    "requester_identity"
                ],
                {"status": "unknown"},
            )
            self.assertEqual(completed["creator_node_id"], "creator-local-a")
            self.assertEqual(
                completed["lineage"]["parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                completed["contribution_attribution"]["creator_node_id"],
                "creator-local-a",
            )
            self.assertEqual(receipt["validator_id"], "worker-local-a")
            self.assertEqual(
                service.get_local_validation_receipt(work_id=local_requested["job_id"])[
                    "requester_identity"
                ],
                {"local_requester_identity": "developer-cli"},
            )

    def test_local_job_submission_rejects_invalid_request_without_manifest(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": [],
                "attribution_metadata": {},
            }

            with self.assertRaisesRegex(RuntimeServiceError, "creator_node_id"):
                service.submit_local_job(request)
            request["creator_node_id"] = "creator-local-a"
            request["input_payload"] = ["not", "an", "object"]
            with self.assertRaisesRegex(RuntimeServiceError, "input_payload"):
                service.submit_local_job(request)

            self.assertFalse((Path(temp_dir) / "data" / "job-submissions").exists())

    def test_submission_status_reports_persistence_failure_with_admission_context(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "record me"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"source": "test"},
            }
            with patch(
                "aethermesh_core.runtime_service.atomic_create_json",
                side_effect=OSError("disk unavailable"),
            ):
                response = service.submit_local_job_status(request)

            self.assertEqual(response["status"], "failed")
            self.assertEqual(response["validation"]["state"], "passed")
            self.assertEqual(response["creator_node_id"], "creator-local-a")
            self.assertIn("could not be recorded", response["message"])
            self.assertIsNone(response["manifest_ref"])
            self.assertIsNone(response["lineage_ref"])
            self.assertIsNone(response["attribution_ref"])

    def test_malformed_job_admission_preserves_local_evidence(self) -> None:
        """Reject malformed local submissions before they can create durable evidence."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            valid_request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "accepted work"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            accepted = service.submit_local_job(valid_request)

            def data_snapshot() -> dict[str, bytes]:
                return {
                    path.relative_to(root).as_posix(): path.read_bytes()
                    for path in (root / "data").rglob("*")
                    if path.is_file()
                }

            evidence_before = data_snapshot()
            self.assertIn(accepted["manifest_ref"], evidence_before)
            self.assertFalse(
                any("job-validation-receipts" in path for path in evidence_before)
            )
            malformed_requests = (
                (
                    "missing job_type",
                    {
                        key: value
                        for key, value in valid_request.items()
                        if key != "job_type"
                    },
                    "job_type",
                ),
                (
                    "invalid job_type type",
                    {**valid_request, "job_type": []},
                    "job_type",
                ),
                (
                    "unsupported schema version",
                    {**valid_request, "schema_version": 2},
                    "schema_version",
                ),
                (
                    "invalid creator node ID",
                    {**valid_request, "creator_node_id": ""},
                    "creator_node_id",
                ),
                (
                    "malformed lineage reference",
                    {
                        **valid_request,
                        "lineage_parent_refs": [
                            "https://remote.example/prior-job.json"
                        ],
                    },
                    "lineage_parent_refs",
                ),
                (
                    "attribution cannot override creator identity",
                    {
                        **valid_request,
                        "attribution_metadata": {"creator_node_id": "other-node"},
                    },
                    "reserved provenance fields: creator_node_id",
                ),
                (
                    "attribution must preserve JSON object keys",
                    {**valid_request, "attribution_metadata": {1: "not-json-key"}},
                    "JSON-compatible data",
                ),
                (
                    "oversized attribution metadata",
                    {
                        **valid_request,
                        "attribution_metadata": {"note": "x" * 4096},
                    },
                    "4096-byte",
                ),
                (
                    "invalid input payload type",
                    {**valid_request, "input_payload": []},
                    "input_payload",
                ),
            )

            for name, malformed_request, error in malformed_requests:
                with self.subTest(name=name):
                    with self.assertRaisesRegex(RuntimeServiceError, error):
                        service.submit_local_job(malformed_request)
                    self.assertEqual(data_snapshot(), evidence_before)

            status = service.get_local_job_status(accepted["job_id"])
            self.assertEqual(status["creator_node_id"], "creator-local-a")
            self.assertEqual(status["lineage"]["parent_refs"], ["data/prior-job.json"])
            self.assertEqual(
                status["contribution_attribution"],
                {
                    "job_id": accepted["job_id"],
                    "creator_node_id": "creator-local-a",
                    "metadata": {"project": "prototype"},
                },
            )

    def test_unsupported_job_type_is_rejected_before_local_evidence(self) -> None:
        """Unsupported work is admission failure, not queued or credited work."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_id": "local-job-0123456789abcdef0123456789abcdef",
                "job_type": "unknown_work",
                "requested_capability": {"identifier": "work.unknown_work"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "must not execute"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            request_before_rejection = json.loads(json.dumps(request))

            with self.assertRaisesRegex(
                RuntimeServiceError,
                "job submission job_type unsupported: 'unknown_work'; "
                "supported local types: echo, hash, basic_compute, schema_transform, "
                "keyword_extract, text_chunk, text_embed, text_stats",
            ):
                service.submit_local_job(request)

            rejection = service.submit_local_job_status(request)
            self.assertEqual(rejection["status"], "rejected")
            self.assertEqual(rejection["creator_node_id"], "creator-local-a")
            self.assertEqual(rejection["job_id"], request["job_id"])
            self.assertEqual(rejection["job_type"], "unknown_work")
            self.assertEqual(
                rejection["requested_capability"], {"identifier": "work.unknown_work"}
            )
            self.assertEqual(
                rejection["lineage"],
                {"job_id": request["job_id"], "parent_refs": ["data/prior-job.json"]},
            )
            self.assertEqual(
                rejection["contribution_attribution"],
                {
                    "job_id": request["job_id"],
                    "creator_node_id": "creator-local-a",
                    "metadata": {"project": "prototype"},
                },
            )
            self.assertIsNone(rejection["manifest_ref"])
            self.assertIsNone(rejection["lineage_ref"])
            self.assertIsNone(rejection["attribution_ref"])
            self.assertEqual(
                rejection["validation"], {"state": "rejected", "receipt_ref": None}
            )
            self.assertIn("unknown_work", rejection["message"])
            self.assertEqual(request, request_before_rejection)
            self.assertFalse((root / "data" / "job-submissions").exists())
            self.assertFalse((root / "data" / "job-status").exists())
            self.assertFalse((root / "data" / "job-results").exists())
            self.assertFalse((root / "data" / "job-validation-receipts").exists())
            self.assertEqual(
                service.contribution_summary(),
                {
                    "schema_version": 1,
                    "network_mode": "local-only-no-p2p",
                    "summary_status": "empty",
                    "current_node_identity": {
                        "node_id": None,
                    },
                    "contribution_count": 0,
                    "accepted_work_count": 0,
                    "non_accepted_work_count": 0,
                    "accepted_count": 0,
                    "rejected_count": 0,
                    "pending_count": 0,
                    "unavailable_count": 0,
                    "invalid_count": 0,
                    "latest_receipt_time": None,
                    "items": [],
                },
            )
            self.assertTrue(
                any(
                    "rejected unsupported local job submission" in event
                    and "creator_node_id=creator-local-a" in event
                    and "job_type='unknown_work'" in event
                    for event in service.recent_logs()["events"]
                )
            )

    def test_job_submission_requires_enabled_local_manifest_capability(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {"payload_type": "json", "content": {"message": "ok"}},
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            accepted = service.submit_local_job(request)
            manifest = json.loads(
                (root / accepted["manifest_ref"]).read_text(encoding="utf-8")
            )
            self.assertEqual(
                accepted["requested_capability"], {"identifier": "work.echo"}
            )
            self.assertEqual(
                manifest["job"]["requested_capability"], {"identifier": "work.echo"}
            )
            self.assertEqual(manifest["creator_node_id"], "creator-local-a")
            self.assertEqual(
                manifest["lineage"]["parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                service.get_local_job_status(accepted["job_id"])[
                    "requested_capability"
                ],
                {"identifier": "work.echo"},
            )

            def data_snapshot() -> dict[str, bytes]:
                return {
                    path.relative_to(root).as_posix(): path.read_bytes()
                    for path in (root / "data").rglob("*")
                    if path.is_file()
                }

            evidence_before_rejections = data_snapshot()
            rejected_requests = (
                (
                    "missing",
                    {
                        key: value
                        for key, value in request.items()
                        if key != "requested_capability"
                    },
                    "requested_capability",
                ),
                (
                    "malformed",
                    {**request, "requested_capability": "work.echo"},
                    "requested_capability",
                ),
                (
                    "non-json malformed",
                    {**request, "requested_capability": {"identifier": {"bad"}}},
                    "requested_capability",
                ),
                (
                    "unknown",
                    {**request, "requested_capability": {"identifier": "work.unknown"}},
                    "not present",
                ),
                (
                    "mismatched job type",
                    {**request, "job_type": "text_stats"},
                    "does not match job_type",
                ),
            )
            for name, rejected, error in rejected_requests:
                with (
                    self.subTest(name=name),
                    self.assertRaisesRegex(RuntimeServiceError, error),
                ):
                    service.submit_local_job(rejected)
                self.assertEqual(data_snapshot(), evidence_before_rejections)

            config = service.load_config()
            config["capabilities"] = {"enabled_work_types": []}
            service._write_config(config)
            with self.assertRaisesRegex(RuntimeServiceError, "disabled"):
                service.submit_local_job(request)
            self.assertEqual(data_snapshot(), evidence_before_rejections)
            log = (root / "logs" / "events.log").read_text(encoding="utf-8")
            self.assertIn(
                "rejected local job submission creator_node_id=creator-local-a", log
            )
            self.assertIn('requested_capability={"identifier":"work.unknown"}', log)

    def test_input_payload_is_hashed_bounded_and_receipt_linked(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "first"},
                    "parameters": {"trim": False},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": [],
                "attribution_metadata": {},
            }
            first = service.submit_local_job(request)
            second = service.submit_local_job(
                {
                    **request,
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "second"},
                    },
                }
            )
            first_manifest = json.loads(
                (Path(temp_dir) / first["manifest_ref"]).read_text()
            )
            second_manifest = json.loads(
                (Path(temp_dir) / second["manifest_ref"]).read_text()
            )
            self.assertNotEqual(
                first_manifest["job"]["input_payload_hash"],
                second_manifest["job"]["input_payload_hash"],
            )
            service.execute_submitted_local_job(first["job_id"], "worker-local-a")
            receipt = service.get_local_validation_receipt(work_id=first["job_id"])
            self.assertEqual(receipt["manifest_ref"], first["manifest_ref"])
            self.assertEqual(
                receipt["input_payload_hash"],
                first_manifest["job"]["input_payload_hash"],
            )
            for payload, message in (
                ({"content": {}}, "requires payload_type"),
                ({"payload_type": "json", "content": []}, "content"),
                (
                    {"payload_type": "json", "content": {}, "parameters": []},
                    "parameters",
                ),
                (
                    {"payload_type": "json", "content": {"text": "x" * 65536}},
                    "65536-byte",
                ),
            ):
                with (
                    self.subTest(payload=payload),
                    self.assertRaisesRegex(RuntimeServiceError, message),
                ):
                    service.submit_local_job({**request, "input_payload": payload})

    def test_deterministic_executor_metadata_and_receipt_preserve_provenance(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            request = {
                "schema_version": 1,
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "repeatable"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            first = service.submit_local_job(request)
            second = service.submit_local_job(request)
            service.execute_submitted_local_job(first["job_id"], "worker-local-a")
            with patch.object(LocalRunner, "EXECUTOR_VERSION", "2"):
                service.execute_submitted_local_job(second["job_id"], "worker-local-a")

            first_receipt = json.loads(
                (
                    root
                    / "data"
                    / "job-validation-receipts"
                    / f"{first['job_id']}.json"
                ).read_text()
            )
            second_receipt = json.loads(
                (
                    root
                    / "data"
                    / "job-validation-receipts"
                    / f"{second['job_id']}.json"
                ).read_text()
            )
            self.assertEqual(
                first_receipt["execution"]["output_digest"],
                second_receipt["execution"]["output_digest"],
            )
            self.assertEqual(
                first_receipt["execution"]["executor_name"], "aethermesh-local-runner"
            )
            self.assertEqual(first_receipt["execution"]["executor_version"], "1")
            self.assertEqual(second_receipt["execution"]["executor_version"], "2")
            self.assertEqual(first_receipt["manifest_ref"], first["manifest_ref"])
            self.assertEqual(first_receipt["creator_node_id"], "creator-local-a")
            self.assertEqual(
                first_receipt["lineage_parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                first_receipt["contribution_attribution"],
                {
                    "job_id": first["job_id"],
                    "creator_node_id": "creator-local-a",
                    "metadata": {"project": "prototype"},
                    "worker_node_id": "worker-local-a",
                    "executor_node_id": "worker-local-a",
                    "validated_contribution_units": 1,
                },
            )
            self.assertTrue(first_receipt["validation"]["valid"])

    def test_local_job_submission_api_reports_local_validation_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            api = create_app(service)

            async def submit() -> tuple[httpx.Response, httpx.Response]:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    accepted = await client.post(
                        "/api/jobs",
                        json={
                            "job_type": "echo",
                            "requested_capability": {"identifier": "work.echo"},
                            "input_payload": {"payload_type": "json", "content": {}},
                            "creator_node_id": "creator-local-a",
                            "requested_validation_mode": "deterministic-local",
                            "schema_version": 1,
                            "lineage_parent_refs": [],
                            "attribution_metadata": {},
                        },
                    )
                    rejected = await client.post(
                        "/api/jobs",
                        json={
                            "job_type": "echo",
                            "requested_capability": {"identifier": "work.echo"},
                            "input_payload": {"payload_type": "json", "content": {}},
                            "requested_validation_mode": "deterministic-local",
                            "schema_version": 1,
                            "lineage_parent_refs": [],
                            "attribution_metadata": {},
                        },
                    )
                    return accepted, rejected

            accepted, rejected = asyncio.run(submit())
            self.assertEqual(accepted.status_code, 200)
            self.assertEqual(accepted.json()["status"], "accepted")
            self.assertEqual(accepted.json()["validation"]["state"], "passed")
            self.assertEqual(rejected.status_code, 200)
            self.assertEqual(rejected.json()["status"], "rejected")
            self.assertIn("creator_node_id", rejected.json()["message"])

    def test_local_job_status_tracks_queued_succeeded_failed_and_not_found(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            accepted = service.submit_local_job(request)
            queued = service.get_local_job_status(accepted["job_id"])
            succeeded = service.execute_submitted_local_job(
                accepted["job_id"], "worker-local-a"
            )
            failed_request = {
                **request,
                "job_type": "text_stats",
                "requested_capability": {"identifier": "work.text_stats"},
            }
            failed_accepted = service.submit_local_job(failed_request)
            failed = service.execute_submitted_local_job(
                failed_accepted["job_id"], "worker-local-b"
            )

            self.assertEqual(queued["status"], "queued")
            self.assertEqual(queued["manifest_ref"], accepted["manifest_ref"])
            self.assertEqual(queued["creator_node_id"], "creator-local-a")
            self.assertIsNone(queued["worker_node_id"])
            self.assertEqual(
                queued["lineage"],
                {"job_id": accepted["job_id"], "parent_refs": ["data/prior-job.json"]},
            )
            self.assertEqual(
                queued["contribution_attribution"]["metadata"], {"project": "prototype"}
            )
            self.assertIsNone(queued["validation"])
            self.assertIsNone(queued["result"])
            self.assertEqual(succeeded["status"], "succeeded")
            self.assertEqual(succeeded["worker_node_id"], "worker-local-a")
            self.assertEqual(succeeded["executor_node_id"], "worker-local-a")
            for submission, execution, worker, expected_status in (
                (accepted, succeeded, "worker-local-a", "succeeded"),
                (failed_accepted, failed, "worker-local-b", "failed"),
            ):
                with self.subTest(result_status=expected_status):
                    document = json.loads(
                        (
                            Path(temp_dir)
                            / "data"
                            / "job-results"
                            / f"{submission['job_id']}.json"
                        ).read_text(encoding="utf-8")
                    )
                    self.assertIs(validate_job_result_document(document), document)
                    self.assertEqual(document["job_id"], submission["job_id"])
                    self.assertEqual(document["creator_node_id"], "creator-local-a")
                    self.assertEqual(document["executor_node_id"], worker)
                    self.assertEqual(
                        document["model_expert_id"],
                        "local-runner:aethermesh-local-runner@1",
                    )
                    self.assertEqual(document["expert_version"], "1")
                    self.assertEqual(document["status"], expected_status)
                    self.assertEqual(
                        document["validation_status"],
                        "passed" if execution["validation"]["passed"] else "failed",
                    )
                    self.assertEqual(
                        document["contribution"]["creator_node_id"], "creator-local-a"
                    )
                    self.assertEqual(
                        document["contribution"]["executor_node_id"], worker
                    )
                    self.assertEqual(
                        document["contribution"]["validator_node_id"], worker
                    )
                    self.assertEqual(
                        document["references"]["manifest_hash"], document["manifest_id"]
                    )
                    self.assertEqual(
                        document["references"]["validation_receipt_ids"],
                        [document["validation_receipt_id"]],
                    )
                    self.assertEqual(
                        document["lineage"]["parent_job_ids"],
                        ["data/prior-job.json"],
                    )
                    self.assertTrue(document["lineage"]["input_manifest_ids"])
                    self.assertEqual(
                        bool(document["lineage"]["output_manifest_ids"]),
                        expected_status == "succeeded",
                    )
                    if expected_status == "failed":
                        self.assertIsNotNone(document["failure_reasons"]["validation"])
                    receipt = json.loads(
                        (
                            Path(temp_dir)
                            / "data"
                            / "job-validation-receipts"
                            / f"{submission['job_id']}.json"
                        ).read_text(encoding="utf-8")
                    )
                    self.assertEqual(
                        receipt["model_expert_id"], document["model_expert_id"]
                    )
                    self.assertEqual(
                        receipt["expert_version"], document["expert_version"]
                    )
            self.assertEqual(
                succeeded["result"],
                {
                    "ref": f"data/job-results/{accepted['job_id']}.json",
                    "summary": "hello",
                },
            )
            self.assertEqual(
                succeeded["validation"],
                {
                    "receipt_ref": f"data/job-validation-receipts/{accepted['job_id']}.json",
                    "passed": True,
                    "reason": "ok",
                },
            )
            self.assertEqual(
                succeeded["contribution_attribution"]["validated_contribution_units"], 1
            )
            self.assertIsNone(succeeded["error"])
            success_record = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-status"
                    / f"{accepted['job_id']}.json"
                ).read_text(encoding="utf-8")
            )["execution_status"]
            self.assertEqual(success_record["status"], "success")
            self.assertEqual(success_record["work_id"], accepted["job_id"])
            self.assertEqual(success_record["creator_node_id"], "creator-local-a")
            self.assertEqual(success_record["manifest_ref"], accepted["manifest_ref"])
            self.assertEqual(
                success_record["input_lineage_ref"],
                f"{accepted['manifest_ref']}#lineage",
            )
            self.assertEqual(success_record["output_ref"], succeeded["result"]["ref"])
            self.assertEqual(
                success_record["validation_receipt_ref"],
                succeeded["validation"]["receipt_ref"],
            )
            self.assertEqual(
                success_record["contribution_attribution"],
                succeeded["contribution_attribution"],
            )
            self.assertIsNone(success_record["failure_reason"])
            self.assertIsNone(success_record["error_summary"])
            self.assertFalse(success_record["retry_eligible"])
            self.assertEqual(failed["status"], "failed")
            self.assertFalse(failed["validation"]["passed"])
            self.assertEqual(
                failed["contribution_attribution"]["creator_node_id"], "creator-local-a"
            )
            self.assertIn("requires string field: text", str(failed["error"]))
            failure_record = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-status"
                    / f"{failed_accepted['job_id']}.json"
                ).read_text(encoding="utf-8")
            )["execution_status"]
            self.assertEqual(failure_record["status"], "failure")
            self.assertEqual(failure_record["creator_node_id"], "creator-local-a")
            self.assertEqual(
                failure_record["contribution_attribution"],
                failed["contribution_attribution"],
            )
            self.assertEqual(
                failure_record["failure_reason"], failed["validation"]["reason"]
            )
            self.assertEqual(failure_record["error_summary"], failed["error"])
            self.assertFalse(failure_record["retry_eligible"])
            execution_error_accepted = service.submit_local_job(request)
            with patch(
                "aethermesh_core.runtime_service.run_local_job",
                side_effect=OSError("controlled runner fault"),
            ):
                execution_error = service.execute_submitted_local_job(
                    execution_error_accepted["job_id"], "worker-local-c"
                )
            execution_error_record = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-status"
                    / f"{execution_error_accepted['job_id']}.json"
                ).read_text(encoding="utf-8")
            )["execution_status"]
            self.assertEqual(execution_error["status"], "failed")
            self.assertEqual(execution_error_record["status"], "failure")
            self.assertEqual(
                execution_error_record["error_summary"],
                "local execution error: OSError",
            )
            self.assertEqual(
                execution_error_record["validation_receipt_ref"],
                execution_error["validation"]["receipt_ref"],
            )
            failed_result = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-results"
                    / f"{execution_error_accepted['job_id']}.json"
                ).read_text(encoding="utf-8")
            )
            failed_receipt = json.loads(
                (
                    Path(temp_dir)
                    / "data"
                    / "job-validation-receipts"
                    / f"{execution_error_accepted['job_id']}.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(failed_result["status"], "failed")
            self.assertEqual(failed_result["validation_status"], "failed")
            self.assertEqual(failed_result["creator_node_id"], "creator-local-a")
            self.assertEqual(
                failed_receipt["manifest_ref"], execution_error_accepted["manifest_ref"]
            )
            self.assertEqual(failed_receipt["creator_node_id"], "creator-local-a")
            self.assertEqual(
                failed_receipt["lineage_parent_refs"], ["data/prior-job.json"]
            )
            self.assertEqual(
                failed_receipt["contribution_attribution"]["creator_node_id"],
                "creator-local-a",
            )
            self.assertFalse(failed_receipt["validation"]["valid"])
            self.assertEqual(
                failed_receipt["validation"]["reason"], "result_not_completed"
            )
            self.assertEqual(
                failed_receipt["next_local_action"],
                "inspect the local result artifact and validation reason; correct "
                "the work or input before submitting a new local job",
            )
            self.assertEqual(
                failed_receipt["validation_method"]["description"],
                "Ran the deterministic local echo validator against the assigned job and "
                "executor result. The validator checks completion, work identity, "
                "contribution units, payload validity, and expected output in order; "
                "outcome: result_not_completed.",
            )
            following_accepted = service.submit_local_job(request)
            following = service.execute_submitted_local_job(
                following_accepted["job_id"], "worker-local-c"
            )
            self.assertEqual(following["status"], "succeeded")
            self.assertEqual(
                service.get_local_job_status(
                    "local-job-00000000000000000000000000000000"
                ),
                {
                    "schema_version": 1,
                    "job_id": "local-job-00000000000000000000000000000000",
                    "status": "not_found",
                    "error": "local job not found",
                },
            )
            self.assertEqual(
                service.get_local_job_status(""),
                {
                    "schema_version": 1,
                    "job_id": "",
                    "status": "not_found",
                    "error": "local job not found",
                },
            )
            escaped_path = service.paths.data_dir / "outside-submission-directory.json"
            escaped_path.write_text("not JSON", encoding="utf-8")
            self.assertEqual(
                service.get_local_job_status("../outside-submission-directory"),
                {
                    "schema_version": 1,
                    "job_id": "../outside-submission-directory",
                    "status": "not_found",
                    "error": "local job not found",
                },
            )

    def test_local_job_status_api_returns_success_and_stable_not_found(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            api = create_app(service)

            async def fetch() -> tuple[httpx.Response, httpx.Response]:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    accepted = await client.post(
                        "/api/jobs",
                        json={
                            "job_type": "echo",
                            "requested_capability": {"identifier": "work.echo"},
                            "input_payload": {
                                "payload_type": "json",
                                "content": {"message": "hello"},
                            },
                            "creator_node_id": "creator-local-a",
                            "requested_validation_mode": "deterministic-local",
                            "schema_version": 1,
                            "lineage_parent_refs": [],
                            "attribution_metadata": {},
                        },
                    )
                    known = await client.get(f"/api/jobs/{accepted.json()['job_id']}")
                    missing = await client.get(
                        "/api/jobs/local-job-00000000000000000000000000000000"
                    )
                    return known, missing

            known, missing = asyncio.run(fetch())
            self.assertEqual(known.status_code, 200)
            self.assertEqual(known.json()["status"], "queued")
            self.assertEqual(missing.status_code, 200)
            self.assertEqual(
                missing.json(),
                {
                    "schema_version": 1,
                    "job_id": "local-job-00000000000000000000000000000000",
                    "status": "not_found",
                    "error": "local job not found",
                },
            )

    def test_local_job_result_read_path_returns_stored_report_and_preserves_identity(
        self,
    ) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            completed = service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            report_path = root / completed["result"]["ref"]
            report_before_read = report_path.read_bytes()

            report = service.get_local_job_result(submission["job_id"])

            self.assertEqual(report["job_id"], submission["job_id"])
            self.assertEqual(report["creator_node_id"], request["creator_node_id"])
            self.assertEqual(report["validation_status"], "passed")
            self.assertEqual(
                report["references"]["validation_receipt_ids"],
                [f"local-validation-receipt-{submission['job_id']}"],
            )
            self.assertEqual(
                report["lineage"]["parent_job_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                report["contribution"]["creator_node_id"], request["creator_node_id"]
            )
            self.assertEqual(report_path.read_bytes(), report_before_read)

    def test_local_job_attribution_trace_is_validation_gated_and_fails_closed(
        self,
    ) -> None:
        request, _expected_output = _valid_local_work_fixture()
        request["creator_node_id"] = "creator-byte-preserved"
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            evidence_before = {
                path: path.read_bytes() for path in (root / "data").rglob("*.json")
            }

            trace = service.trace_local_job_attribution(submission["job_id"])

            self.assertEqual(
                trace["trace_scope"], "local-only-validation-gated-attribution"
            )
            self.assertEqual(
                [item["record_type"] for item in trace["chain"]],
                [
                    "job",
                    "result",
                    "validation_receipt",
                    "contribution",
                    "manifest",
                    "creator_node",
                ],
            )
            job, result, receipt, contribution, manifest, creator = trace["chain"]
            self.assertEqual(job["result_id"], result["result_id"])
            self.assertEqual(
                result["validation_receipt_id"], receipt["validation_receipt_id"]
            )
            self.assertEqual(
                receipt["contribution_id"], contribution["contribution_id"]
            )
            self.assertEqual(contribution["manifest_ref"], manifest["manifest_ref"])
            self.assertEqual(creator["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                evidence_before,
                {path: path.read_bytes() for path in (root / "data").rglob("*.json")},
            )

            api = create_app(service)

            async def fetch_trace() -> httpx.Response:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    return await client.get(f"/api/jobs/{submission['job_id']}/trace")

            response = asyncio.run(fetch_trace())
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), trace)

            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{submission['job_id']}.json"
            )
            stored_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            stored_receipt["manifest_ref"] = "data/job-submissions/other.json"
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "trace validation receipt"
            ):
                service.trace_local_job_attribution(submission["job_id"])
            failed_response = asyncio.run(fetch_trace())
            self.assertEqual(failed_response.status_code, 400)
            self.assertEqual(
                failed_response.json()["error"]["code"], "TRACE_LINEAGE_FAILURE"
            )
            receipt_path.unlink()
            with self.assertRaisesRegex(RuntimeServiceError, "receipt not found"):
                service.trace_local_job_attribution(submission["job_id"])

    def test_execution_writes_a_provenanced_result_report_only_after_runner_finishes(
        self,
    ) -> None:
        request, expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            job_id = submission["job_id"]
            report_path = root / "data" / "job-results" / f"{job_id}.json"
            manifest = json.loads(
                (root / submission["manifest_ref"]).read_text(encoding="utf-8")
            )

            self.assertFalse(report_path.exists())

            def run_after_asserting_report_is_absent(*args: Any, **kwargs: Any) -> Any:
                self.assertFalse(report_path.exists())
                return run_local_job(*args, **kwargs)

            with patch(
                "aethermesh_core.runtime_service.run_local_job",
                side_effect=run_after_asserting_report_is_absent,
            ):
                completed = service.execute_submitted_local_job(
                    job_id, "worker-local-fixture"
                )

            self.assertEqual(completed["status"], "succeeded")
            self.assertTrue(report_path.exists())
            report = json.loads(report_path.read_text(encoding="utf-8"))
            self.assertEqual(report["job_id"], job_id)
            self.assertEqual(
                report["references"]["manifest_hash"],
                canonical_json_hash(manifest, prefix="sha256:"),
            )
            self.assertEqual(report["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                report["validation_receipt_id"],
                f"local-validation-receipt-{job_id}",
            )
            self.assertEqual(
                report["references"]["validation_receipt_ids"],
                [report["validation_receipt_id"]],
            )
            self.assertEqual(report["validation_status"], "passed")
            self.assertEqual(
                report["lineage"]["parent_job_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                report["contribution"],
                {
                    "creator_node_id": request["creator_node_id"],
                    "executor_node_id": "worker-local-fixture",
                    "validator_node_id": "worker-local-fixture",
                    "upstream_lineage_sources": request["lineage_parent_refs"],
                    "local_operator_id": None,
                },
            )
            self.assertEqual(report["status"], "succeeded")
            self.assertEqual(
                report["output_payload"]["inline_payload"], expected_output["output"]
            )
            self.assertIsNone(report["output_payload"]["payload_ref"])
            self.assertLessEqual(report["started_at"], report["finished_at"])
            self.assertLessEqual(report["finished_at"], report["reported_at"])
            audit_path = root / "data" / "audit" / "result-reports.jsonl"
            audit_events = [
                json.loads(line)
                for line in audit_path.read_text(encoding="utf-8").splitlines()
            ]
            self.assertEqual(len(audit_events), 1)
            audit_event = audit_events[0]
            self.assertEqual(
                audit_event["event_id"], f"local-audit-{job_id}-result-reported"
            )
            self.assertEqual(audit_event["creator_node_id"], request["creator_node_id"])
            self.assertEqual(audit_event["reporting_node_id"], "worker-local-fixture")
            self.assertEqual(audit_event["manifest_id"], report["manifest_id"])
            self.assertEqual(
                audit_event["validation_receipt_id"], report["validation_receipt_id"]
            )
            self.assertEqual(audit_event["lineage_refs"], [submission["manifest_ref"]])
            self.assertEqual(
                audit_event["contribution_attribution_ids"],
                [f"local-contribution-{job_id}"],
            )
            self.assertNotIn("output_payload", audit_event)

            audit_paths = (
                root / "data" / "audit" / "job-submissions.jsonl",
                root / "data" / "audit" / "job-executions.jsonl",
                root / "data" / "audit" / "validation-receipt-creations.jsonl",
                root / "data" / "audit" / "result-reports.jsonl",
            )
            run_events = [
                json.loads(line)
                for path in audit_paths
                for line in path.read_text(encoding="utf-8").splitlines()
                if json.loads(line)["local_run_id"] == job_id
            ]
            self.assertEqual(
                [
                    event["event_sequence"]
                    for event in sorted(
                        run_events, key=lambda event: event["event_sequence"]
                    )
                ],
                [1, 2, 3, 4, 5],
            )
            for event in run_events:
                self.assertEqual(event["local_run_id"], job_id)
                self.assertEqual(event["creator_node_id"], request["creator_node_id"])
                self.assertRegex(event["timestamp"], r"^\d{4}-\d{2}-\d{2}T.*Z$")
                self.assertTrue(event["manifest_id"])
            receipt_event = next(
                event
                for event in run_events
                if event["event_type"] == "validation_receipt_created"
            )
            self.assertEqual(
                receipt_event["validation_receipt_id"], report["validation_receipt_id"]
            )
            self.assertEqual(
                receipt_event["lineage_refs"],
                [submission["manifest_ref"], *request["lineage_parent_refs"]],
            )

            service._append_result_reported_audit_event(
                job_id=job_id,
                worker_node_id="worker-local-fixture",
                manifest=manifest,
                manifest_id=report["manifest_id"],
                manifest_ref=submission["manifest_ref"],
                validation_receipt_ref=f"data/job-validation-receipts/{job_id}.json",
                result_status=report["status"],
            )
            self.assertEqual(
                len(audit_path.read_text(encoding="utf-8").splitlines()), 1
            )

    def test_result_report_audit_requires_existing_manifest_and_receipt(self) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            manifest = json.loads(
                (root / submission["manifest_ref"]).read_text(encoding="utf-8")
            )
            job_id = submission["job_id"]

            with self.assertRaisesRegex(
                RuntimeServiceError, "manifest and validation receipt"
            ):
                service._append_result_reported_audit_event(
                    job_id=job_id,
                    worker_node_id="worker-local-fixture",
                    manifest=manifest,
                    manifest_id=canonical_json_hash(manifest, prefix="sha256:"),
                    manifest_ref=submission["manifest_ref"],
                    validation_receipt_ref=f"data/job-validation-receipts/{job_id}.json",
                    result_status="succeeded",
                )
            self.assertFalse(
                (root / "data" / "audit" / "result-reports.jsonl").exists()
            )

    def test_local_job_result_read_path_rejects_missing_invalid_and_mismatched_reports(
        self,
    ) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            with self.assertRaisesRegex(RuntimeServiceError, "job_id"):
                service.get_local_job_result("not-a-local-job")
            with self.assertRaisesRegex(RuntimeServiceError, "result report not found"):
                service.get_local_job_result(request["job_id"])

            submission = service.submit_local_job(request)
            completed = service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            report_path = root / completed["result"]["ref"]
            mismatched = json.loads(report_path.read_text(encoding="utf-8"))
            mismatched["job_id"] = "local-job-ffffffffffffffffffffffffffffffff"
            mismatched["result_hash"] = canonical_result_document_hash(mismatched)
            report_path.write_text(json.dumps(mismatched), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "does not match its job ID"
            ):
                service.get_local_job_result(submission["job_id"])

    def test_result_report_preflight_rejects_malformed_candidates_without_evidence_mutation(
        self,
    ) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            report = service.get_local_job_result(submission["job_id"])

            def evidence_snapshot() -> dict[str, bytes]:
                return {
                    str(path.relative_to(root / "data")): path.read_bytes()
                    for directory in (
                        "job-results",
                        "job-validation-receipts",
                        "job-status",
                    )
                    for path in (root / "data" / directory).glob("*.json")
                }

            pending_candidate = deepcopy(report)
            pending_candidate["validation_status"] = "not_run"
            pending_candidate["result_hash"] = None
            ready = service.preflight_local_result_report(pending_candidate)
            self.assertEqual(ready["status"], "ready_for_validation")
            self.assertEqual(ready["job_id"], submission["job_id"])
            expected_manifest_hash = canonical_json_hash(
                json.loads(
                    (root / submission["manifest_ref"]).read_text(encoding="utf-8")
                ),
                prefix="sha256:",
            )
            self.assertEqual(
                report["references"]["manifest_hash"], expected_manifest_hash
            )

            fallback_candidate = deepcopy(pending_candidate)
            fallback_candidate["manifest_id"] = submission["manifest_ref"]
            fallback_candidate["references"]["manifest_hash"] = None
            fallback_candidate["references"]["manifest_ref"] = submission[
                "manifest_ref"
            ]
            fallback_candidate["lineage"]["input_manifest_ids"] = [
                submission["manifest_ref"]
            ]
            fallback_ready = service.preflight_local_result_report(fallback_candidate)
            self.assertEqual(fallback_ready["status"], "ready_for_validation")
            self.assertEqual(fallback_ready["manifest_ref"], submission["manifest_ref"])

            malformed_timestamp = deepcopy(report)
            malformed_timestamp["reported_at"] = "not-a-timestamp"
            mismatched_manifest = deepcopy(report)
            mismatched_manifest["manifest_id"] = "sha256:" + "f" * 64
            mismatched_manifest["references"]["manifest_hash"] = mismatched_manifest[
                "manifest_id"
            ]
            mismatched_manifest["result_hash"] = canonical_result_document_hash(
                mismatched_manifest
            )
            mismatched_worker = deepcopy(pending_candidate)
            mismatched_worker["result_id"] = "local-result-forged"
            mismatched_worker["executor_node_id"] = "worker-forged"
            mismatched_worker["validator_node_id"] = "worker-forged"
            mismatched_worker["contribution"]["executor_node_id"] = "worker-forged"
            mismatched_worker["contribution"]["validator_node_id"] = "worker-forged"
            missing_manifest = deepcopy(report)
            missing_manifest["job_id"] = "local-job-ffffffffffffffffffffffffffffffff"
            missing_manifest["task_id"] = missing_manifest["job_id"]
            missing_manifest["result_hash"] = canonical_result_document_hash(
                missing_manifest
            )
            candidates: tuple[object, ...] = (
                {},
                ["wrong-type"],
                malformed_timestamp,
                mismatched_manifest,
                mismatched_worker,
                missing_manifest,
                {"unsupported": "x" * (17 * 1024)},
                {f"unsupported-{'x' * 128}-{index}": None for index in range(32)},
                {"unserializable": {"not-json"}},
            )
            evidence_before = evidence_snapshot()
            contributions_before = service.contribution_summary()

            for candidate in candidates:
                rejection = service.preflight_local_result_report(candidate)
                self.assertEqual(rejection["status"], "rejected")
                self.assertIn(
                    rejection["reason_code"],
                    {
                        "invalid_result_report",
                        "manifest_mismatch",
                        "missing_manifest",
                    },
                )
                self.assertRegex(
                    rejection["attempt_id"], r"^local-rejected-result-report-"
                )
                self.assertLessEqual(len(rejection["reason"]), 512)

            self.assertEqual(evidence_snapshot(), evidence_before)
            self.assertEqual(service.contribution_summary(), contributions_before)
            logs = sorted((root / "data" / "rejected-result-reports").glob("*.json"))
            self.assertEqual(len(logs), len(candidates))
            rejection_logs = [
                json.loads(path.read_text(encoding="utf-8")) for path in logs
            ]
            for rejection_log in rejection_logs:
                self.assertEqual(rejection_log["status"], "rejected")
                self.assertIn(
                    rejection_log["reason_code"],
                    {
                        "invalid_result_report",
                        "manifest_mismatch",
                        "missing_manifest",
                    },
                )
                self.assertRegex(rejection_log["processed_at"], r"Z$")
                self.assertLessEqual(len(rejection_log["reason"]), 512)
                self.assertIn("report", rejection_log)
            self.assertTrue(
                any(log["report"].get("truncated") is True for log in rejection_logs)
            )
            self.assertTrue(
                any(
                    log["report"].get("unserializable_type") == "dict"
                    for log in rejection_logs
                )
            )

            api = create_app(service)

            async def preflight_via_api() -> httpx.Response:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    return await client.post("/api/result-reports/preflight", json={})

            response = asyncio.run(preflight_via_api())
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json()["status"], "rejected")

    def test_local_job_result_api_lists_metadata_and_reads_controlled_details(
        self,
    ) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            submission = service.submit_local_job(request)
            service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            second_request = deepcopy(request)
            second_request["job_id"] = "local-job-fedcba9876543210fedcba9876543210"
            second_submission = service.submit_local_job(second_request)
            service.execute_submitted_local_job(
                second_submission["job_id"], "worker-local-fixture"
            )
            api = create_app(service)

            async def fetch() -> tuple[httpx.Response, ...]:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    return (
                        await client.get("/api/result-reports"),
                        await client.get(f"/api/jobs/{submission['job_id']}/result"),
                        await client.get(
                            "/api/jobs/local-job-00000000000000000000000000000000/result"
                        ),
                        await client.get("/api/jobs/not-a-local-job/result"),
                    )

            listed, stored, missing, invalid = asyncio.run(fetch())
            self.assertEqual(listed.status_code, 200)
            listing = listed.json()
            self.assertEqual(listing["schema_version"], 1)
            self.assertEqual(listing["total"], 2)
            summaries = listing["result_reports"]
            self.assertEqual(
                [summary["job_id"] for summary in summaries],
                sorted([submission["job_id"], second_submission["job_id"]]),
            )
            summary = next(
                item for item in summaries if item["job_id"] == submission["job_id"]
            )
            stored_report = service.get_local_job_result(submission["job_id"])
            self.assertEqual(summary["validation"]["status"], "passed")
            self.assertEqual(
                summary["validation"]["receipt_id"],
                f"local-validation-receipt-{submission['job_id']}",
            )
            self.assertEqual(summary["manifest"]["id"], stored_report["manifest_id"])
            self.assertEqual(
                summary["lineage"]["parent_job_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(summary["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                summary["contribution"]["creator_node_id"], request["creator_node_id"]
            )
            self.assertNotIn("output_payload", summary)
            self.assertNotIn("summary", summary)
            self.assertNotIn("failure_reasons", summary)
            self.assertEqual(stored.status_code, 200)
            detail = stored.json()
            self.assertEqual(detail["job_id"], submission["job_id"])
            self.assertEqual(detail["validation_status"], "passed")
            self.assertEqual(detail["manifest_id"], stored_report["manifest_id"])
            self.assertEqual(detail["creator_node_id"], request["creator_node_id"])
            self.assertEqual(
                detail["lineage"]["parent_job_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                detail["contribution"]["creator_node_id"], request["creator_node_id"]
            )
            self.assertEqual(missing.status_code, 404)
            self.assertEqual(missing.json()["error"]["code"], "RESULT_REPORT_NOT_FOUND")
            self.assertEqual(invalid.status_code, 400)
            self.assertEqual(invalid.json()["error"]["code"], "INVALID_INPUT")

    def test_local_job_result_listing_ignores_unrelated_storage_files(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            self.assertEqual(
                service.list_local_job_results(),
                {"schema_version": 1, "total": 0, "result_reports": []},
            )
            result_directory = root / "data" / "job-results"
            result_directory.mkdir(parents=True)
            (result_directory / "unrelated.json").write_text("not a report")
            self.assertEqual(
                service.list_local_job_results(),
                {"schema_version": 1, "total": 0, "result_reports": []},
            )

    def test_validation_receipt_api_reads_stored_evidence_and_rejects_bad_lookups(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {"project": "prototype"},
            }
            accepted = service.submit_local_job(request)
            pending = service.submit_local_job(request)
            service.execute_submitted_local_job(accepted["job_id"], "worker-local-a")
            api = create_app(service)

            async def fetch() -> tuple[httpx.Response, ...]:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    receipt_id = f"local-validation-receipt-{accepted['job_id']}"
                    return (
                        await client.get(
                            "/api/validation-receipts",
                            params={"receipt_id": receipt_id},
                        ),
                        await client.get(
                            "/api/validation-receipts",
                            params={"work_id": accepted["job_id"]},
                        ),
                        await client.get(
                            "/api/validation-receipts", params={"latest": "true"}
                        ),
                        await client.get(
                            "/api/validation-receipts",
                            params={"work_id": pending["job_id"]},
                        ),
                        await client.get(
                            "/api/validation-receipts",
                            params={"receipt_id": "not-a-receipt"},
                        ),
                        await client.get("/api/validation-receipts"),
                        await client.get(
                            "/api/validation-receipts", params={"latest": "yes"}
                        ),
                    )

            (
                by_receipt,
                by_work,
                latest,
                pending_response,
                malformed,
                receipt_list,
                bad_latest,
            ) = asyncio.run(fetch())
            self.assertEqual(by_receipt.status_code, 200)
            payload = by_receipt.json()
            self.assertEqual(payload["schema_version"], 5)
            self.assertEqual(payload, by_work.json())
            self.assertEqual(payload, latest.json())
            self.assertEqual(
                payload["receipt_id"], f"local-validation-receipt-{accepted['job_id']}"
            )
            self.assertEqual(payload["creator_node_id"], "creator-local-a")
            self.assertEqual(payload["executor_node_id"], "worker-local-a")
            self.assertEqual(
                payload["model_expert_id"],
                "local-runner:aethermesh-local-runner@1",
            )
            self.assertEqual(payload["manifest_ref"], accepted["manifest_ref"])
            self.assertEqual(payload["lineage_parent_ids"], ["data/prior-job.json"])
            self.assertEqual(payload["validation_status"], "passed")
            self.assertEqual(payload["validator_identity"], "worker-local-a")
            self.assertEqual(
                payload["contribution_attribution"]["metadata"],
                {"project": "prototype"},
            )
            self.assertEqual(payload["validation_scope"], "local-only-not-consensus")
            self.assertEqual(
                payload["evidence"]["receipt_ref"],
                f"data/job-validation-receipts/{accepted['job_id']}.json",
            )
            self.assertRegex(
                payload["validated_at"],
                r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$",
            )
            receipt_path = (
                Path(temp_dir)
                / "data"
                / "job-validation-receipts"
                / f"{accepted['job_id']}.json"
            )
            stored_receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            self.assertEqual(stored_receipt["executor_node_id"], "worker-local-a")
            self.assertEqual(
                stored_receipt["execution"]["executor_node_id"], "worker-local-a"
            )
            self.assertEqual(
                stored_receipt["contribution_attribution"],
                payload["contribution_attribution"],
            )
            stored_receipt["model_expert_id"] = "local-runner:other@1"
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "incomplete provenance evidence"
            ):
                service.get_local_validation_receipt(work_id=accepted["job_id"])
            stored_receipt["model_expert_id"] = payload["model_expert_id"]
            stored_receipt["execution"]["executor_node_id"] = "worker-local-other"
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "invalid executor timing evidence"
            ):
                service.get_local_validation_receipt(work_id=accepted["job_id"])
            stored_receipt["execution"]["executor_node_id"] = "worker-local-a"
            stored_receipt.pop("validated_at")
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "missing or invalid validated_at timestamp"
            ):
                service.get_local_validation_receipt(work_id=accepted["job_id"])
            self.assertEqual(pending_response.status_code, 404)
            self.assertEqual(
                pending_response.json()["error"]["code"], "VALIDATION_FAILURE"
            )
            self.assertEqual(receipt_list.status_code, 200)
            self.assertEqual(receipt_list.json()["total"], 1)
            for response in (malformed, bad_latest):
                self.assertEqual(response.status_code, 400)

            stored_receipt.pop("executor_node_id")
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "validation receipt has no executor identity"
            ):
                service.get_local_validation_receipt(work_id=accepted["job_id"])

            stored_receipt["result_ref"] = (
                "data/job-results/local-job-" + "0" * 32 + ".json"
            )
            receipt_path.write_text(json.dumps(stored_receipt), encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "validation receipt does not match its work result"
            ):
                service.get_local_validation_receipt(work_id=accepted["job_id"])

    def test_validation_receipt_list_and_detail_are_read_only(self) -> None:
        request, _expected_output = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(request)
            service.execute_submitted_local_job(
                submission["job_id"], "worker-local-fixture"
            )
            receipt_id = f"local-validation-receipt-{submission['job_id']}"
            evidence_paths = sorted((root / "data").rglob("*.json"))
            evidence_before = {path: path.read_bytes() for path in evidence_paths}
            api = create_app(service)

            async def fetch() -> tuple[httpx.Response, httpx.Response, httpx.Response]:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    return (
                        await client.get("/api/validation-receipts"),
                        await client.get(f"/api/validation-receipts/{receipt_id}"),
                        await client.get(
                            "/api/validation-receipts/"
                            "local-validation-receipt-local-job-" + "0" * 32
                        ),
                    )

            receipt_list, detail, unknown = asyncio.run(fetch())
            self.assertEqual(receipt_list.status_code, 200)
            self.assertEqual(detail.status_code, 200)
            self.assertEqual(unknown.status_code, 404)
            self.assertEqual(unknown.json()["error"]["code"], "VALIDATION_FAILURE")
            detail_payload = detail.json()
            self.assertEqual(detail_payload["receipt_id"], receipt_id)
            self.assertEqual(detail_payload["manifest_ref"], submission["manifest_ref"])
            self.assertEqual(
                detail_payload["lineage_parent_ids"], request["lineage_parent_refs"]
            )
            self.assertEqual(
                detail_payload["creator_node_id"], request["creator_node_id"]
            )
            self.assertEqual(
                detail_payload["contribution_attribution"]["metadata"],
                request["attribution_metadata"],
            )
            self.assertEqual(
                receipt_list.json(),
                {
                    "schema_version": 1,
                    "network_mode": "local-only-no-p2p",
                    "total": 1,
                    "validation_receipts": [
                        {
                            "receipt_id": receipt_id,
                            "validation_receipt_id": receipt_id,
                            "work_id": submission["job_id"],
                            "creator_node_id": request["creator_node_id"],
                            "manifest_ref": submission["manifest_ref"],
                            "lineage_parent_ids": request["lineage_parent_refs"],
                            "contribution_attribution": detail_payload[
                                "contribution_attribution"
                            ],
                            "status": "accepted",
                            "validation_status": "passed",
                            "validated_at": detail_payload["validated_at"],
                            "validation_summary": detail_payload["validation"],
                        }
                    ],
                },
            )
            self.assertEqual(
                {path: path.read_bytes() for path in evidence_paths}, evidence_before
            )

    def test_contribution_summary_is_deterministic_and_honest_about_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            self.assertEqual(
                service.contribution_summary(),
                {
                    "schema_version": 1,
                    "network_mode": "local-only-no-p2p",
                    "summary_status": "empty",
                    "current_node_identity": {
                        "node_id": None,
                    },
                    "contribution_count": 0,
                    "accepted_work_count": 0,
                    "non_accepted_work_count": 0,
                    "accepted_count": 0,
                    "rejected_count": 0,
                    "pending_count": 0,
                    "unavailable_count": 0,
                    "invalid_count": 0,
                    "latest_receipt_time": None,
                    "items": [],
                },
            )
            initialized = service.initialize_local_node_data()
            request = {
                "job_type": "echo",
                "requested_capability": {"identifier": "work.echo"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": ["data/prior-job.json"],
                "attribution_metadata": {},
            }
            accepted = service.submit_local_job(request)
            service.execute_submitted_local_job(accepted["job_id"], "worker-local-a")
            failed = service.submit_local_job(
                {
                    **request,
                    "job_type": "text_stats",
                    "requested_capability": {"identifier": "work.text_stats"},
                }
            )
            service.execute_submitted_local_job(failed["job_id"], "worker-local-b")
            queued = service.submit_local_job(request)
            missing_reference = service.submit_local_job(request)
            missing_reference_status = (
                Path(temp_dir)
                / "data"
                / "job-status"
                / f"{missing_reference['job_id']}.json"
            )
            missing_reference_status.parent.mkdir(parents=True, exist_ok=True)
            missing_reference_status.write_text(
                json.dumps(
                    {
                        "job_id": missing_reference["job_id"],
                        "status": "succeeded",
                        "validation": {
                            "passed": True,
                            "receipt_ref": (
                                "data/job-validation-receipts/"
                                f"{accepted['job_id']}.json"
                            ),
                        },
                    }
                ),
                encoding="utf-8",
            )
            missing_receipt = service.submit_local_job(request)
            service.execute_submitted_local_job(
                missing_receipt["job_id"], "worker-local-c"
            )
            (
                Path(temp_dir)
                / "data"
                / "job-validation-receipts"
                / f"{missing_receipt['job_id']}.json"
            ).unlink()
            missing_manifest = service.submit_local_job(request)
            service.execute_submitted_local_job(
                missing_manifest["job_id"], "worker-local-d"
            )
            (Path(temp_dir) / missing_manifest["manifest_ref"]).unlink()

            summary = service.contribution_summary()
            self.assertEqual(summary, service.contribution_summary())
            self.assertEqual(
                summary["current_node_identity"],
                {
                    "node_id": initialized["node_id"],
                },
            )
            self.assertEqual(summary["contribution_count"], 6)
            self.assertEqual(summary["accepted_work_count"], 1)
            self.assertEqual(summary["non_accepted_work_count"], 5)
            self.assertEqual(summary["accepted_count"], 1)
            self.assertEqual(summary["rejected_count"], 1)
            self.assertEqual(summary["pending_count"], 1)
            self.assertEqual(summary["unavailable_count"], 2)
            self.assertEqual(summary["invalid_count"], 1)
            self.assertIsNotNone(summary["latest_receipt_time"])
            items = {item["work_item_id"]: item for item in summary["items"]}
            accepted_item = items[accepted["job_id"]]
            self.assertEqual(accepted_item["acceptance_status"], "accepted")
            self.assertEqual(accepted_item["validation_status"], "accepted")
            self.assertEqual(accepted_item["creator_node_id"], "creator-local-a")
            self.assertEqual(accepted_item["contributing_node_id"], "worker-local-a")
            self.assertEqual(accepted_item["manifest_ref"], accepted["manifest_ref"])
            self.assertEqual(
                accepted_item["validation_receipt_ref"],
                f"data/job-validation-receipts/{accepted['job_id']}.json",
            )
            self.assertEqual(accepted_item["lineage_links"], ["data/prior-job.json"])
            self.assertEqual(
                accepted_item["lineage_parent_ids"], ["data/prior-job.json"]
            )
            self.assertEqual(
                accepted_item["validation_receipt_id"],
                f"local-validation-receipt-{accepted['job_id']}",
            )
            self.assertEqual(
                accepted_item["attribution_id"],
                f"local-contribution-{accepted['job_id']}",
            )
            self.assertEqual(
                accepted_item["lineage_id"],
                f"local-lineage-{accepted['job_id']}",
            )
            self.assertEqual(
                accepted_item["contribution_attribution"]["creator_node_id"],
                "creator-local-a",
            )
            self.assertIsNotNone(accepted_item["manifest_id"])
            self.assertIsInstance(accepted_item["timestamps"]["submitted_at"], int)
            self.assertEqual(
                items[failed["job_id"]]["acceptance_status"], "not_accepted"
            )
            self.assertEqual(items[failed["job_id"]]["validation_status"], "rejected")
            self.assertEqual(
                items[queued["job_id"]]["acceptance_status"], "not_accepted"
            )
            self.assertEqual(items[queued["job_id"]]["validation_status"], "pending")
            self.assertEqual(
                items[missing_reference["job_id"]]["acceptance_status"], "degraded"
            )
            self.assertEqual(
                items[missing_reference["job_id"]]["validation_status"], "invalid"
            )
            self.assertIn(
                "validation receipt reference does not match work item",
                " ".join(items[missing_reference["job_id"]]["evidence_errors"]),
            )
            self.assertEqual(
                items[missing_receipt["job_id"]]["acceptance_status"], "degraded"
            )
            self.assertEqual(
                items[missing_receipt["job_id"]]["validation_status"], "unavailable"
            )
            self.assertIn(
                "missing validation receipt",
                " ".join(items[missing_receipt["job_id"]]["evidence_errors"]),
            )
            self.assertEqual(
                items[missing_manifest["job_id"]]["acceptance_status"], "degraded"
            )
            self.assertEqual(
                items[missing_manifest["job_id"]]["validation_status"], "unavailable"
            )
            self.assertIn(
                "missing job submission manifest",
                " ".join(items[missing_manifest["job_id"]]["evidence_errors"]),
            )

            api = create_app(service)

            async def fetch() -> httpx.Response:
                transport = httpx.ASGITransport(app=api)
                async with httpx.AsyncClient(
                    transport=transport, base_url="http://testserver"
                ) as client:
                    return await client.get("/api/contributions")

            response = asyncio.run(fetch())
            self.assertEqual(response.status_code, 200)
            self.assertEqual(response.json(), summary)

    def test_contribution_summary_inspection_is_repeatable_ordered_and_preserves_evidence(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submissions = []
            for creator_node_id, worker_node_id, lineage_parent_ref in (
                ("creator-node-z", "worker-node-z", "data/lineage/z-parent.json"),
                ("creator-node-a", "worker-node-a", "data/lineage/a-parent.json"),
            ):
                submission = service.submit_local_job(
                    {
                        "job_type": "echo",
                        "requested_capability": {"identifier": "work.echo"},
                        "input_payload": {
                            "payload_type": "json",
                            "content": {"message": creator_node_id},
                        },
                        "creator_node_id": creator_node_id,
                        "requested_validation_mode": "deterministic-local",
                        "schema_version": 1,
                        "lineage_parent_refs": [lineage_parent_ref],
                        "attribution_metadata": {},
                    }
                )
                service.execute_submitted_local_job(
                    submission["job_id"], worker_node_id
                )
                submissions.append(
                    (submission, creator_node_id, worker_node_id, lineage_parent_ref)
                )

            first = service.contribution_summary()
            second = service.contribution_summary()

            self.assertEqual(
                json.dumps(first, sort_keys=True, separators=(",", ":")),
                json.dumps(second, sort_keys=True, separators=(",", ":")),
            )
            items = first["items"]
            self.assertEqual(
                [item["work_item_id"] for item in items],
                sorted(submission["job_id"] for submission, *_ in submissions),
            )
            for (
                submission,
                creator_node_id,
                worker_node_id,
                lineage_parent_ref,
            ) in submissions:
                item = next(
                    item
                    for item in items
                    if item["work_item_id"] == submission["job_id"]
                )
                self.assertEqual(item["creator_node_id"], creator_node_id)
                self.assertEqual(item["contributing_node_id"], worker_node_id)
                self.assertEqual(
                    item["contribution_attribution"]["creator_node_id"], creator_node_id
                )
                self.assertEqual(item["manifest_ref"], submission["manifest_ref"])
                self.assertIsNotNone(item["manifest_id"])
                self.assertEqual(
                    item["validation_receipt_id"],
                    f"local-validation-receipt-{submission['job_id']}",
                )
                self.assertEqual(item["lineage_links"], [lineage_parent_ref])
                self.assertEqual(item["lineage_parent_ids"], [lineage_parent_ref])
                self.assertEqual(item["acceptance_status"], "accepted")

            malformed_job_id = "local-job-malformed-entry"
            malformed_status = root / "data" / "job-status" / f"{malformed_job_id}.json"
            malformed_status.write_text("{not json", encoding="utf-8")
            after_malformed = service.contribution_summary()
            inspected_valid_items = [
                item
                for item in after_malformed["items"]
                if item["work_item_id"] != malformed_job_id
            ]
            self.assertEqual(inspected_valid_items, items)
            malformed_item = next(
                item
                for item in after_malformed["items"]
                if item["work_item_id"] == malformed_job_id
            )
            self.assertEqual(malformed_item["validation_status"], "unavailable")
            self.assertIn(
                "job status record JSON is malformed",
                " ".join(malformed_item["evidence_errors"]),
            )

    def test_contribution_summary_reports_orphaned_receipt(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            service.execute_submitted_local_job(submission["job_id"], "worker-local-a")
            (root / submission["manifest_ref"]).unlink()
            (root / "data" / "job-status" / f"{submission['job_id']}.json").unlink()

            summary = service.contribution_summary()
            item = summary["items"][0]

            self.assertEqual(summary["contribution_count"], 1)
            self.assertEqual(item["validation_status"], "unavailable")
            self.assertEqual(
                item["validation_receipt_id"],
                f"local-validation-receipt-{submission['job_id']}",
            )
            self.assertIn(
                "validation receipt has no matching job status reference",
                item["evidence_errors"],
            )

            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{submission['job_id']}.json"
            )
            receipt_path.unlink()
            status_path = root / "data" / "job-status" / f"{submission['job_id']}.json"
            status_path.write_text(
                json.dumps(
                    {
                        "job_id": submission["job_id"],
                        "status": "succeeded",
                        "validation": {},
                    }
                ),
                encoding="utf-8",
            )
            item = service.contribution_summary()["items"][0]
            self.assertIn(
                "job status record has no validation receipt reference",
                item["evidence_errors"],
            )

    def test_contribution_summary_reports_missing_status_as_unavailable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            status_path = root / "data" / "job-status" / f"{submission['job_id']}.json"
            status_path.unlink()

            item = service.contribution_summary()["items"][0]

            self.assertEqual(item["validation_status"], "unavailable")
            self.assertIn(
                f"missing job status record: {submission['job_id']}.json",
                item["evidence_errors"],
            )

    def test_contribution_summary_marks_inconsistent_receipt_evidence_invalid(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            submission = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            service.execute_submitted_local_job(submission["job_id"], "worker-local-a")
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{submission['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["validation_receipt_id"] = "invalid-receipt-id"
            receipt["validation"] = {}
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            item = service.contribution_summary()["items"][0]

            self.assertEqual(item["acceptance_status"], "degraded")
            self.assertEqual(item["validation_status"], "invalid")
            self.assertIsNone(service.contribution_summary()["latest_receipt_time"])
            self.assertIn(
                "validation receipt has invalid receipt ID", item["evidence_errors"]
            )
            self.assertIn(
                "validation receipt has invalid validation evidence",
                item["evidence_errors"],
            )
            self.assertIn(
                "job status validation does not match receipt", item["evidence_errors"]
            )
            self.assertIn(
                "validation receipt status is inconsistent", item["evidence_errors"]
            )

    def test_rejected_work_is_auditable_but_a_corrected_submission_gets_credit(
        self,
    ) -> None:
        """A correction is new lineage evidence, never a rewrite of a rejection."""

        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            rejected_request = {
                "job_type": "text_stats",
                "requested_capability": {"identifier": "work.text_stats"},
                "input_payload": {
                    "payload_type": "json",
                    "content": {"message": "hello"},
                },
                "creator_node_id": "creator-local-a",
                "requested_validation_mode": "deterministic-local",
                "schema_version": 1,
                "lineage_parent_refs": [],
                "attribution_metadata": {"source": "correction-test"},
            }
            rejected = service.submit_local_job(rejected_request)
            service.execute_submitted_local_job(rejected["job_id"], "worker-local-a")

            corrected = service.submit_local_job(
                {
                    **rejected_request,
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "lineage_parent_refs": [rejected["manifest_ref"]],
                }
            )
            service.execute_submitted_local_job(corrected["job_id"], "worker-local-a")

            summary = service.contribution_summary()
            items = {item["work_item_id"]: item for item in summary["items"]}
            self.assertEqual(summary["accepted_work_count"], 1)
            self.assertEqual(summary["non_accepted_work_count"], 1)
            self.assertEqual(
                items[rejected["job_id"]]["acceptance_status"], "not_accepted"
            )
            self.assertEqual(
                items[corrected["job_id"]]["acceptance_status"], "accepted"
            )
            self.assertEqual(
                items[corrected["job_id"]]["lineage_links"], [rejected["manifest_ref"]]
            )

            rejected_receipt = service.get_local_validation_receipt(
                work_id=rejected["job_id"]
            )
            corrected_receipt = service.get_local_validation_receipt(
                work_id=corrected["job_id"]
            )
            self.assertEqual(rejected_receipt["status"], "rejected")
            self.assertEqual(rejected_receipt["creator_node_id"], "creator-local-a")
            self.assertEqual(rejected_receipt["manifest_ref"], rejected["manifest_ref"])
            self.assertEqual(rejected_receipt["lineage_parent_ids"], [])
            self.assertEqual(
                rejected_receipt["contribution_attribution"]["metadata"],
                {"source": "correction-test"},
            )
            self.assertEqual(corrected_receipt["status"], "accepted")
            self.assertNotEqual(
                rejected_receipt["validation_receipt_id"],
                corrected_receipt["validation_receipt_id"],
            )
            self.assertTrue(
                (root / rejected_receipt["manifest_ref"]).exists(),
                "the rejected work manifest remains available for audit",
            )

    def test_contribution_summary_flags_invalid_validation_timestamp(self) -> None:
        request, _ = _valid_local_work_fixture()
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            service = NodeRuntimeService.from_home(root)
            evidence = self._execute_fixed_deterministic_fixture(root, request)
            receipt_path = (
                root
                / "data"
                / "job-validation-receipts"
                / f"{evidence['submission']['job_id']}.json"
            )
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
            receipt["validated_at"] = "not-a-timestamp"
            receipt_path.write_text(json.dumps(receipt), encoding="utf-8")

            summary = service.contribution_summary()

            self.assertEqual(summary["accepted_work_count"], 0)
            item = summary["items"][0]
            self.assertEqual(item["acceptance_status"], "degraded")
            self.assertIn(
                "missing or invalid validated_at timestamp",
                " ".join(item["evidence_errors"]),
            )

    def test_contribution_summary_rejects_mismatched_evidence(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            accepted = service.submit_local_job(
                {
                    "job_type": "echo",
                    "requested_capability": {"identifier": "work.echo"},
                    "input_payload": {
                        "payload_type": "json",
                        "content": {"message": "hello"},
                    },
                    "creator_node_id": "creator-local-a",
                    "requested_validation_mode": "deterministic-local",
                    "schema_version": 1,
                    "lineage_parent_refs": [],
                    "attribution_metadata": {},
                }
            )
            job_id = accepted["job_id"]
            service.execute_submitted_local_job(job_id, "worker-local-a")
            manifest_path = Path(temp_dir) / accepted["manifest_ref"]
            status_path = Path(temp_dir) / "data" / "job-status" / f"{job_id}.json"
            receipt_path = (
                Path(temp_dir) / "data" / "job-validation-receipts" / f"{job_id}.json"
            )
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            status = json.loads(status_path.read_text(encoding="utf-8"))
            receipt = json.loads(receipt_path.read_text(encoding="utf-8"))

            for path, document, expected_error in (
                (
                    status_path,
                    {**status, "validation": {"passed": True}},
                    "job status record has no validation receipt reference",
                ),
                (
                    manifest_path,
                    {**manifest, "job": {**manifest["job"], "job_id": "other"}},
                    "job submission manifest does not match work item",
                ),
                (
                    status_path,
                    {**status, "job_id": "other"},
                    "job status record does not match work item",
                ),
                (
                    receipt_path,
                    {**receipt, "job_id": "other"},
                    "validation receipt does not match work item",
                ),
                (
                    receipt_path,
                    {**receipt, "result_ref": "data/job-results/other.json"},
                    "validation receipt does not match work result",
                ),
                (
                    manifest_path,
                    {**manifest, "job": []},
                    "job submission manifest has invalid job evidence",
                ),
                (
                    status_path,
                    {**status, "contribution_attribution": {}},
                    "contribution attribution has no creator node ID",
                ),
                (
                    status_path,
                    {
                        **status,
                        "contribution_attribution": {
                            **status["contribution_attribution"],
                            "job_id": "other",
                        },
                    },
                    "contribution attribution does not match work item",
                ),
                (
                    status_path,
                    {
                        **status,
                        "contribution_attribution": {
                            **status["contribution_attribution"],
                            "creator_node_id": "other",
                        },
                    },
                    "contribution attribution creator does not match manifest",
                ),
                (
                    status_path,
                    {
                        **status,
                        "worker_node_id": None,
                        "contribution_attribution": {
                            "creator_node_id": "creator-local-a"
                        },
                    },
                    "contribution attribution has no contributing node ID",
                ),
                (
                    status_path,
                    {
                        **status,
                        "contribution_attribution": {
                            **status["contribution_attribution"],
                            "worker_node_id": "other",
                        },
                    },
                    "contribution attribution worker does not match job status",
                ),
                (
                    receipt_path,
                    {**receipt, "version": 1},
                    "validation receipt has unsupported version",
                ),
                (
                    receipt_path,
                    {
                        key: value
                        for key, value in receipt.items()
                        if key != "validation_method"
                    },
                    "validation receipt has no validation method",
                ),
                (
                    receipt_path,
                    {**receipt, "validator_id": "other"},
                    "validation receipt validator does not match worker",
                ),
                (
                    manifest_path,
                    {**manifest, "lineage": []},
                    "job submission manifest has invalid lineage evidence",
                ),
                (
                    manifest_path,
                    {**manifest, "lineage": {"job_id": "other", "parent_refs": []}},
                    "job submission lineage does not match work item",
                ),
                (
                    manifest_path,
                    {**manifest, "lineage": {"parent_refs": "not-a-list"}},
                    "job submission manifest has invalid lineage links",
                ),
            ):
                manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
                status_path.write_text(json.dumps(status), encoding="utf-8")
                receipt_path.write_text(json.dumps(receipt), encoding="utf-8")
                path.write_text(json.dumps(document), encoding="utf-8")
                item = service.contribution_summary()["items"][0]
                self.assertEqual(item["acceptance_status"], "degraded")
                self.assertIn(expected_error, item["evidence_errors"])

    def test_init_creates_reusable_local_node_data_and_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))

            initialized = service.initialize_local_node_data()
            status = service.get_node_status()

            self.assertEqual(
                set(initialized),
                {
                    "initialized",
                    "node_id",
                    "node_name",
                    "home",
                    "config_path",
                    "data_dir",
                    "log_dir",
                    "identity_path",
                    "identity_persisted",
                },
            )
            self.assertTrue(initialized["initialized"])
            self.assertEqual(initialized["home"], str(Path(temp_dir)))
            self.assertEqual(
                initialized["config_path"], str(Path(temp_dir) / "config.json")
            )
            self.assertEqual(initialized["data_dir"], str(Path(temp_dir) / "data"))
            self.assertEqual(initialized["log_dir"], str(Path(temp_dir) / "logs"))
            self.assertEqual(
                initialized["identity_path"], str(Path(temp_dir) / "identity.json")
            )
            self.assertFalse(initialized["identity_persisted"])
            self.assertFalse(Path(initialized["identity_path"]).exists())
            self.assertEqual(
                service.recent_logs(limit=1)["events"][0].split(" ", 1)[1],
                "initialized local node data",
            )
            self.assertEqual(
                set(status),
                {
                    "initialized",
                    "node_id",
                    "node_name",
                    "status",
                    "version",
                    "uptime_seconds",
                    "pid",
                    "config_path",
                    "data_dir",
                    "log_dir",
                    "api",
                    "peer_count",
                    "job_counts",
                    "capabilities",
                    "package",
                    "network_health",
                    "system",
                },
            )
            self.assertTrue(status["initialized"])
            self.assertRegex(
                str(status["node_name"]),
                r"^[a-z]+-[a-z]+-[a-z]+-[a-z]+_[a-f0-9]{6}$",
            )
            self.assertEqual(initialized["node_name"], status["node_name"])
            self.assertEqual(
                str(status["node_name"]).rsplit("_", 1)[1], status["node_id"][:6]
            )
            self.assertEqual(status["status"], "stopped")
            self.assertIsNone(status["uptime_seconds"])
            self.assertIsNone(status["pid"])
            self.assertEqual(status["api"]["host"], "127.0.0.1")
            self.assertEqual(status["api"]["port"], 7280)
            self.assertTrue(status["api"]["localhost_only"])
            self.assertEqual(status["peer_count"], 0)
            self.assertEqual(
                status["job_counts"], {"current": 0, "completed": 0, "failed": 0}
            )
            self.assertEqual(
                set(status["system"]),
                {
                    "platform",
                    "python_version",
                    "cpu_count",
                    "processor",
                    "memory_total_bytes",
                    "disk_data_path_total_bytes",
                    "disk_data_path_free_bytes",
                },
            )

            config = json.loads((Path(temp_dir) / "config.json").read_text())
            self.assertEqual(config["version"], 1)
            self.assertEqual(config["node"]["node_id"], status["node_id"])
            self.assertEqual(config["node"]["node_name"], status["node_name"])
            self.assertEqual(config["node"]["status"], "local_only")
            self.assertEqual(config["paths"]["home"], str(Path(temp_dir)))
            self.assertEqual(config["paths"]["data_dir"], str(Path(temp_dir) / "data"))
            self.assertEqual(config["paths"]["log_dir"], str(Path(temp_dir) / "logs"))
            self.assertEqual(config["api"], {"host": "127.0.0.1", "port": 7280})
            self.assertEqual(
                config["identity"],
                {"persist": False, "path": str(Path(temp_dir) / "identity.json")},
            )
            self.assertEqual(
                config["capabilities"]["enabled_work_types"],
                [
                    "echo",
                    "hash",
                    "basic_compute",
                    "schema_transform",
                    "keyword_extract",
                    "text_chunk",
                    "text_embed",
                    "text_stats",
                ],
            )

            with patch.object(
                service, "_runtime_marker", return_value=(False, None, int(time.time()))
            ):
                self.assertIsNone(service.get_node_status()["uptime_seconds"])

    def test_identity_persistence_only_when_configured(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.paths.home.mkdir(parents=True, exist_ok=True)
            persistent_config = service._default_config(node_id=None)
            persistent_config["identity"] = {
                "persist": True,
                "path": "identity.json",
            }
            service._write_config(persistent_config)

            first = service.initialize_local_node_data()
            persisted_document = json.loads(service.paths.identity_path.read_text())
            second = service.initialize_local_node_data()

            self.assertTrue(first["identity_persisted"])
            self.assertTrue(service.paths.identity_path.exists())
            self.assertEqual(first["identity_path"], str(service.paths.identity_path))
            self.assertEqual(first["node_id"], second["node_id"])
            self.assertEqual(
                persisted_document["node"]["creator_node_id"], first["node_id"]
            )
            self.assertEqual(persisted_document["references"]["manifest_refs"], [])
            self.assertEqual(
                persisted_document["references"]["validation_receipt_refs"], []
            )
            self.assertIn("version_metadata", persisted_document["references"])
            self.assertEqual(
                persisted_document["lineage"],
                {"parent_node_ids": [], "lineage_links": []},
            )
            self.assertEqual(
                persisted_document["contribution_attribution"],
                {
                    "creator_node_id": first["node_id"],
                    "attribution_node_id": first["node_id"],
                    "contribution_refs": [],
                },
            )

        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            first = service.initialize_local_node_data()
            second = service.initialize_local_node_data()

            self.assertFalse(first["identity_persisted"])
            self.assertFalse(service.paths.identity_path.exists())
            self.assertEqual(first["node_id"], second["node_id"])

    def test_persisted_identity_manifest_survives_restart_without_rewrite(self) -> None:
        first_node_id = deterministic_machine_node_id()
        with tempfile.TemporaryDirectory() as temp_dir:
            home = Path(temp_dir) / "persisted-node"
            service = NodeRuntimeService.from_home(home)
            service.paths.home.mkdir(parents=True, exist_ok=True)
            persistent_config = service._default_config(node_id=None)
            persistent_config["identity"] = {"persist": True, "path": "identity.json"}
            service._write_config(persistent_config)

            with patch(
                "aethermesh_core.identity._new_local_node_id",
                return_value=first_node_id,
            ):
                first = service.initialize_local_node_data()
                service.mark_runtime_started()
            persisted_document = json.loads(service.paths.identity_path.read_text())
            persisted_document["references"]["manifest_refs"] = [
                f"manifests/local-batch.json#node:{first_node_id}"
            ]
            persisted_document["references"]["validation_receipt_refs"] = [
                "receipts/validation-0001.json"
            ]
            persisted_document["lineage"]["parent_node_ids"] = ["local-root"]
            persisted_document["lineage"]["lineage_links"] = [
                "lineage/local-node-link.json"
            ]
            persisted_document["contribution_attribution"]["contribution_refs"] = [
                "contributions/contribution-0001.json"
            ]
            service.paths.identity_path.write_text(
                json.dumps(persisted_document), encoding="utf-8"
            )
            manifest_after_first_start = json.loads(
                service.paths.identity_path.read_text(encoding="utf-8")
            )
            service.mark_runtime_stopped()

            restarted_service = NodeRuntimeService.from_home(home)
            with patch(
                "aethermesh_core.identity._new_local_node_id",
                side_effect=AssertionError("restart must reuse persisted identity"),
            ):
                restarted = restarted_service.initialize_local_node_data()
                restarted_status = restarted_service.start_node_runtime()
            manifest_after_restart = json.loads(
                restarted_service.paths.identity_path.read_text(encoding="utf-8")
            )
            restarted_service.mark_runtime_stopped()

            fresh_home = Path(temp_dir) / "fresh-node"
            fresh_service = NodeRuntimeService.from_home(fresh_home)
            fresh_service.paths.home.mkdir(parents=True, exist_ok=True)
            fresh_config = fresh_service._default_config(node_id=None)
            fresh_config["identity"] = {"persist": True, "path": "identity.json"}
            fresh_service._write_config(fresh_config)
            with patch(
                "aethermesh_core.identity._new_local_node_id",
                side_effect=AssertionError(
                    "fresh persisted identity must be deterministic"
                ),
            ):
                fresh = fresh_service.initialize_local_node_data()
            fresh_manifest = json.loads(
                fresh_service.paths.identity_path.read_text(encoding="utf-8")
            )

        self.assertEqual(first["node_id"], first_node_id)
        self.assertEqual(restarted["node_id"], first_node_id)
        self.assertEqual(restarted_status["node_id"], first_node_id)
        self.assertEqual(manifest_after_restart, manifest_after_first_start)
        self.assertEqual(
            manifest_after_restart["node"]["creator_node_id"], first_node_id
        )
        self.assertEqual(
            manifest_after_restart["contribution_attribution"]["creator_node_id"],
            first_node_id,
        )
        self.assertEqual(
            manifest_after_restart["contribution_attribution"]["attribution_node_id"],
            first_node_id,
        )
        self.assertEqual(
            manifest_after_restart["references"]["manifest_refs"],
            [f"manifests/local-batch.json#node:{first_node_id}"],
        )
        self.assertEqual(
            manifest_after_restart["references"]["validation_receipt_refs"],
            ["receipts/validation-0001.json"],
        )
        self.assertEqual(
            manifest_after_restart["lineage"],
            {
                "parent_node_ids": ["local-root"],
                "lineage_links": ["lineage/local-node-link.json"],
            },
        )
        self.assertEqual(
            manifest_after_restart["contribution_attribution"]["contribution_refs"],
            ["contributions/contribution-0001.json"],
        )
        self.assertEqual(fresh["node_id"], first_node_id)
        self.assertEqual(fresh["node_id"], first["node_id"])
        self.assertEqual(fresh_manifest["node"]["creator_node_id"], first_node_id)

    def test_peers_jobs_and_health_are_honest_when_node_has_no_runtime_work(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.initialize_local_node_data()

            self.assertEqual(
                service.list_peers(),
                {
                    "bootstrap_status": "not_configured",
                    "peer_count": 0,
                    "peers": [],
                    "note": "No peer discovery source is configured for the local daemon yet.",
                },
            )
            capabilities = service.list_capabilities()
            self.assertEqual(capabilities["schema_version"], 1)
            self.assertEqual(capabilities["network_mode"], "local-only-no-p2p")
            self.assertFalse(capabilities["advertised"])
            self.assertTrue(
                all(
                    {"identifier", "description", "status", "schema_version"}
                    <= capability.keys()
                    for capability in capabilities["capabilities"]
                )
            )
            self.assertEqual(
                {
                    capability["identifier"]: capability["status"]
                    for capability in capabilities["capabilities"]
                },
                {
                    "work.echo": "enabled",
                    "work.hash": "enabled",
                    "work.basic_compute": "enabled",
                    "work.schema_transform": "enabled",
                    "work.keyword_extract": "enabled",
                    "work.text_chunk": "enabled",
                    "work.text_embed": "enabled",
                    "work.text_stats": "enabled",
                    "provenance.creator_node_id": "disabled",
                    "provenance.manifest": "enabled",
                    "provenance.validation_receipt": "enabled",
                    "provenance.lineage_reference": "enabled",
                    "provenance.contribution_attribution": "enabled",
                    "provenance.end_to_end_runtime_lineage": "disabled",
                },
            )
            self.assertEqual(
                service.network_health(),
                {
                    "status": "local_only",
                    "peer_count": 0,
                    "api_reachable": True,
                    "localhost_only": True,
                    "note": "Public peer networking is not configured for this local prototype.",
                },
            )
            package = service.package_info()
            self.assertEqual(package["name"], "aethermesh")
            self.assertIn("version", package)
            self.assertEqual(package["source"], "installed")
            self.assertEqual(
                service.list_jobs(),
                {
                    "current": [],
                    "completed": [],
                    "failed": [],
                    "validation_status": "not_active",
                    "note": "No persistent daemon job queue is active yet.",
                },
            )
            self.assertEqual(service.health()["ok"], True)
            self.assertEqual(service.health()["bind_host"], "127.0.0.1")

    def test_capability_listing_uses_configured_work_type_enablement(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.initialize_local_node_data()
            config = service.load_config()
            config["capabilities"] = {"enabled_work_types": ["echo"]}
            service._write_config(config)

            capabilities = {
                entry["identifier"]: entry["status"]
                for entry in service.list_capabilities()["capabilities"]
            }

            self.assertEqual(capabilities["work.echo"], "enabled")
            self.assertEqual(capabilities["work.text_stats"], "disabled")
            self.assertEqual(capabilities["provenance.creator_node_id"], "disabled")
            self.assertEqual(capabilities["provenance.manifest"], "enabled")

            config["identity"]["persist"] = True
            service._write_config(config)
            persistent_capabilities = {
                entry["identifier"]: entry["status"]
                for entry in service.list_capabilities()["capabilities"]
            }
            self.assertEqual(
                persistent_capabilities["provenance.creator_node_id"], "enabled"
            )

    def test_capability_availability_is_local_validation_gated_and_deterministic(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            initialized = service.initialize_local_node_data()

            first = service.list_capabilities()
            second = service.list_capabilities()
            echo = next(
                entry
                for entry in first["capabilities"]
                if entry["identifier"] == "work.echo"
            )

            self.assertEqual(first, second)
            self.assertNotIn("resource_hints", echo)
            self.assertEqual(echo["creator_node_id"], initialized["node_id"])
            self.assertEqual(
                echo["capability_manifest_id"], "local-capability-work-echo-v1"
            )
            self.assertEqual(
                echo["lineage"],
                {"capability_manifest_id": echo["capability_manifest_id"]},
            )
            self.assertEqual(
                echo["contribution_attribution"],
                {"creator_node_id": initialized["node_id"]},
            )
            self.assertEqual(
                echo["availability"],
                {
                    "status": "available",
                    "reason": None,
                    "worker_capacity": {"current": 0, "maximum": 1},
                    "validation_receipt_refs": [],
                },
            )

    def test_capability_resource_hints_are_advisory_and_preserve_provenance(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            initialized = service.initialize_local_node_data()
            config = service.load_config()
            config["capabilities"]["resource_hints"] = {
                "work.echo": {
                    "cpu_class": "general-purpose CPU",
                    "ram_range": "256 MiB to 512 MiB",
                    "disk_needs": "minimal local scratch space",
                    "expected_duration": "usually under one second",
                    "network_sensitivity": "offline-safe",
                    "accelerator_type": "none required",
                    "energy_profile": "low local energy use",
                    "operator_cost_label": "operator-local-low",
                    "operator_notes": "advisory estimate; verify locally before routing",
                }
            }
            service._write_config(config)

            echo = next(
                entry
                for entry in service.list_capabilities()["capabilities"]
                if entry["identifier"] == "work.echo"
            )

            self.assertEqual(
                echo["resource_hints"],
                config["capabilities"]["resource_hints"]["work.echo"],
            )
            self.assertEqual(echo["creator_node_id"], initialized["node_id"])
            self.assertEqual(
                echo["lineage"],
                {"capability_manifest_id": echo["capability_manifest_id"]},
            )
            self.assertEqual(echo["availability"]["validation_receipt_refs"], [])
            self.assertEqual(
                echo["contribution_attribution"],
                {"creator_node_id": initialized["node_id"]},
            )

    def test_capability_resource_hints_reject_economic_metadata(self) -> None:
        for hints in [
            {"token_price": "one"},
            {"operator_cost_label": "token reward for each job"},
            {"operator_notes": "payments and pricing are negotiated elsewhere"},
        ]:
            with (
                self.subTest(hints=hints),
                self.assertRaisesRegex(
                    RuntimeServiceError, "advisory field|token economics"
                ),
            ):
                _config_capability_resource_hints(
                    {"capabilities": {"resource_hints": {"work.echo": hints}}}
                )

        self.assertEqual(
            _config_capability_resource_hints(
                {
                    "capabilities": {
                        "resource_hints": {
                            "work.echo": {
                                "operator_notes": "mistake-resistant local estimate"
                            }
                        }
                    }
                }
            ),
            {"work.echo": {"operator_notes": "mistake-resistant local estimate"}},
        )

    def test_capability_resource_hints_validate_registered_text_metadata(self) -> None:
        invalid_configs = [
            {"capabilities": []},
            {"capabilities": {"resource_hints": {"work.unknown": {}}}},
            {"capabilities": {"resource_hints": {"work.echo": []}}},
            {"capabilities": {"resource_hints": {"work.echo": {"cpu_class": ""}}}},
        ]
        for config in invalid_configs:
            with (
                self.subTest(config=config),
                self.assertRaises(RuntimeServiceError),
            ):
                _config_capability_resource_hints(config)

    def test_capability_availability_reports_local_failure_and_capacity(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.initialize_local_node_data()
            config = service.load_config()
            config["capabilities"] = {"enabled_work_types": ["echo"]}
            service._write_config(config)

            unavailable = next(
                entry
                for entry in service.list_capabilities()["capabilities"]
                if entry["identifier"] == "work.text_stats"
            )
            self.assertEqual(unavailable["availability"]["status"], "unavailable")
            self.assertEqual(
                unavailable["availability"]["reason"],
                "disabled in local configuration",
            )

            with patch(
                "aethermesh_core.runtime_service.validate_job_result",
                return_value=types.SimpleNamespace(valid=False),
            ):
                degraded = next(
                    entry
                    for entry in service.list_capabilities()["capabilities"]
                    if entry["identifier"] == "work.echo"
                )
            self.assertEqual(degraded["availability"]["status"], "degraded")
            self.assertEqual(
                degraded["availability"]["reason"],
                "local capability validation failed",
            )

            with patch(
                "aethermesh_core.runtime_service.LocalRunner.run",
                side_effect=ValueError("private path"),
            ):
                failed_check = next(
                    entry
                    for entry in service.list_capabilities()["capabilities"]
                    if entry["identifier"] == "work.echo"
                )
            self.assertEqual(failed_check["availability"]["status"], "degraded")
            self.assertEqual(
                failed_check["availability"]["reason"],
                "local capability validation failed",
            )

            with patch.object(service, "list_jobs", return_value={"current": [{}]}):
                busy = next(
                    entry
                    for entry in service.list_capabilities()["capabilities"]
                    if entry["identifier"] == "work.echo"
                )
            self.assertEqual(busy["availability"]["status"], "busy")
            self.assertEqual(
                busy["availability"]["worker_capacity"], {"current": 1, "maximum": 1}
            )
            self.assertEqual(busy["creator_node_id"], degraded["creator_node_id"])
            self.assertEqual(
                busy["capability_manifest_id"], degraded["capability_manifest_id"]
            )

    def test_existing_config_is_merged_and_logs_are_limited(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.paths.home.mkdir(parents=True, exist_ok=True)
            service.paths.log_dir.mkdir(parents=True)
            service.paths.config_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "node": {"node_id": "stale-node", "label": "dev"},
                        "api": {"host": "localhost", "port": 9999},
                        "custom": "kept",
                    }
                ),
                encoding="utf-8",
            )
            service.paths.events_path.write_text("one\ntwo\n", encoding="utf-8")

            result = service.initialize_local_node_data()

            config = service.load_config()
            self.assertEqual(config["custom"], "kept")
            self.assertEqual(config["node"]["label"], "dev")
            self.assertEqual(config["node"]["node_id"], result["node_id"])
            self.assertEqual(config["node"]["node_name"], result["node_name"])
            self.assertEqual(config["api"], {"host": "localhost", "port": 9999})
            self.assertEqual(len(service.recent_logs(limit=2)["events"]), 2)

            service.paths.events_path.write_text(
                "\n".join(f"event-{index}" for index in range(101)) + "\n",
                encoding="utf-8",
            )
            default_logs = service.recent_logs()["events"]
            self.assertEqual(len(default_logs), 100)
            self.assertEqual(default_logs[0], "event-1")
            self.assertEqual(default_logs[-1], "event-100")

    def test_runtime_markers_cover_started_stopped_and_bad_marker_states(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))

            status = service.start_node_runtime()
            self.assertEqual(status["status"], "running")
            self.assertIsInstance(status["uptime_seconds"], int)
            self.assertTrue(service.paths.pid_path.exists())
            restarted = service.start_node_runtime()
            self.assertEqual(restarted["status"], "running")

            service.mark_runtime_stopped()
            self.assertFalse(service.paths.pid_path.exists())
            service.mark_runtime_stopped()

            service.paths.pid_path.write_text("not json", encoding="utf-8")
            self.assertEqual(service._runtime_marker(), (False, None, None))
            service.paths.pid_path.write_text(
                json.dumps({"version": 1, "pid": True}), encoding="utf-8"
            )
            self.assertEqual(service._runtime_marker(), (False, None, None))
            service.paths.pid_path.write_text(
                json.dumps({"version": 1, "pid": 999999999}), encoding="utf-8"
            )
            self.assertEqual(service._runtime_marker(), (False, 999999999, None))

        with tempfile.TemporaryDirectory() as temp_dir:
            nested_home = Path(temp_dir) / "nested" / "runtime"
            service = NodeRuntimeService.from_home(nested_home)
            initialized = service.initialize_local_node_data()
            self.assertEqual(initialized["home"], str(nested_home))
            self.assertTrue(service.paths.config_path.exists())
            self.assertFalse(service.paths.identity_path.exists())

        with tempfile.TemporaryDirectory() as temp_dir:
            nested_home = Path(temp_dir) / "nested" / "runtime"
            service = NodeRuntimeService.from_home(nested_home)
            service.mark_runtime_started()
            self.assertTrue(service.paths.pid_path.exists())

        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.paths.home.mkdir(parents=True, exist_ok=True)
            service._write_config(service._default_config(node_id="orphaned-config"))
            self.assertFalse(service.paths.identity_path.exists())
            status = service.start_node_runtime()
            self.assertEqual(status["status"], "running")
            self.assertFalse(service.paths.identity_path.exists())

    def test_config_errors_and_defaults_are_explicit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            self.assertEqual(service.recent_logs(), {"events": []})
            service.paths.home.mkdir(parents=True, exist_ok=True)
            service.paths.config_path.write_text("[]", encoding="utf-8")
            with self.assertRaisesRegex(
                RuntimeServiceError, "config JSON must be an object"
            ):
                service.load_config()
            service.paths.config_path.write_text(
                json.dumps({"version": 2}), encoding="utf-8"
            )
            with self.assertRaisesRegex(RuntimeServiceError, "version 1"):
                service.load_config()

        self.assertIsNone(_config_node_id({"node": []}))
        self.assertEqual(_config_node_id({"node": {"node_id": "node-a"}}), "node-a")
        self.assertIsNone(_config_node_id({"node": {"node_id": ""}}))
        self.assertIsNone(_config_node_name({"node": []}))
        self.assertEqual(
            _config_node_name(
                {"node": {"node_name": "lucid-beacon-tensor-vault_bd0e94"}}
            ),
            "lucid-beacon-tensor-vault_bd0e94",
        )
        self.assertIsNone(_config_node_name({"node": {"node_name": ""}}))
        self.assertEqual(_config_api_host({"api": {"host": 7}}), "127.0.0.1")
        self.assertEqual(_config_api_host({"api": {"host": "localhost"}}), "localhost")
        self.assertEqual(_config_api_host({}), "127.0.0.1")
        self.assertEqual(_config_api_port({"api": {"port": True}}), 7280)
        self.assertEqual(_config_api_port({"api": {"port": 9999}}), 9999)
        self.assertEqual(_config_api_port({}), 7280)
        self.assertEqual(
            _config_enabled_work_types({}),
            {
                "echo",
                "hash",
                "basic_compute",
                "schema_transform",
                "keyword_extract",
                "text_chunk",
                "text_embed",
                "text_stats",
            },
        )
        self.assertEqual(
            _config_enabled_work_types({"capabilities": {}}),
            {
                "echo",
                "hash",
                "basic_compute",
                "schema_transform",
                "keyword_extract",
                "text_chunk",
                "text_embed",
                "text_stats",
            },
        )
        self.assertEqual(
            _config_enabled_work_types(
                {"capabilities": {"enabled_work_types": ["echo"]}}
            ),
            {"echo"},
        )
        with self.assertRaisesRegex(
            RuntimeServiceError, "capabilities' must be an object"
        ):
            _config_enabled_work_types({"capabilities": []})
        with self.assertRaisesRegex(RuntimeServiceError, "enabled_work_types"):
            _config_enabled_work_types({"capabilities": {"enabled_work_types": [""]}})
        self.assertEqual(_config_capability_resource_hints({}), {})
        with self.assertRaisesRegex(RuntimeServiceError, "resource_hints"):
            _config_capability_resource_hints({"capabilities": {"resource_hints": []}})
        self.assertFalse(_config_identity_persistence_enabled({}))
        self.assertTrue(
            _config_identity_persistence_enabled({"identity": {"persist": True}})
        )
        with self.assertRaisesRegex(RuntimeServiceError, "identity.persist"):
            _config_identity_persistence_enabled({"identity": {"persist": "yes"}})
        self.assertEqual(
            _config_identity_path({"identity": {}}, Path("home/id.json")),
            Path("home/id.json"),
        )
        self.assertEqual(
            _config_identity_path(
                {"identity": {"path": "identity.json"}}, Path("home/id.json")
            ),
            Path("home/identity.json"),
        )
        with self.assertRaisesRegex(RuntimeServiceError, "identity.path"):
            _config_identity_path({"identity": {"path": ""}}, Path("home/id.json"))
        self.assertEqual(
            _merge_config({"a": {"b": 2}, "c": 3}, {"a": {"d": 4}, "c": 0}),
            {"a": {"b": 2, "d": 4}, "c": 3},
        )

    def test_environment_and_platform_fallback_helpers(self) -> None:
        with (
            tempfile.TemporaryDirectory() as temp_dir,
            patch.dict(os.environ, {"AETHERMESH_HOME": temp_dir}),
        ):
            self.assertEqual(_default_home(), Path(temp_dir))
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(_default_home(), Path.home() / ".aethermesh")

        self.assertEqual(_package_version(), "0.2.0-alpha")

        sysconf_calls: list[str] = []

        def fake_sysconf(name: str) -> int:
            sysconf_calls.append(name)
            return {"SC_PAGE_SIZE": 4096, "SC_PHYS_PAGES": 12345}[name]

        with (
            patch(
                "aethermesh_core.runtime_service.os.sysconf_names",
                {"SC_PAGE_SIZE": 1, "SC_PHYS_PAGES": 2},
            ),
            patch(
                "aethermesh_core.runtime_service.os.sysconf", side_effect=fake_sysconf
            ),
        ):
            self.assertEqual(_memory_total_bytes(), 4096 * 12345)
        self.assertEqual(sysconf_calls, ["SC_PAGE_SIZE", "SC_PHYS_PAGES"])

        for partial_names in [
            {"SC_PAGE_SIZE": 1},
            {"SC_PHYS_PAGES": 2},
        ]:
            with (
                self.subTest(partial_names=partial_names),
                patch(
                    "aethermesh_core.runtime_service.os.sysconf_names",
                    partial_names,
                ),
                patch("aethermesh_core.runtime_service.os.sysconf") as sysconf,
            ):
                self.assertIsNone(_memory_total_bytes())
                sysconf.assert_not_called()

        with patch(
            "aethermesh_core.runtime_service.os.kill", side_effect=PermissionError
        ):
            self.assertTrue(_pid_is_alive(123))
        with patch(
            "aethermesh_core.runtime_service.os.kill", side_effect=ProcessLookupError
        ):
            self.assertFalse(_pid_is_alive(123))
        with patch("aethermesh_core.runtime_service.os.sysconf", side_effect=OSError):
            self.assertIsNone(_memory_total_bytes())
        with patch("aethermesh_core.runtime_service.os.sysconf_names", {}):
            self.assertIsNone(_memory_total_bytes())
        with patch("aethermesh_core.runtime_service.os", types.SimpleNamespace()):
            self.assertIsNone(_memory_total_bytes())


class ApiTests(unittest.TestCase):
    def test_local_api_routes_return_service_status(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(Path(temp_dir))
            service.initialize_local_node_data()

            payloads = asyncio.run(_fetch_api_payloads(create_app(service)))

            self.assertEqual(payloads["health"]["ok"], True)
            status_without_dynamic_system = dict(payloads["status"])
            status_alias_without_dynamic_system = dict(payloads["status_alias"])
            status_without_dynamic_system.pop("system")
            status_alias_without_dynamic_system.pop("system")
            self.assertEqual(
                status_alias_without_dynamic_system, status_without_dynamic_system
            )
            self.assertEqual(
                set(payloads["status_alias"]["system"]),
                set(payloads["status"]["system"]),
            )
            self.assertEqual(
                payloads["version_alias"]["version"], payloads["status"]["version"]
            )
            service_status = service.get_node_status()
            self.assertEqual(payloads["status"]["node_id"], service_status["node_id"])
            self.assertEqual(
                payloads["status"]["node_name"], service_status["node_name"]
            )
            self.assertEqual(
                payloads["status_alias"]["node_name"], service_status["node_name"]
            )
            node_without_dynamic_system = dict(payloads["node"])
            node_alias_without_dynamic_system = dict(payloads["node_alias"])
            node_without_dynamic_system.pop("system")
            node_alias_without_dynamic_system.pop("system")
            self.assertEqual(
                node_alias_without_dynamic_system, node_without_dynamic_system
            )
            self.assertEqual(
                set(payloads["node_alias"]["system"]), set(payloads["node"]["system"])
            )
            self.assertEqual(payloads["node"]["node_id"], service_status["node_id"])
            self.assertEqual(payloads["node"]["node_name"], service_status["node_name"])
            self.assertRegex(
                str(payloads["node"]["node_name"]),
                r"^[a-z]+-[a-z]+-[a-z]+-[a-z]+_[a-f0-9]{6}$",
            )
            self.assertEqual(
                str(payloads["node"]["node_name"]).rsplit("_", 1)[1],
                payloads["node"]["node_id"][:6],
            )
            self.assertEqual(payloads["node"]["status"], "stopped")
            self.assertEqual(payloads["peers"], payloads["peers_alias"])
            self.assertEqual(payloads["peers"]["peers"], [])
            self.assertEqual(payloads["jobs"]["current"], [])
            self.assertEqual(payloads["capabilities"], payloads["capabilities_alias"])
            self.assertEqual(payloads["capabilities"]["schema_version"], 1)
            self.assertEqual(
                payloads["capabilities"]["network_mode"], "local-only-no-p2p"
            )
            self.assertTrue(
                all(
                    {"identifier", "description", "status", "schema_version"}
                    <= capability.keys()
                    for capability in payloads["capabilities"]["capabilities"]
                )
            )
            self.assertEqual(
                next(
                    capability["status"]
                    for capability in payloads["capabilities"]["capabilities"]
                    if capability["identifier"]
                    == "provenance.end_to_end_runtime_lineage"
                ),
                "disabled",
            )
            self.assertEqual(payloads["package"]["name"], "aethermesh")
            self.assertEqual(payloads["package"]["source"], "installed")
            self.assertEqual(payloads["network"]["status"], "local_only")
            self.assertTrue(payloads["network"]["localhost_only"])
            self.assertEqual(payloads["logs"], payloads["logs_alias"])
            self.assertIn("events", payloads["logs"])
            self.assertIn("events", payloads["events"])
            self.assertEqual(payloads["shutdown"]["shutdown_requested"], True)
            self.assertEqual(payloads["restart"]["restart_requested"], True)
            self.assertEqual(
                payloads["root"],
                {
                    "service": "aethermesh-local-node",
                    "version": payloads["health"]["version"],
                    "status": "stopped",
                    "network_mode": "local-only-no-p2p",
                    "endpoints": {
                        "health": "/health",
                        "status": "/api/status",
                        "node": "/api/node",
                        "capabilities": "/api/capabilities",
                        "jobs": "/api/jobs",
                        "network": "/api/network",
                        "openapi": "/openapi.json",
                    },
                },
            )

            self.assertEqual(
                set(payloads["health"]),
                {
                    "ok",
                    "service",
                    "version",
                    "status",
                    "bind_host",
                    "port",
                    "config_path",
                },
            )
            self.assertEqual(payloads["health"]["service"], "aethermesh-local-node")
            self.assertEqual(payloads["health"]["status"], "stopped")
            self.assertEqual(payloads["health"]["bind_host"], "127.0.0.1")
            self.assertEqual(payloads["health"]["port"], 7280)
            self.assertEqual(
                payloads["health"]["config_path"], str(service.paths.config_path)
            )

            api = create_app(service)
            self.assertEqual(api.title, "AetherMesh Local Node API")
            self.assertEqual(api.version, "0.2.0-alpha")
            self.assertIs(api.router.lifespan_context, _lifespan)

    def test_root_is_json_with_live_runtime_status_and_reachable_endpoints(
        self,
    ) -> None:
        async def exercise() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                service = NodeRuntimeService.from_home(Path(temp_dir))
                api = create_app(service)
                transport = httpx.ASGITransport(app=api)
                async with (
                    _lifespan(api),
                    httpx.AsyncClient(
                        transport=transport, base_url="http://testserver"
                    ) as client,
                ):
                    response = await client.get("/")
                    self.assertEqual(response.status_code, 200)
                    self.assertEqual(
                        response.headers["content-type"], "application/json"
                    )
                    payload = response.json()
                    self.assertEqual(payload["status"], "running")
                    self.assertEqual(payload["network_mode"], "local-only-no-p2p")
                    for endpoint in payload["endpoints"].values():
                        linked_response = await client.get(endpoint)
                        self.assertEqual(linked_response.status_code, 200, endpoint)
                        self.assertEqual(
                            linked_response.headers["content-type"], "application/json"
                        )
                self.assertEqual(service.get_node_status()["status"], "stopped")

        asyncio.run(exercise())

    def test_lifespan_uses_same_runtime_service(self) -> None:
        async def exercise() -> None:
            with tempfile.TemporaryDirectory() as temp_dir:
                service = NodeRuntimeService.from_home(Path(temp_dir))
                api = create_app(service)
                async with _lifespan(api):
                    self.assertEqual(service.get_node_status()["status"], "running")
                self.assertEqual(service.get_node_status()["status"], "stopped")

        asyncio.run(exercise())


class AppCliTests(unittest.TestCase):
    def test_cli_smoke_commands_use_runtime_service(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})

            version = runner.invoke(app_cli.app, ["--version"])
            self.assertEqual(version.exit_code, 0)
            self.assertIn("0.2.0-alpha", version.output)

            init = runner.invoke(app_cli.app, ["init"])
            self.assertEqual(init.exit_code, 0)
            self.assertIn("Initialized AetherMesh", init.output)

            for args, expected in [
                (["status"], "stopped"),
                (["node", "status"], "stopped"),
                (["node", "stop"], "Marked foreground"),
                (["peers"], "No peers discovered"),
                (["jobs"], "No current jobs"),
            ]:
                result = runner.invoke(app_cli.app, args)
                self.assertEqual(result.exit_code, 0, result.output)
                self.assertIn(expected, result.output)

    def test_cli_node_start_dry_run_reports_localhost_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})
            result = runner.invoke(app_cli.app, ["node", "start", "--dry-run"])

            self.assertEqual(result.exit_code, 0)
            self.assertIn("127.0.0.1", result.output)
            self.assertIn("7280", result.output)

    def test_cli_node_start_stop_ignore_consumer_desktop_settings(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(temp_dir)
            service.initialize_local_node_data()
            settings_path = Path(temp_dir) / "config" / "desktop-settings.json"
            settings_path.parent.mkdir(parents=True, exist_ok=True)
            settings_text = json.dumps({"backgroundNodeEnabled": True})
            settings_path.write_text(settings_text, encoding="utf-8")
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})
            with (
                patch(
                    "aethermesh_core.app_cli._local_api_is_aethermesh",
                    return_value=False,
                ),
                patch("aethermesh_core.app_cli._serve") as serve,
                patch("subprocess.run") as run_process,
                patch("webbrowser.open") as open_browser,
            ):
                start = runner.invoke(app_cli.app, ["node", "start"])
                service.mark_runtime_started()
                stop = runner.invoke(app_cli.app, ["node", "stop"])

            self.assertEqual(start.exit_code, 0, start.output)
            self.assertEqual(stop.exit_code, 0, stop.output)
            serve.assert_called_once_with(host="127.0.0.1", port=7280)
            run_process.assert_not_called()
            open_browser.assert_not_called()
            self.assertEqual(service.get_node_status()["status"], "stopped")
            self.assertIn("Marked foreground", stop.output)
            self.assertIn("Ctrl+C", stop.output)
            self.assertEqual(settings_path.read_text(encoding="utf-8"), settings_text)

    def test_cli_node_start_reuses_existing_local_api(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})
            with (
                patch(
                    "aethermesh_core.app_cli._local_api_is_aethermesh",
                    return_value=True,
                ),
                patch("aethermesh_core.app_cli._serve") as serve,
            ):
                result = runner.invoke(app_cli.app, ["node", "start"])
            self.assertEqual(result.exit_code, 0, result.output)
            self.assertIn("already running", result.output)
            serve.assert_not_called()

    def test_cli_ui_command_is_retired_without_starting_or_mutating_runtime(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(temp_dir)
            service.initialize_local_node_data()
            config_before = service.paths.config_path.read_bytes()
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})
            with (
                patch("aethermesh_core.app_cli._serve") as serve,
                patch("webbrowser.open") as open_browser,
            ):
                for args in (["ui"], ["ui", "--no-open"], ["ui", "--dry-run"]):
                    result = runner.invoke(app_cli.app, args)
                    self.assertEqual(result.exit_code, 2, result.output)
                    self.assertIn("No such command", result.output)
                    self.assertIn("ui", result.output)
            serve.assert_not_called()
            open_browser.assert_not_called()
            self.assertEqual(service.paths.config_path.read_bytes(), config_before)
            self.assertFalse(service.paths.pid_path.exists())

    def test_local_api_health_detection_handles_false_paths(self) -> None:
        self.assertFalse(app_cli._local_api_is_aethermesh(host="0.0.0.0", port=7280))
        self.assertFalse(app_cli._local_api_is_aethermesh(host="127.0.0.1", port=1))

        class FakeResponse:
            def __init__(self, payload: bytes) -> None:
                self._payload = payload

            def read(self) -> bytes:
                return self._payload

        class FakeConnection:
            def __init__(self, host: str, port: int, timeout: float) -> None:
                self.host = host
                self.port = port
                self.timeout = timeout
                self.closed = False
                self.payload = b'{"service":"not-aethermesh"}'

            def request(self, method: str, target: str) -> None:
                self.method = method
                self.target = target

            def getresponse(self) -> FakeResponse:
                return FakeResponse(self.payload)

            def close(self) -> None:
                self.closed = True

        fake = FakeConnection("127.0.0.1", 7280, 0.5)
        with patch(
            "aethermesh_core.app_cli.http.client.HTTPConnection", return_value=fake
        ):
            self.assertFalse(
                app_cli._local_api_is_aethermesh(host="127.0.0.1", port=7280)
            )
        self.assertEqual(fake.method, "GET")
        self.assertEqual(fake.target, "/health")
        self.assertTrue(fake.closed)

        fake_ok = FakeConnection("127.0.0.1", 7280, 0.5)
        fake_ok.payload = b'{"service":"aethermesh-local-node"}'
        with patch(
            "aethermesh_core.app_cli.http.client.HTTPConnection", return_value=fake_ok
        ):
            self.assertTrue(
                app_cli._local_api_is_aethermesh(host="127.0.0.1", port=7280)
            )

        with patch(
            "aethermesh_core.app_cli.http.client.HTTPConnection",
            side_effect=OSError("no listener"),
        ):
            self.assertFalse(
                app_cli._local_api_is_aethermesh(host="127.0.0.1", port=7280)
            )

    def test_cli_update_installs_latest_release_and_reports_errors(self) -> None:
        class FakeUpdateResult:
            def __init__(self, *, installed: bool) -> None:
                self.installed = installed

            def to_dict(self) -> dict[str, object]:
                return {
                    "release_tag": "v0.2.0-alpha-abc123",
                    "release_name": "0.2.0-alpha - (...bc123)",
                    "release_url": "https://github.example/release",
                    "wheel_name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "wheel_url": "https://github.example/aethermesh.whl",
                    "sha256": "abc123",
                    "expected_sha256": "abc123",
                    "installed": self.installed,
                }

        runner = CliRunner()
        with patch(
            "aethermesh_core.app_cli.update_from_latest_release",
            return_value=FakeUpdateResult(installed=False),
        ) as updater:
            result = runner.invoke(app_cli.app, ["update", "--dry-run"])

        self.assertEqual(result.exit_code, 0, result.output)
        updater.assert_called_once_with(dry_run=True, release_url=None)
        self.assertIn("v0.2.0-alpha-abc123", result.output)
        self.assertIn("verified", result.output)
        self.assertIn("not installed", result.output)

        with patch(
            "aethermesh_core.app_cli.update_from_latest_release",
            return_value=FakeUpdateResult(installed=True),
        ) as updater:
            result = runner.invoke(app_cli.app, ["update"])

        self.assertEqual(result.exit_code, 0, result.output)
        updater.assert_called_once_with(dry_run=False, release_url=None)
        self.assertIn("Installed latest AetherMesh release", result.output)

        with patch(
            "aethermesh_core.app_cli.update_from_latest_release",
            side_effect=ReleaseUpdateError("network sad"),
        ):
            result = runner.invoke(app_cli.app, ["update"])

        self.assertNotEqual(result.exit_code, 0)
        self.assertIn("network sad", result.output)

    def test_cli_warning_and_table_branches_are_exercised(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = CliRunner(env={"AETHERMESH_HOME": temp_dir})

            start = runner.invoke(
                app_cli.app,
                ["node", "start", "--host", "0.0.0.0", "--dry-run"],
            )
            self.assertEqual(start.exit_code, 0)
            self.assertIn("non-localhost", start.output)

            class FakeService:
                def list_peers(self) -> dict[str, object]:
                    return {
                        "peers": [
                            {
                                "node_id": "peer-a",
                                "status": "available",
                                "capabilities": ["echo"],
                            }
                        ]
                    }

                def list_jobs(self) -> dict[str, object]:
                    return {
                        "current": [{"job_id": "job-a"}],
                        "completed": [{"job_id": "job-b"}],
                        "failed": [{"job_id": "job-c"}],
                        "note": "active",
                    }

            with patch.object(
                app_cli.NodeRuntimeService, "default", return_value=FakeService()
            ):
                self.assertIn("peer-a", runner.invoke(app_cli.app, ["peers"]).output)
                self.assertNotIn(
                    "No current jobs", runner.invoke(app_cli.app, ["jobs"]).output
                )

            with app_cli.console.capture() as status_capture:
                app_cli._print_status({"api": "not-a-dict"})
            status_output = status_capture.get()
            self.assertIn("AetherMesh Node Status", status_output)
            self.assertIn("Field", status_output)
            self.assertIn("Value", status_output)
            self.assertIn("node_id", status_output)
            self.assertNotIn("http://", status_output)

            with app_cli.console.capture() as api_status_capture:
                app_cli._print_status(
                    {
                        "node_id": "node-a",
                        "status": "running",
                        "version": "9.9.9",
                        "uptime_seconds": 42,
                        "config_path": "/tmp/aethermesh/config.json",
                        "data_dir": "/tmp/aethermesh/data",
                        "peer_count": 3,
                        "api": {"host": "localhost", "port": 9999},
                    }
                )
            api_status_output = api_status_capture.get()
            for expected in [
                "node-a",
                "running",
                "9.9.9",
                "42",
                "/tmp/aethermesh/config.json",
                "/tmp/aethermesh/data",
                "3",
                "http://localhost:9999",
            ]:
                self.assertIn(expected, api_status_output)

            class FakeTable:
                def __init__(self, title: str) -> None:
                    self.title = title
                    self.columns: list[str] = []
                    self.rows: list[tuple[str, ...]] = []

                def add_column(self, name: str) -> None:
                    self.columns.append(name)

                def add_row(self, *values: str) -> None:
                    self.rows.append(values)

            printed: list[FakeTable] = []
            with (
                patch.object(app_cli, "Table", FakeTable),
                patch.object(app_cli.console, "print", side_effect=printed.append),
            ):
                app_cli._print_status(
                    {
                        "node_id": "node-a",
                        "status": "running",
                        "version": "9.9.9",
                        "uptime_seconds": 42,
                        "config_path": "/tmp/aethermesh/config.json",
                        "data_dir": "/tmp/aethermesh/data",
                        "peer_count": 3,
                        "api": {"host": "localhost", "port": 9999},
                    }
                )
            self.assertEqual(len(printed), 1)
            self.assertEqual(printed[0].title, "AetherMesh Node Status")
            self.assertEqual(printed[0].columns, ["Field", "Value"])
            self.assertEqual(printed[0].rows[-1], ("api", "http://localhost:9999"))

    def test_serve_uses_uvicorn_and_reports_missing_api_dependencies(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            calls: dict[str, object] = {}

            def fake_run(api: FastAPI, host: str, port: int, log_level: str) -> None:
                self.assertIsInstance(api, FastAPI)
                self.assertEqual(log_level, "info")
                calls.update(
                    {"app": api, "host": host, "port": port, "log_level": log_level}
                )

            fake_uvicorn = types.SimpleNamespace(run=fake_run)
            with (
                patch.dict(sys.modules, {"uvicorn": fake_uvicorn}),
                patch.dict(os.environ, {"AETHERMESH_HOME": temp_dir}),
                patch("webbrowser.open") as open_browser,
            ):
                with app_cli.console.capture() as warning_capture:
                    app_cli._serve(host="0.0.0.0", port=7777)
                output = warning_capture.get()
                self.assertIn(
                    "Warning: non-localhost API binding is not the safe default.",
                    output,
                )
                self.assertIn(
                    "Starting AetherMesh local API on http://0.0.0.0:7777", output
                )
                self.assertEqual(calls["host"], "0.0.0.0")
                self.assertEqual(calls["port"], 7777)
                self.assertEqual(calls["log_level"], "info")
                with app_cli.console.capture() as localhost_capture:
                    app_cli._serve(host="127.0.0.1", port=7280)
                self.assertNotIn("non-localhost", localhost_capture.get())
                self.assertEqual(calls["host"], "127.0.0.1")
                with app_cli.console.capture() as localhost_name_capture:
                    app_cli._serve(host="localhost", port=7281)
                self.assertNotIn("non-localhost", localhost_name_capture.get())
                self.assertEqual(calls["host"], "localhost")
                self.assertEqual(calls["port"], 7281)
                open_browser.assert_not_called()

            with (
                patch(
                    "aethermesh_core.app_cli._local_api_is_aethermesh",
                    return_value=False,
                ),
                patch("aethermesh_core.app_cli._serve") as serve,
            ):
                result = CliRunner(env={"AETHERMESH_HOME": temp_dir}).invoke(
                    app_cli.app, ["node", "start"]
                )
                self.assertEqual(result.exit_code, 0, result.output)
                serve.assert_called_once_with(host="127.0.0.1", port=7280)

            real_import = builtins.__import__

            def blocked_import(
                name: str,
                globals: dict[str, object] | None = None,
                locals: dict[str, object] | None = None,
                fromlist: tuple[str, ...] = (),
                level: int = 0,
            ) -> object:
                if name == "uvicorn":
                    raise ImportError("blocked")
                return real_import(name, globals, locals, fromlist, level)

            with (
                patch("builtins.__import__", side_effect=blocked_import),
                self.assertRaisesRegex(
                    Exception, r"API dependencies are missing.*\.\[api\]"
                ),
            ):
                app_cli._serve(host="127.0.0.1", port=7280)


class LocalSafetyMetadataTests(unittest.TestCase):
    def test_invalid_outputs_emit_rejected_receipts_with_preserved_provenance(
        self,
    ) -> None:
        request = {
            "schema_version": 1,
            "job_type": "echo",
            "requested_capability": {"identifier": "work.echo"},
            "input_payload": {
                "payload_type": "json",
                "content": {"message": "expected"},
            },
            "creator_node_id": "creator-local-a",
            "requested_validation_mode": "deterministic-local",
            "lineage_parent_refs": ["data/prior-job.json"],
            "attribution_metadata": {"source": "test"},
        }
        invalid_outputs = (
            ("incomplete output", None, "output_schema.v1.echo.output: expected str"),
            ("manifest expectation mismatch", "unexpected", "output_mismatch"),
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(temp_dir)
            for name, output, expected_reason in invalid_outputs:
                with self.subTest(name=name):
                    submission = service.submit_local_job(request)
                    with patch(
                        "aethermesh_core.runtime_service.run_local_job",
                        return_value=JobResult(
                            job_id=submission["job_id"],
                            node_id="worker-local-a",
                            status="completed",
                            output=output,
                            error=None,
                            contribution_units=1,
                        ),
                    ):
                        status = service.execute_submitted_local_job(
                            submission["job_id"], "worker-local-a"
                        )

                    receipt = service.get_local_validation_receipt(
                        work_id=submission["job_id"]
                    )
                    self.assertEqual(status["status"], "failed")
                    self.assertFalse(status["validation"]["passed"])
                    self.assertEqual(receipt["status"], "rejected")
                    self.assertNotIn(receipt["status"], {"accepted", "pending"})
                    self.assertEqual(receipt["rejection_reason"], expected_reason)
                    self.assertEqual(
                        receipt["rejection_reason"], status["validation"]["reason"]
                    )
                    self.assertEqual(
                        receipt["creator_node_id"], request["creator_node_id"]
                    )
                    self.assertEqual(
                        receipt["manifest_ref"], submission["manifest_ref"]
                    )
                    self.assertEqual(
                        receipt["lineage_parent_ids"], request["lineage_parent_refs"]
                    )
                    self.assertEqual(
                        receipt["contribution_attribution"],
                        status["contribution_attribution"],
                    )
                    self.assertEqual(
                        receipt["contribution_attribution"],
                        {
                            "job_id": submission["job_id"],
                            "creator_node_id": request["creator_node_id"],
                            "metadata": request["attribution_metadata"],
                            "worker_node_id": "worker-local-a",
                            "executor_node_id": "worker-local-a",
                            "validated_contribution_units": 0,
                        },
                    )
            contribution_summary = service.contribution_summary()
            self.assertEqual(contribution_summary["accepted_work_count"], 0)
            self.assertEqual(contribution_summary["non_accepted_work_count"], 2)

    def test_timeout_and_cancellation_record_failed_local_evidence(self) -> None:
        request = {
            "schema_version": 1,
            "job_type": "echo",
            "requested_capability": {"identifier": "work.echo"},
            "input_payload": {"payload_type": "json", "content": {"message": "safe"}},
            "creator_node_id": "creator-local-a",
            "requested_validation_mode": "deterministic-local",
            "lineage_parent_refs": ["data/prior-job.json"],
            "attribution_metadata": {"source": "test"},
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            service = NodeRuntimeService.from_home(temp_dir)
            timed_out = service.submit_local_job(
                {**request, "local_safety": {"timeout_seconds": 0}}
            )
            cancelled = service.submit_local_job(
                {**request, "local_safety": {"cancellation_requested": True}}
            )
            for accepted, outcome in (
                (timed_out, "timed_out"),
                (cancelled, "cancelled"),
            ):
                with self.subTest(outcome=outcome):
                    status = service.execute_submitted_local_job(
                        accepted["job_id"], "worker-local-a"
                    )
                    receipt = json.loads(
                        (
                            Path(temp_dir)
                            / "data"
                            / "job-validation-receipts"
                            / f"{accepted['job_id']}.json"
                        ).read_text(encoding="utf-8")
                    )
                    receipt_view = service.get_local_validation_receipt(
                        work_id=accepted["job_id"]
                    )
                    self.assertEqual(status["status"], "failed")
                    self.assertFalse(status["validation"]["passed"])
                    self.assertEqual(receipt["status"], "rejected")
                    self.assertEqual(
                        receipt["rejection_reason"], "result_not_completed"
                    )
                    self.assertEqual(receipt_view["status"], "rejected")
                    self.assertEqual(
                        receipt_view["rejection_reason"], "result_not_completed"
                    )
                    self.assertEqual(
                        receipt_view["creator_node_id"], request["creator_node_id"]
                    )
                    self.assertEqual(
                        receipt_view["manifest_ref"], accepted["manifest_ref"]
                    )
                    self.assertEqual(
                        receipt_view["lineage_parent_ids"],
                        request["lineage_parent_refs"],
                    )
                    self.assertEqual(
                        receipt_view["contribution_attribution"],
                        status["contribution_attribution"],
                    )
                    self.assertFalse(receipt_view["validation"]["valid"])
                    self.assertEqual(
                        status["contribution_attribution"]["creator_node_id"],
                        "creator-local-a",
                    )
                    self.assertEqual(
                        status["contribution_attribution"][
                            "validated_contribution_units"
                        ],
                        0,
                    )
                    self.assertEqual(
                        status["lineage"]["parent_refs"], ["data/prior-job.json"]
                    )
                    self.assertEqual(receipt["manifest_ref"], accepted["manifest_ref"])
                    self.assertEqual(
                        receipt["validation"]["execution_outcome"], outcome
                    )
                    self.assertFalse(receipt["validation"]["valid"])
                    self.assertRegex(
                        receipt["execution"]["executor_finished_at"],
                        r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$",
                    )
                    self.assertLessEqual(
                        receipt["execution"]["executor_started_at"],
                        receipt["execution"]["executor_finished_at"],
                    )


if __name__ == "__main__":
    unittest.main()
