from __future__ import annotations

import argparse
import contextlib
import io
import tempfile
import unittest
from collections import Counter
from pathlib import Path
from typing import Any, cast
from unittest import mock

from aethermesh_core import cli
from aethermesh_core.dispatch import _node_heartbeat_payloads, dispatch_local_batch
from aethermesh_core.flow_audit import (
    FlowAuditError,
    _assert_contribution_message_matches_receipt,
    _assert_result_message_matches_receipt,
    _find_flow_message_for_receipt,
    _format_claim,
    _ledger_mismatch_detail,
)
from aethermesh_core.identity import IdentityPersistenceError, _load_identity
from aethermesh_core.job_manifest import ManifestError, load_job_manifest
from aethermesh_core.json_io import atomic_write_json, remove_temp_file
from aethermesh_core.ledger import (
    ContributionLedger,
    ContributionRecord,
    LedgerPersistenceError,
    _string_output,
    save_ledger_document,
)
from aethermesh_core.ledger import (
    _remove_temp_file as remove_ledger_temp,
)
from aethermesh_core.local_transport import (
    LocalTransportError,
    _message_from_inbox_entry,
    _write_inbox_document,
)
from aethermesh_core.message_bus import LocalMessageBus
from aethermesh_core.message_log import (
    MessageLogPersistenceError,
    _message_from_document_entry,
    write_message_log,
)
from aethermesh_core.messages import MeshMessage, message_from_mapping
from aethermesh_core.models import Job, JobResult, NodeIdentity
from aethermesh_core.node_service import (
    InboxProcessResult,
    LocalNodeService,
    ProcessedAssignment,
    _fallback_job_id,
    _job_from_assignment_payload,
)
from aethermesh_core.node_state import (
    NodeStatePersistenceError,
    empty_node_processing_state,
    load_node_processing_state,
    save_node_processing_state,
)
from aethermesh_core.node_state import (
    _remove_temp_file as remove_node_state_temp,
)
from aethermesh_core.peer_registry import (
    PeerRegistryError,
    _parse_heartbeat,
    peer_summary_document,
)
from aethermesh_core.receipts import (
    ReceiptPersistenceError,
    _output_summary,
    _single_emitted_message,
    _validate_receipt_document,
    write_receipt_document,
)
from aethermesh_core.receipts import (
    _remove_temp_file as remove_receipt_temp,
)
from aethermesh_core.result_hash import result_hash_from_fields
from aethermesh_core.scheduler import ScheduledJob, _coerce_job
from aethermesh_core.validation import validate_job_result
from scripts import ci_quality_gates


class QualityGateEdgeCoverageTests(unittest.TestCase):
    def test_pr_size_excludes_generated_static_data_from_reviewability_counts(
        self,
    ) -> None:
        args = argparse.Namespace(
            base="origin/main",
            max_files=1,
            max_lines=5,
            max_binary_files=0,
            exclude_path_prefix=["graphify-out/", "wordlists/node-names/"],
        )
        diff = (
            "100\t200\tgraphify-out/graph.json\n"
            "-\t-\tgraphify-out/graph.html\n"
            "9000\t0\twordlists/node-names/cpu-traits.txt\n"
            "2\t3\tsrc/aethermesh_core/example.py"
        )
        result = argparse.Namespace(returncode=0, stdout=diff)
        stdout = io.StringIO()

        with (
            mock.patch.object(ci_quality_gates, "run", return_value=result),
            contextlib.redirect_stdout(stdout),
        ):
            exit_code = ci_quality_gates.command_pr_size(args)

        self.assertEqual(exit_code, 0)
        self.assertIn("files=1", stdout.getvalue())
        self.assertIn("changed_lines=5", stdout.getvalue())
        self.assertIn("binary_files=0", stdout.getvalue())
        self.assertIn("ignored_files=3", stdout.getvalue())

    def test_remove_temp_helpers_ignore_missing_files(self) -> None:
        for remover in (remove_temp_file, remove_ledger_temp, remove_receipt_temp):
            remover("/tmp/aethermesh-definitely-missing-temp-file")

    def test_json_write_wrappers_report_write_failures(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory_path = Path(tmp) / "as-directory"
            directory_path.mkdir()
            with self.assertRaises(LocalTransportError):
                _write_inbox_document(directory_path, {"version": 1})
            with self.assertRaises(MessageLogPersistenceError):
                write_message_log(directory_path, {"version": 1, "messages": []})
            with self.assertRaises(ReceiptPersistenceError):
                write_receipt_document(
                    directory_path,
                    {"version": 1, "run_source": "run-local-flow", "receipts": []},
                )
            with self.assertRaises(LedgerPersistenceError):
                save_ledger_document(directory_path, ContributionLedger())

    def test_message_mapping_rejects_bad_entries(self) -> None:
        with self.assertRaises(ValueError):
            message_from_mapping("not-a-dict")
        with self.assertRaises(ValueError):
            message_from_mapping({"message_id": "m", "message_type": "job_assigned"})
        with self.assertRaises(LocalTransportError):
            _message_from_inbox_entry("not-a-dict", 0)
        with self.assertRaises(MessageLogPersistenceError):
            _message_from_document_entry("not-a-dict", 0)

    def test_manifest_and_identity_os_errors_are_reported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            directory_path = Path(tmp) / "dir"
            directory_path.mkdir()
            with self.assertRaises(ManifestError):
                load_job_manifest(directory_path)
            with self.assertRaises(IdentityPersistenceError):
                _load_identity(directory_path)
            bool_version = Path(tmp) / "manifest.json"
            bool_version.write_text(
                '{"version": true, "nodes": [], "jobs": []}', encoding="utf-8"
            )
            with self.assertRaises(ManifestError):
                load_job_manifest(bool_version)

    def test_dispatch_rejects_empty_nodes_and_bad_roster_fields(self) -> None:
        with self.assertRaises(ValueError):
            dispatch_local_batch(
                manifest_path="m", message_log_path="x", nodes=[], jobs=[]
            )

        class BadRegistry:
            def to_roster(self) -> list[dict[str, Any]]:
                return [
                    {
                        "node_id": "n",
                        "status": "available",
                        "heartbeat_sequence": "bad",
                        "heartbeat_count": 1,
                        "capabilities": [],
                    }
                ]

        with self.assertRaises(ValueError):
            _node_heartbeat_payloads(BadRegistry())

        class BadCapabilities:
            def to_roster(self) -> list[dict[str, Any]]:
                return [
                    {
                        "node_id": "n",
                        "status": "available",
                        "heartbeat_sequence": 1,
                        "heartbeat_count": 1,
                        "capabilities": [1],
                    }
                ]

        with self.assertRaises(ValueError):
            _node_heartbeat_payloads(BadCapabilities())

    def test_cli_defensive_branches(self) -> None:
        with (
            tempfile.TemporaryDirectory() as tmp,
            self.assertRaises(ValueError),
        ):
            cli.run_local_flow("missing.json", str(Path(tmp) / "out" / "child"))
        bad_result = InboxProcessResult(
            node_id="n",
            processed=[],
            ignored_message_ids=[],
            skipped_processed_message_ids=[],
            processed_message_ids=[],
        )
        payload = cli._inbox_process_result_to_dict(
            bad_result, ContributionLedger(), None
        )
        self.assertNotIn("ledger_summary", payload)
        with self.assertRaises(SystemExit):
            cli.main(["not-a-command"])

    def test_node_state_defensive_branches(self) -> None:
        with self.assertRaises(NodeStatePersistenceError):
            empty_node_processing_state("")
        with tempfile.TemporaryDirectory() as tmp:
            directory_path = Path(tmp) / "dir"
            directory_path.mkdir()
            with self.assertRaises(NodeStatePersistenceError):
                load_node_processing_state(directory_path, expected_node_id="n")
            with self.assertRaises(NodeStatePersistenceError):
                save_node_processing_state(
                    directory_path, empty_node_processing_state("n")
                )
            bad_count = Path(tmp) / "bad-count.json"
            bad_count.write_text(
                '{"version": 1, "node_id": "n", "processed_message_ids": [], "processed_assignment_count": true}',
                encoding="utf-8",
            )
            with self.assertRaises(NodeStatePersistenceError):
                load_node_processing_state(bad_count, expected_node_id="n")

    def test_peer_registry_defensive_branches(self) -> None:
        with self.assertRaises(PeerRegistryError):
            _parse_heartbeat(
                MeshMessage(
                    "m",
                    "node_heartbeat",
                    "n",
                    None,
                    {"node_id": "", "status": "available", "heartbeat_sequence": 1},
                )
            )
        with self.assertRaises(PeerRegistryError):
            _parse_heartbeat(
                MeshMessage(
                    "m",
                    "node_heartbeat",
                    "n",
                    None,
                    {"node_id": "n", "status": "", "heartbeat_sequence": 1},
                )
            )
        with self.assertRaises(PeerRegistryError):
            _parse_heartbeat(
                MeshMessage(
                    "m",
                    "node_heartbeat",
                    "n",
                    None,
                    {
                        "node_id": "n",
                        "status": "available",
                        "heartbeat_sequence": 1,
                        "heartbeat_count": -1,
                    },
                )
            )
        with self.assertRaises(PeerRegistryError):
            _parse_heartbeat(
                MeshMessage(
                    "m",
                    "node_heartbeat",
                    "n",
                    None,
                    {
                        "node_id": "n",
                        "status": "available",
                        "heartbeat_sequence": 1,
                        "capabilities": [""],
                    },
                )
            )

    def test_receipt_defensive_branches(self) -> None:
        result_hash = result_hash_from_fields(
            job_id="job",
            node_id="node-a",
            status="completed",
            output={"z": "value"},
            error=None,
        )
        result_msg = MeshMessage(
            "r",
            "job_result_reported",
            "node-a",
            "local-ledger",
            {
                "job_id": "job",
                "status": "completed",
                "output": {"z": "value"},
                "error": None,
                "result_hash": result_hash,
            },
            "job",
        )
        contribution_msg = MeshMessage(
            "c",
            "contribution_recorded",
            "local-ledger",
            "node-a",
            {
                "job_id": "job",
                "node_id": "node-a",
                "status": "completed",
                "valid": True,
                "validation": "ok",
                "contribution_units": 1,
            },
            "job",
        )
        receipt = {
            "job_id": "job",
            "job_type": "echo",
            "node_id": "node-a",
            "assignment_message_id": "a",
            "correlation_id": "job",
            "result_message_id": "r",
            "contribution_message_id": "c",
            "result_status": "completed",
            "result_hash": result_hash,
            "validation": {"valid": True, "reason": "ok"},
            "credited_units": 1,
            "output_summary": {"z": "value"},
        }
        self.assertEqual(
            _output_summary([object()])["items"][0].startswith("<object object"), True
        )
        _assert_result_message_matches_receipt("ctx", receipt, result_msg)
        _assert_contribution_message_matches_receipt("ctx", receipt, contribution_msg)
        with self.assertRaises(FlowAuditError):
            _assert_result_message_matches_receipt("ctx", receipt, contribution_msg)
        with self.assertRaises(FlowAuditError):
            _assert_contribution_message_matches_receipt("ctx", receipt, result_msg)
        with self.assertRaises(ValueError):
            _single_emitted_message(
                ProcessedAssignment(
                    message_id="a",
                    correlation_id="job",
                    job=Job("job", "echo", {}),
                    result=JobResult("job", "node-a", "completed", {}, None, 1),
                    validation=validate_job_result(
                        Job("job", "echo", {}),
                        JobResult("job", "node-a", "completed", {}, None, 1),
                    ),
                    contribution_record=ContributionRecord(
                        node_id="node-a",
                        job_id="job",
                        status="completed",
                        contribution_units=1,
                    ),
                    emitted_messages=[],
                ),
                "job_result_reported",
            )
        invalid_docs = [
            {"version": 2, "run_source": "run-local-flow", "receipts": []},
            {"version": 1, "run_source": "other", "receipts": []},
            {"version": 1, "run_source": "run-local-flow", "receipts": ["bad"]},
            {
                "version": 1,
                "run_source": "run-local-flow",
                "receipts": [{**receipt, "credited_units": True}],
            },
            {
                "version": 1,
                "run_source": "run-local-flow",
                "receipts": [
                    {**receipt, "validation": {"valid": "yes", "reason": "ok"}}
                ],
            },
            {
                "version": 1,
                "run_source": "run-local-flow",
                "receipts": [{**receipt, "output_summary": []}],
            },
        ]
        for document in invalid_docs:
            with self.assertRaises(ReceiptPersistenceError):
                _validate_receipt_document(document)

    def test_flow_audit_edge_helpers(self) -> None:
        receipt = {"job_id": "job", "node_id": "node-a"}
        with self.assertRaises(FlowAuditError):
            _find_flow_message_for_receipt(
                "ctx", [], receipt, message_id="m", message_type="job_result_reported"
            )
        duplicate = MeshMessage(
            "m", "job_result_reported", "node-a", "local-ledger", {"job_id": "job"}
        )
        with self.assertRaises(FlowAuditError):
            _find_flow_message_for_receipt(
                "ctx",
                [duplicate, duplicate],
                receipt,
                message_id="m",
                message_type="job_result_reported",
            )
        weird = MeshMessage("m", "job_assigned", "node-a", None, {"job_id": "job"})
        with self.assertRaises(FlowAuditError):
            _find_flow_message_for_receipt(
                "ctx", [weird], receipt, message_id="m", message_type="job_assigned"
            )
        detail = _ledger_mismatch_detail(
            Counter({("n", "j", "completed", 1, True, "ok", "echo"): 1}),
            Counter({("n2", "j", "failed", 0, False, "bad", "echo"): 1}),
        )
        self.assertIn("missing node=n", detail)
        self.assertIn("extra node=n2", detail)
        self.assertIn(
            "job_type=echo",
            _format_claim(("n", "j", "completed", 1, True, "ok", "echo")),
        )

    def test_ledger_record_and_validation_edges(self) -> None:
        for field in ("message", "validation_valid", "validation_reason", "job_type"):
            payload = {
                "node_id": "n",
                "job_id": "j",
                "status": "completed",
                "contribution_units": 1,
                field: 1,
            }
            with self.assertRaises(LedgerPersistenceError):
                ContributionRecord.from_dict(payload)
        with self.assertRaises(LedgerPersistenceError):
            ContributionRecord.from_dict(
                {
                    "node_id": True,
                    "job_id": "j",
                    "status": "completed",
                    "contribution_units": 1,
                }
            )
        invalid_result = JobResult("j", "n", "completed", {"unexpected": True}, None, 1)
        validation = validate_job_result(
            Job("j", "echo", {"message": "hello"}), invalid_result
        )
        self.assertFalse(validation.valid)

    def test_final_quality_gate_branch_edges(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            output = Path(tmp) / "atomic.json"
            with self.assertRaises(TypeError):
                atomic_write_json(output, {"bad": object()})
            with self.assertRaises(OSError):
                atomic_write_json(Path("/dev/null/child.json"), {"ok": True})

            state_path = Path(tmp) / "state.json"
            with (
                mock.patch(
                    "aethermesh_core.node_state.os.replace", side_effect=OSError("boom")
                ),
                self.assertRaises(NodeStatePersistenceError),
            ):
                save_node_processing_state(state_path, empty_node_processing_state("n"))

        self.assertIsNone(_string_output(None))

        bus = LocalMessageBus()
        bus.register_node("n")
        bus.register_node("other")
        bus.register_node("sender")
        bus._inboxes["n"].append(
            MeshMessage(
                "x",
                "job_assigned",
                "sender",
                "other",
                {"job_id": "j", "job_type": "echo", "payload": {"message": "hi"}},
            )
        )
        service = LocalNodeService(
            identity=NodeIdentity("n"),
            message_bus=bus,
            runner=cast(Any, object()),
            ledger=ContributionLedger(),
        )
        self.assertEqual(service.process_inbox().ignored_message_ids, ["x"])

        no_payload = MeshMessage(
            "m",
            "job_assigned",
            "scheduler",
            "n",
            {"job_id": "j", "job_type": "echo", "payload": "bad"},
        )
        self.assertEqual(_job_from_assignment_payload(no_payload).payload, {})
        self.assertEqual(
            _fallback_job_id(MeshMessage("m2", "job_assigned", "scheduler", "n", {})),
            "malformed-m2",
        )

        scheduled_job = ScheduledJob("j", "echo")
        self.assertIs(_coerce_job(scheduled_job), scheduled_job)
        self.assertEqual(_coerce_job(object()).job_type, "echo")
        remove_node_state_temp(
            Path("/tmp/aethermesh-definitely-missing-node-state-temp")
        )
        remove_node_state_temp(Path("/tmp"))
        unsupported_validation = validate_job_result(
            Job("u", "unknown", {}), JobResult("u", "n", "completed", {}, None, 1)
        )
        self.assertEqual(unsupported_validation.reason, "unsupported_job_type")

        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "messages.json"
            write_message_log(
                log_path,
                {
                    "version": 1,
                    "messages": [
                        MeshMessage(
                            "h2",
                            "node_heartbeat",
                            "n",
                            None,
                            {
                                "node_id": "n",
                                "status": "available",
                                "heartbeat_sequence": 2,
                                "capabilities": ["echo"],
                            },
                        ).to_dict(),
                        MeshMessage(
                            "h1",
                            "node_heartbeat",
                            "n",
                            None,
                            {
                                "node_id": "n",
                                "status": "offline",
                                "heartbeat_sequence": 1,
                                "capabilities": [],
                            },
                        ).to_dict(),
                    ],
                },
            )
            peers = cast(
                list[dict[str, object]], peer_summary_document(log_path)["peers"]
            )
            self.assertIsInstance(peers, list)
            self.assertIsInstance(peers[0], dict)
            self.assertEqual(peers[0]["heartbeat_count"], 2)

        self.assertEqual(_output_summary(None), {})
        self.assertEqual(_output_summary("x"), {"value": "x"})
        self.assertEqual(
            _output_summary({1: "ignored", "a": [None, {"b": ["c"]}]}),
            {"a": [None, {"b": ["c"]}]},
        )
        self.assertEqual(
            _output_summary(object())["value"].startswith("<object object"), True
        )
        self.assertIn(
            "missing node=n",
            _ledger_mismatch_detail(
                Counter({("n", "j", "completed", 1, True, "ok", "echo"): 1}), Counter()
            ),
        )
        self.assertIn(
            "extra node=n2",
            _ledger_mismatch_detail(
                Counter(), Counter({("n2", "j", "failed", 0, False, "bad", "echo"): 1})
            ),
        )


if __name__ == "__main__":
    unittest.main()
