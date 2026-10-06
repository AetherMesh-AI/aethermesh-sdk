import contextlib
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from aethermesh_core.cli import build_parser, main
from aethermesh_core.message_log import write_message_log
from aethermesh_core.messages import MeshMessage
from aethermesh_core.release_update import ReleaseUpdateError
from aethermesh_core.version_metadata import capture_version_metadata


class CliTests(unittest.TestCase):
    def test_build_parser_declares_command_argument_contracts(self) -> None:
        parser = build_parser()

        self.assertEqual(parser.prog, "aethermesh-core")
        help_text = parser.format_help()
        for command in [
            "run-demo",
            "reset-identity",
            "simulate-local",
            "run-local-batch",
            "dispatch-local-batch",
            "dispatch-peer-batch",
            "run-local-flow",
            "run-local-transport-flow",
            "run-peer-transport-flow",
            "audit-local-flow",
            "aggregate-local-flow",
            "validate-local-results",
            "ledger-summary",
            "peer-summary",
            "announce-local-node",
            "materialize-local-inboxes",
            "collect-local-outboxes",
            "process-local-inbox",
            "update",
        ]:
            self.assertIn(command, help_text)

        demo = parser.parse_args(["run-demo"])
        self.assertEqual(demo.command, "run-demo")
        self.assertEqual(demo.node_id, None)
        self.assertEqual(demo.identity_path, None)
        self.assertEqual(demo.ephemeral_identity, False)
        self.assertEqual(demo.message, "hello mesh")
        self.assertEqual(demo.include_ledger, False)
        self.assertEqual(demo.ledger_path, None)

        batch = parser.parse_args(
            [
                "run-local-batch",
                "--manifest",
                "manifest.json",
                "--ledger-path",
                "ledger.json",
                "--message-log-path",
                "messages.json",
            ]
        )
        self.assertEqual(batch.command, "run-local-batch")
        self.assertEqual(batch.manifest, "manifest.json")
        self.assertEqual(batch.ledger_path, "ledger.json")
        self.assertEqual(batch.message_log_path, "messages.json")

        dispatch = parser.parse_args(
            [
                "dispatch-local-batch",
                "--manifest",
                "manifest.json",
                "--message-log-path",
                "dispatch.json",
            ]
        )
        self.assertEqual(dispatch.command, "dispatch-local-batch")
        self.assertEqual(dispatch.manifest, "manifest.json")
        self.assertEqual(dispatch.message_log_path, "dispatch.json")

        peer_dispatch = parser.parse_args(
            [
                "dispatch-peer-batch",
                "--peer-log-path",
                "peers.json",
                "--manifest",
                "manifest.json",
                "--message-log-path",
                "dispatch.json",
            ]
        )
        self.assertEqual(peer_dispatch.command, "dispatch-peer-batch")
        self.assertEqual(peer_dispatch.peer_log_path, "peers.json")
        self.assertEqual(peer_dispatch.manifest, "manifest.json")
        self.assertEqual(peer_dispatch.message_log_path, "dispatch.json")

        flow = parser.parse_args(
            ["run-local-flow", "--manifest", "manifest.json", "--output-dir", "out"]
        )
        self.assertEqual(flow.command, "run-local-flow")
        self.assertEqual(flow.manifest, "manifest.json")
        self.assertEqual(flow.output_dir, "out")
        self.assertEqual(flow.ephemeral_identity, False)

        transport_flow = parser.parse_args(
            [
                "run-local-transport-flow",
                "--manifest",
                "manifest.json",
                "--output-dir",
                "out",
                "--transport-dir",
                "transport",
            ]
        )
        self.assertEqual(transport_flow.command, "run-local-transport-flow")
        self.assertEqual(transport_flow.manifest, "manifest.json")
        self.assertEqual(transport_flow.output_dir, "out")
        self.assertEqual(transport_flow.transport_dir, "transport")

        peer_transport_flow = parser.parse_args(
            [
                "run-peer-transport-flow",
                "--peer-log-path",
                "peers.json",
                "--manifest",
                "manifest.json",
                "--output-dir",
                "out",
                "--transport-dir",
                "transport",
            ]
        )
        self.assertEqual(peer_transport_flow.command, "run-peer-transport-flow")
        self.assertEqual(peer_transport_flow.peer_log_path, "peers.json")
        self.assertEqual(peer_transport_flow.manifest, "manifest.json")
        self.assertEqual(peer_transport_flow.output_dir, "out")
        self.assertEqual(peer_transport_flow.transport_dir, "transport")

        audit = parser.parse_args(["audit-local-flow", "--output-dir", "out"])
        self.assertEqual(audit.command, "audit-local-flow")
        self.assertEqual(audit.output_dir, "out")

        aggregate = parser.parse_args(
            [
                "aggregate-local-flow",
                "--output-dir",
                "out",
                "--aggregate-path",
                "aggregate.json",
            ]
        )
        self.assertEqual(aggregate.command, "aggregate-local-flow")
        self.assertEqual(aggregate.output_dir, "out")
        self.assertEqual(aggregate.aggregate_path, "aggregate.json")

        validation = parser.parse_args(
            [
                "validate-local-results",
                "--assignment-log-path",
                "dispatch.json",
                "--result-log-path",
                "flow.json",
                "--validation-log-path",
                "validation.json",
            ]
        )
        self.assertEqual(validation.command, "validate-local-results")
        self.assertEqual(validation.assignment_log_path, "dispatch.json")
        self.assertEqual(validation.result_log_path, "flow.json")
        self.assertEqual(validation.validation_log_path, "validation.json")

        ledger = parser.parse_args(["ledger-summary", "--ledger-path", "ledger.json"])
        self.assertEqual(ledger.command, "ledger-summary")
        self.assertEqual(ledger.ledger_path, "ledger.json")

        peers = parser.parse_args(
            ["peer-summary", "--message-log-path", "messages.json"]
        )
        self.assertEqual(peers.command, "peer-summary")
        self.assertEqual(peers.message_log_path, "messages.json")

        announcement = parser.parse_args(
            [
                "announce-local-node",
                "--node-id",
                "node-a",
                "--message-log-path",
                "announce.json",
                "--status",
                "offline",
                "--capability",
                "echo",
                "--capability",
                "text_stats",
            ]
        )
        self.assertEqual(announcement.command, "announce-local-node")
        self.assertEqual(announcement.node_id, "node-a")
        self.assertEqual(announcement.message_log_path, "announce.json")
        self.assertEqual(announcement.status, "offline")
        self.assertEqual(announcement.capability, ["echo", "text_stats"])

        materialize = parser.parse_args(
            [
                "materialize-local-inboxes",
                "--message-log-path",
                "dispatch.json",
                "--transport-dir",
                "transport",
            ]
        )
        self.assertEqual(materialize.command, "materialize-local-inboxes")
        self.assertEqual(materialize.message_log_path, "dispatch.json")
        self.assertEqual(materialize.transport_dir, "transport")

        collect = parser.parse_args(
            [
                "collect-local-outboxes",
                "--transport-dir",
                "transport",
                "--message-log-path",
                "collected.json",
            ]
        )
        self.assertEqual(collect.command, "collect-local-outboxes")
        self.assertEqual(collect.transport_dir, "transport")
        self.assertEqual(collect.message_log_path, "collected.json")

        inbox = parser.parse_args(
            [
                "process-local-inbox",
                "--node-id",
                "node-a",
                "--message-log-path",
                "messages.json",
                "--transport-dir",
                "transport",
                "--ledger-path",
                "ledger.json",
                "--output-message-log-path",
                "worker.json",
                "--node-state-path",
                "node-state.json",
                "--write-transport-outbox",
            ]
        )
        self.assertEqual(inbox.command, "process-local-inbox")
        self.assertEqual(inbox.node_id, "node-a")
        self.assertEqual(inbox.message_log_path, "messages.json")
        self.assertEqual(inbox.transport_dir, "transport")
        self.assertEqual(inbox.ledger_path, "ledger.json")
        self.assertEqual(inbox.output_message_log_path, "worker.json")
        self.assertEqual(inbox.node_state_path, "node-state.json")
        self.assertEqual(inbox.write_transport_outbox, True)

        update = parser.parse_args(["update", "--dry-run"])
        self.assertEqual(update.command, "update")
        self.assertEqual(update.dry_run, True)
        self.assertEqual(update.release_url, None)

    def test_build_parser_help_snapshots_stay_stable(self) -> None:
        parser = build_parser()
        subparsers_action = next(
            action for action in parser._actions if action.dest == "command"
        )
        subparser_help = {
            command: self._normalize_parser_help(subparser.format_help())
            for command, subparser in dict(subparsers_action.choices).items()
        }
        self.assertEqual(
            self._normalize_parser_help(parser.format_help()),
            "usage: aethermesh-core [-h] {run-demo,reset-identity,simulate-local,update,start-local-node,shutdown-local-node,restart-local-node,run-local-batch,dispatch-local-batch,dispatch-peer-batch,run-local-flow,run-local-transport-flow,run-peer-transport-flow,audit-local-flow,aggregate-local-flow,validate-local-results,ledger-summary,peer-summary,announce-local-node,materialize-local-inboxes,collect-local-outboxes,process-local-inbox} ... positional arguments: {run-demo,reset-identity,simulate-local,update,start-local-node,shutdown-local-node,restart-local-node,run-local-batch,dispatch-local-batch,dispatch-peer-batch,run-local-flow,run-local-transport-flow,run-peer-transport-flow,audit-local-flow,aggregate-local-flow,validate-local-results,ledger-summary,peer-summary,announce-local-node,materialize-local-inboxes,collect-local-outboxes,process-local-inbox} run-demo Run one local echo job and print its JSON result. reset-identity Explicitly reset a persisted local node identity after quarantining the old one. simulate-local Run a deterministic local multi-node simulation and print JSON. update Install the latest AetherMesh wheel from the newest GitHub release. start-local-node Initialize one local-only node runtime with identity, manifest, receipt, and lineage artifacts. shutdown-local-node Gracefully stop a local-only node runtime and persist final state. restart-local-node Cleanly stop and restart a local-only node runtime from persisted state. run-local-batch Run a manifest-backed local multi-node job batch and print JSON. dispatch-local-batch Write assignment-only local dispatch messages for a manifest batch. dispatch-peer-batch Write local dispatch messages using heartbeat- discovered peers. run-local-flow Run dispatch plus all available local worker inboxes for a manifest. run-local-transport-flow Run the local flow using file-backed transport inboxes with a default transport directory. run-peer-transport-flow Run local file transport using heartbeat-discovered peers as the worker roster. audit-local-flow Read and verify a completed run-local-flow artifact directory. aggregate-local-flow Audit and aggregate a completed local flow artifact directory. validate-local-results Replay local assignment/result logs and write a validation report. ledger-summary Inspect an existing local contribution ledger and print JSON totals. peer-summary Inspect heartbeat-derived peers from an existing local message log. announce-local-node Write one local node heartbeat announcement message log. materialize-local-inboxes Materialize addressed message-log entries into file- backed local inboxes. collect-local-outboxes Collect per-node local transport outboxes into one message log. process-local-inbox Replay a local message log or local transport inbox for one node's work. options: -h, --help show this help message and exit",
        )
        self.assertEqual(
            subparser_help,
            {
                "run-demo": "usage: aethermesh-core run-demo [-h] [--node-id NODE_ID] [--identity-path IDENTITY_PATH] [--ephemeral-identity] [--message MESSAGE] [--include-ledger] [--ledger-path LEDGER_PATH] options: -h, --help show this help message and exit --node-id NODE_ID Node id to use for the demo. Defaults to a deterministic machine id. --identity-path IDENTITY_PATH Opt in to JSON-file-backed local node identity persistence. --ephemeral-identity Use a fresh test-only node identity for this run without touching persistent identity files. --message MESSAGE Message payload for the local echo job. --include-ledger Include an in-memory contribution summary for the demo result. --ledger-path LEDGER_PATH Opt in to JSON-file-backed local contribution ledger persistence.",
                "reset-identity": "usage: aethermesh-core reset-identity [-h] --identity-path IDENTITY_PATH [--confirm-reset] [--reason REASON] [--quarantine-dir QUARANTINE_DIR] [--audit-receipt-path AUDIT_RECEIPT_PATH] [--rotate-creator-identity] options: -h, --help show this help message and exit --identity-path IDENTITY_PATH Path to the existing persisted local node identity JSON file. --confirm-reset Required acknowledgement that lineage and attribution continuity may be affected. --reason REASON Optional local audit reason recorded in the reset receipt. --quarantine-dir QUARANTINE_DIR Optional directory for quarantined previous identity material and receipts. --audit-receipt-path AUDIT_RECEIPT_PATH Optional path for the local identity reset audit receipt JSON. --rotate-creator-identity Also rotate creator_node_id; default preserves creator identity for attribution continuity.",
                "simulate-local": "usage: aethermesh-core simulate-local [-h] options: -h, --help show this help message and exit",
                "update": "usage: aethermesh-core update [-h] [--dry-run] [--release-url RELEASE_URL] options: -h, --help show this help message and exit --dry-run Download and verify the latest release wheel without installing it. --release-url RELEASE_URL Override the GitHub latest-release API URL for update discovery.",
                "start-local-node": "usage: aethermesh-core start-local-node [-h] --runtime-dir RUNTIME_DIR [--reset-creator-identity] options: -h, --help show this help message and exit --runtime-dir RUNTIME_DIR Local runtime directory for identity, manifests, receipts, logs, work, and lineage artifacts. --reset-creator-identity Explicitly rotate the creator identity before startup; normal startup preserves it.",
                "shutdown-local-node": "usage: aethermesh-core shutdown-local-node [-h] --runtime-dir RUNTIME_DIR [--timeout-seconds TIMEOUT_SECONDS] options: -h, --help show this help message and exit --runtime-dir RUNTIME_DIR Local runtime directory containing identity, manifests, receipts, logs, work, and lineage artifacts. --timeout-seconds TIMEOUT_SECONDS Bounded graceful shutdown timeout for local persistence checks.",
                "restart-local-node": "usage: aethermesh-core restart-local-node [-h] --runtime-dir RUNTIME_DIR [--timeout-seconds TIMEOUT_SECONDS] options: -h, --help show this help message and exit --runtime-dir RUNTIME_DIR Local runtime directory containing identity, manifests, receipts, logs, work, and lineage artifacts. --timeout-seconds TIMEOUT_SECONDS Bounded graceful shutdown timeout before local restart.",
                "run-local-batch": "usage: aethermesh-core run-local-batch [-h] --manifest MANIFEST [--ledger-path LEDGER_PATH] [--message-log-path MESSAGE_LOG_PATH] options: -h, --help show this help message and exit --manifest MANIFEST Path to a version 1 local job-batch JSON manifest. --ledger-path LEDGER_PATH Opt in to JSON-file-backed local contribution ledger persistence. --message-log-path MESSAGE_LOG_PATH Opt in to overwriting a local JSON audit log of deterministic mesh messages.",
                "dispatch-local-batch": "usage: aethermesh-core dispatch-local-batch [-h] --manifest MANIFEST --message-log-path MESSAGE_LOG_PATH options: -h, --help show this help message and exit --manifest MANIFEST Path to a version 1 local job-batch JSON manifest. --message-log-path MESSAGE_LOG_PATH Path to write the version 1 assignment-only local message log.",
                "dispatch-peer-batch": "usage: aethermesh-core dispatch-peer-batch [-h] --peer-log-path PEER_LOG_PATH --manifest MANIFEST --message-log-path MESSAGE_LOG_PATH options: -h, --help show this help message and exit --peer-log-path PEER_LOG_PATH Path to an existing version 1 local heartbeat message log. --manifest MANIFEST Path to a version 1 manifest whose jobs should be dispatched. --message-log-path MESSAGE_LOG_PATH Path to write the version 1 assignment-only local message log.",
                "run-local-flow": "usage: aethermesh-core run-local-flow [-h] --manifest MANIFEST --output-dir OUTPUT_DIR [--transport-dir TRANSPORT_DIR] [--ephemeral-identity] options: -h, --help show this help message and exit --manifest MANIFEST Path to a version 1 local job-batch JSON manifest. --output-dir OUTPUT_DIR Directory for deterministic local flow artifacts. --transport-dir TRANSPORT_DIR Opt in to file-backed local transport inboxes for worker processing. --ephemeral-identity Replace manifest worker node IDs with fresh test-only IDs for this flow run.",
                "run-local-transport-flow": "usage: aethermesh-core run-local-transport-flow [-h] --manifest MANIFEST --output-dir OUTPUT_DIR [--transport-dir TRANSPORT_DIR] options: -h, --help show this help message and exit --manifest MANIFEST Path to a version 1 local job-batch JSON manifest. --output-dir OUTPUT_DIR Directory for deterministic local flow artifacts. --transport-dir TRANSPORT_DIR Override the default file-backed local transport directory.",
                "run-peer-transport-flow": "usage: aethermesh-core run-peer-transport-flow [-h] --peer-log-path PEER_LOG_PATH --manifest MANIFEST --output-dir OUTPUT_DIR [--transport-dir TRANSPORT_DIR] options: -h, --help show this help message and exit --peer-log-path PEER_LOG_PATH Path to an existing version 1 local heartbeat peer message log. --manifest MANIFEST Path to a version 1 manifest whose jobs should be run. --output-dir OUTPUT_DIR Directory for deterministic local peer transport artifacts. --transport-dir TRANSPORT_DIR Override the default file-backed local transport directory.",
                "audit-local-flow": "usage: aethermesh-core audit-local-flow [-h] --output-dir OUTPUT_DIR options: -h, --help show this help message and exit --output-dir OUTPUT_DIR Directory containing deterministic local flow artifacts to audit.",
                "aggregate-local-flow": "usage: aethermesh-core aggregate-local-flow [-h] --output-dir OUTPUT_DIR [--aggregate-path AGGREGATE_PATH] options: -h, --help show this help message and exit --output-dir OUTPUT_DIR Directory containing deterministic local flow artifacts to aggregate. --aggregate-path AGGREGATE_PATH Path to write the aggregate result JSON. Defaults to <output-dir>/aggregate-result.json.",
                "validate-local-results": "usage: aethermesh-core validate-local-results [-h] --assignment-log-path ASSIGNMENT_LOG_PATH --result-log-path RESULT_LOG_PATH --validation-log-path VALIDATION_LOG_PATH options: -h, --help show this help message and exit --assignment-log-path ASSIGNMENT_LOG_PATH Path to an existing version 1 dispatch/assignment message log. --result-log-path RESULT_LOG_PATH Path to an existing version 1 worker/result message log. --validation-log-path VALIDATION_LOG_PATH New path to write the deterministic local validation report.",
                "ledger-summary": "usage: aethermesh-core ledger-summary [-h] --ledger-path LEDGER_PATH options: -h, --help show this help message and exit --ledger-path LEDGER_PATH Path to an existing version 1 local contribution ledger JSON file.",
                "peer-summary": "usage: aethermesh-core peer-summary [-h] --message-log-path MESSAGE_LOG_PATH options: -h, --help show this help message and exit --message-log-path MESSAGE_LOG_PATH Path to an existing version 1 local message log.",
                "announce-local-node": "usage: aethermesh-core announce-local-node [-h] --node-id NODE_ID --message-log-path MESSAGE_LOG_PATH [--status {available,offline}] [--capability CAPABILITY] options: -h, --help show this help message and exit --node-id NODE_ID Local node id to announce. --message-log-path MESSAGE_LOG_PATH New path to write the version 1 local announcement message log. --status {available,offline} Local node status to announce. Defaults to available. --capability CAPABILITY Capability to announce. May be supplied multiple times; defaults to local capabilities.",
                "materialize-local-inboxes": "usage: aethermesh-core materialize-local-inboxes [-h] --message-log-path MESSAGE_LOG_PATH --transport-dir TRANSPORT_DIR options: -h, --help show this help message and exit --message-log-path MESSAGE_LOG_PATH Path to a version 1 local dispatch/message log. --transport-dir TRANSPORT_DIR Directory where per-node local transport inboxes should be written.",
                "collect-local-outboxes": "usage: aethermesh-core collect-local-outboxes [-h] --transport-dir TRANSPORT_DIR --message-log-path MESSAGE_LOG_PATH options: -h, --help show this help message and exit --transport-dir TRANSPORT_DIR Directory containing per-node local transport outboxes. --message-log-path MESSAGE_LOG_PATH Path to write the collected version 1 local message log.",
                "process-local-inbox": "usage: aethermesh-core process-local-inbox [-h] --node-id NODE_ID [--message-log-path MESSAGE_LOG_PATH] [--transport-dir TRANSPORT_DIR] [--ledger-path LEDGER_PATH] [--output-message-log-path OUTPUT_MESSAGE_LOG_PATH] [--node-state-path NODE_STATE_PATH] [--write-transport-outbox] options: -h, --help show this help message and exit --node-id NODE_ID Local node id whose replayed inbox should be processed. --message-log-path MESSAGE_LOG_PATH Path to a version 1 local message log produced by run-local-batch. --transport-dir TRANSPORT_DIR Read this node's file-backed local transport inbox instead of a message log. --ledger-path LEDGER_PATH Opt in to persisting validation-gated contribution records. --output-message-log-path OUTPUT_MESSAGE_LOG_PATH Opt in to writing replayed plus emitted worker messages as a local message log. --node-state-path NODE_STATE_PATH Opt in to JSON-file-backed local processed-assignment state for resume/idempotency. --write-transport-outbox Opt in to writing emitted worker messages to this node's local transport outbox.",
            },
        )

    @staticmethod
    def _normalize_parser_help(text: str) -> str:
        return " ".join(text.split()).replace("run- local", "run-local")

    def test_validate_local_results_cli_writes_report_and_reports_errors(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_path = Path(temp_dir)
            assignment_path = temp_path / "dispatch.json"
            result_path = temp_path / "flow.json"
            validation_path = temp_path / "validation.json"
            write_message_log(
                assignment_path,
                {
                    "version": 1,
                    "metadata": {"message_count": 1},
                    "messages": [
                        MeshMessage(
                            message_id="msg-0001",
                            message_type="job_assigned",
                            sender_node_id="local-scheduler",
                            recipient_node_id="node-a",
                            payload={
                                "job_id": "job-a",
                                "job_type": "echo",
                                "payload": {"message": "hello"},
                            },
                            correlation_id="job-a",
                        ).to_dict()
                    ],
                },
            )
            write_message_log(
                result_path,
                {
                    "version": 1,
                    "metadata": {"message_count": 1},
                    "messages": [
                        MeshMessage(
                            message_id="msg-0002",
                            message_type="job_result_reported",
                            sender_node_id="node-a",
                            recipient_node_id="local-ledger",
                            payload={
                                "job_id": "job-a",
                                "status": "completed",
                                "output": "hello",
                                "error": None,
                                "contribution_units": 1,
                            },
                            correlation_id="job-a",
                        ).to_dict()
                    ],
                },
            )

            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "validate-local-results",
                        "--assignment-log-path",
                        str(assignment_path),
                        "--result-log-path",
                        str(result_path),
                        "--validation-log-path",
                        str(validation_path),
                    ]
                )

            self.assertEqual(exit_code, 0)
            payload = json.loads(stdout.getvalue())
            self.assertEqual(payload["summary"]["valid_results"], 1)
            self.assertTrue(validation_path.exists())

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "validate-local-results",
                        "--assignment-log-path",
                        str(assignment_path),
                        "--result-log-path",
                        str(result_path),
                        "--validation-log-path",
                        str(validation_path),
                    ]
                )
            self.assertEqual(exit_code, 1)
            self.assertIn("already exists", stderr.getvalue())

    def test_update_cli_prints_release_update_result_and_reports_errors(self) -> None:
        class FakeUpdateResult:
            def to_dict(self) -> dict[str, object]:
                return {
                    "release_tag": "v0.2.0-alpha-abc123",
                    "release_name": "0.2.0-alpha - (...bc123)",
                    "wheel_name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "sha256": "abc123",
                    "installed": False,
                }

        stdout = io.StringIO()
        with (
            patch(
                "aethermesh_core.cli.update_from_latest_release",
                return_value=FakeUpdateResult(),
            ) as updater,
            contextlib.redirect_stdout(stdout),
        ):
            exit_code = main(["update", "--dry-run"])

        self.assertEqual(exit_code, 0)
        updater.assert_called_once_with(dry_run=True, release_url=None)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["release_tag"], "v0.2.0-alpha-abc123")
        self.assertEqual(payload["installed"], False)

        stderr = io.StringIO()
        with (
            patch(
                "aethermesh_core.cli.update_from_latest_release",
                side_effect=ReleaseUpdateError("network sad"),
            ),
            contextlib.redirect_stderr(stderr),
        ):
            exit_code = main(["update"])

        self.assertEqual(exit_code, 1)
        self.assertIn("network sad", stderr.getvalue())

    def test_simulate_local_prints_deterministic_json_shape(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = main(["simulate-local"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["nodes"], ["local-node-a", "local-node-b"])
        self.assertEqual(
            payload["assignments"],
            [
                {"job_id": "echo-1", "node_id": "local-node-a"},
                {"job_id": "text-stats-1", "node_id": "local-node-b"},
                {"job_id": "keyword-extract-1", "node_id": "local-node-a"},
                {"job_id": "echo-2", "node_id": "local-node-b"},
                {"job_id": "echo-3", "node_id": "local-node-a"},
            ],
        )
        self.assertEqual(len(payload["results"]), 5)
        self.assertEqual(payload["results"][0]["status"], "completed")
        self.assertEqual(payload["results"][1]["output"]["word_count"], 4)
        self.assertEqual(payload["results"][1]["output"]["line_count"], 2)
        self.assertEqual(
            payload["results"][1]["output"]["normalized_preview"],
            "hello mesh hello node",
        )
        self.assertEqual(len(payload["messages"]), 22)
        self.assertEqual(payload["messages"][0]["message_id"], "msg-0001")
        self.assertEqual(payload["messages"][0]["message_type"], "node_heartbeat")
        self.assertEqual(payload["messages"][0]["correlation_id"], None)
        self.assertEqual(payload["messages"][0]["recipient_node_id"], None)
        self.assertEqual(
            payload["messages"][0]["payload"],
            {
                "node_id": "local-node-a",
                "status": "available",
                "heartbeat_sequence": 1,
                "heartbeat_count": 1,
            },
        )
        self.assertEqual(payload["messages"][1]["message_type"], "node_heartbeat")
        self.assertEqual(payload["messages"][2]["message_type"], "job_assigned")
        self.assertEqual(payload["messages"][2]["correlation_id"], "echo-1")
        self.assertEqual(payload["messages"][2]["recipient_node_id"], "local-node-a")
        self.assertEqual(payload["messages"][3]["message_type"], "job_result_reported")
        self.assertEqual(payload["messages"][4]["message_type"], "job_validated")
        self.assertEqual(
            payload["messages"][5]["message_type"], "contribution_recorded"
        )
        self.assertEqual(payload["validations"][0]["valid"], True)
        self.assertEqual(
            payload["validation_summary"], {"valid": 5, "invalid": 0, "unsupported": 0}
        )
        self.assertEqual(payload["summaries"][0]["node_id"], "local-node-a")
        self.assertEqual(
            payload["node_roster"],
            [
                {
                    "node_id": "local-node-a",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 1,
                    "heartbeat_count": 1,
                    "assigned_jobs": 3,
                    "contribution_units": 4,
                },
                {
                    "node_id": "local-node-b",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 2,
                    "heartbeat_count": 1,
                    "assigned_jobs": 2,
                    "contribution_units": 3,
                },
            ],
        )
        self.assertEqual(
            payload["totals"],
            {
                "nodes": 2,
                "jobs": 5,
                "results": 5,
                "completed_jobs": 5,
                "failed_jobs": 0,
                "valid_results": 5,
                "invalid_results": 0,
                "unsupported_results": 0,
                "contribution_units": 7,
            },
        )

    def test_run_local_batch_prints_manifest_backed_simulation_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a", "local-node-b"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            },
                            {
                                "job_id": "text-stats-1",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh\nhello node"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["nodes"], ["local-node-a", "local-node-b"])
        self.assertEqual(
            payload["assignments"],
            [
                {"job_id": "echo-1", "node_id": "local-node-a"},
                {"job_id": "text-stats-1", "node_id": "local-node-b"},
            ],
        )
        self.assertEqual(payload["results"][0]["output"], "hello mesh")
        self.assertEqual(payload["results"][1]["output"]["word_count"], 4)
        self.assertEqual(
            payload["validation_summary"], {"valid": 2, "invalid": 0, "unsupported": 0}
        )
        self.assertEqual(payload["totals"]["jobs"], 2)
        self.assertEqual(payload["totals"]["contribution_units"], 3)

    def test_run_local_batch_skips_offline_manifest_nodes_and_prints_roster(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {"node_id": "local-node-a", "status": "available"},
                            {"node_id": "local-node-b", "status": "offline"},
                            "local-node-c",
                        ],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "one"},
                            },
                            {
                                "job_id": "echo-2",
                                "job_type": "echo",
                                "payload": {"message": "two"},
                            },
                            {
                                "job_id": "echo-3",
                                "job_type": "echo",
                                "payload": {"message": "three"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(
            payload["nodes"], ["local-node-a", "local-node-b", "local-node-c"]
        )
        self.assertEqual(
            payload["assignments"],
            [
                {"job_id": "echo-1", "node_id": "local-node-a"},
                {"job_id": "echo-2", "node_id": "local-node-c"},
                {"job_id": "echo-3", "node_id": "local-node-a"},
            ],
        )
        self.assertEqual(
            payload["node_roster"],
            [
                {
                    "node_id": "local-node-a",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 1,
                    "heartbeat_count": 1,
                    "assigned_jobs": 2,
                    "contribution_units": 2,
                },
                {
                    "node_id": "local-node-b",
                    "status": "offline",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 0,
                    "heartbeat_count": 0,
                    "assigned_jobs": 0,
                    "contribution_units": 0,
                },
                {
                    "node_id": "local-node-c",
                    "status": "available",
                    "capabilities": [
                        "echo",
                        "keyword_extract",
                        "text_chunk",
                        "text_embed",
                        "text_retrieve",
                        "text_stats",
                    ],
                    "heartbeat_sequence": 2,
                    "heartbeat_count": 1,
                    "assigned_jobs": 1,
                    "contribution_units": 1,
                },
            ],
        )

    def test_run_local_batch_all_offline_returns_nonzero_without_writing_ledger(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [{"node_id": "local-node-a", "status": "offline"}],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            ledger_exists = ledger_path.exists()

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("no available nodes for local job assignment", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertFalse(ledger_exists)

    def test_run_local_batch_manifest_error_returns_nonzero_without_traceback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            manifest_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("error: manifest JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_run_local_batch_rejects_unsupported_job_type(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {"job_id": "bad-1", "job_type": "unknown", "payload": {}}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("manifest jobs[0].job_type must be one of", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_run_local_batch_does_not_write_ledger_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])
            payload = json.loads(stdout.getvalue())
            ledger_exists = ledger_path.exists()

        self.assertEqual(exit_code, 0)
        self.assertFalse(ledger_exists)
        self.assertNotIn("ledger_path", payload)
        self.assertNotIn("persisted_ledger_summaries", payload)

    def test_run_local_batch_does_not_write_message_log_by_default(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-messages.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["run-local-batch", "--manifest", str(manifest_path)])
            payload = json.loads(stdout.getvalue())
            message_log_exists = message_log_path.exists()

        self.assertEqual(exit_code, 0)
        self.assertFalse(message_log_exists)
        self.assertNotIn("message_log_path", payload)

    def test_run_local_batch_writes_message_log_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "nested" / "local-messages.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a", "local-node-b"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            },
                            {
                                "job_id": "text-stats-1",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh\nhello node"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            persisted = json.loads(message_log_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["message_log_path"], str(message_log_path))
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(persisted["metadata"]["manifest_path"], str(manifest_path))
        self.assertEqual(persisted["metadata"]["message_count"], 10)
        self.assertEqual(persisted["metadata"]["job_count"], 2)
        self.assertEqual(persisted["metadata"]["completed_count"], 2)
        self.assertEqual(persisted["metadata"]["failed_count"], 0)
        self.assertEqual(persisted["metadata"]["total_contribution_units"], 3)
        self.assertEqual(
            [message["message_id"] for message in persisted["messages"]],
            [f"msg-{index:04d}" for index in range(1, 11)],
        )
        self.assertEqual(persisted["messages"][0]["message_type"], "node_heartbeat")
        self.assertEqual(persisted["messages"][2]["message_type"], "job_assigned")
        self.assertEqual(
            persisted["messages"][3]["message_type"], "job_result_reported"
        )
        self.assertEqual(persisted["messages"][4]["message_type"], "job_validated")
        self.assertEqual(
            persisted["messages"][5]["message_type"], "contribution_recorded"
        )

    def test_run_local_batch_message_log_write_error_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-messages.json"
            message_log_path.mkdir()
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("could not write message log file", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_run_local_batch_persists_json_ledger_and_accumulates(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a", "local-node-b"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            },
                            {
                                "job_id": "text-stats-1",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh\nhello node"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            first_stdout = io.StringIO()
            with contextlib.redirect_stdout(first_stdout):
                first_exit = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            second_stdout = io.StringIO()
            with contextlib.redirect_stdout(second_stdout):
                second_exit = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))

        first_payload = json.loads(first_stdout.getvalue())
        second_payload = json.loads(second_stdout.getvalue())
        self.assertEqual(first_exit, 0)
        self.assertEqual(second_exit, 0)
        self.assertEqual(first_payload["ledger_path"], str(ledger_path))
        self.assertEqual(len(first_payload["results"]), 2)
        self.assertEqual(
            first_payload["persisted_ledger_summaries"][0]["total_result_count"], 1
        )
        self.assertEqual(
            first_payload["persisted_ledger_summaries"][1]["total_contribution_units"],
            2,
        )
        self.assertEqual(
            second_payload["persisted_ledger_summaries"][0]["total_result_count"], 2
        )
        self.assertEqual(
            second_payload["persisted_ledger_summaries"][1]["total_contribution_units"],
            4,
        )
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(len(persisted["records"]), 4)
        self.assertEqual(persisted["records"][0]["node_id"], "local-node-a")
        self.assertEqual(persisted["records"][0]["validation_valid"], True)
        self.assertEqual(persisted["records"][0]["validation_reason"], "ok")
        self.assertEqual(persisted["records"][0]["job_type"], "echo")
        self.assertEqual(persisted["records"][0]["manifest_ref"], str(manifest_path))
        self.assertEqual(persisted["records"][1]["node_id"], "local-node-b")
        self.assertEqual(persisted["records"][1]["validation_valid"], True)
        self.assertEqual(persisted["records"][1]["validation_reason"], "ok")
        self.assertEqual(persisted["records"][1]["job_type"], "text_stats")
        self.assertEqual(persisted["records"][1]["manifest_ref"], str(manifest_path))

    def test_run_local_batch_preserves_existing_ledger_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            ledger_path.write_text(
                json.dumps({"version": 1, "records": [], "owner": "local-dev"}),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(persisted["owner"], "local-dev")
        self.assertEqual(len(persisted["records"]), 1)

    def test_run_local_batch_persists_invalid_completed_result_with_zero_units(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {"job_id": "echo-1", "job_type": "echo", "payload": {}}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(
            payload["validation_summary"], {"valid": 0, "invalid": 1, "unsupported": 0}
        )
        self.assertEqual(persisted["records"][0]["status"], "completed")
        self.assertEqual(persisted["records"][0]["contribution_units"], 0)
        self.assertEqual(persisted["records"][0]["validation_valid"], False)
        self.assertEqual(
            persisted["records"][0]["validation_reason"], "missing_payload_message"
        )
        self.assertEqual(persisted["records"][0]["job_type"], "echo")

    def test_run_local_batch_unsupported_job_type_does_not_write_audit_record(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {
                                "node_id": "local-node-a",
                                "capabilities": ["unknown"],
                            }
                        ],
                        "jobs": [
                            {"job_id": "bad-1", "job_type": "unknown", "payload": {}}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("manifest jobs[0].job_type must be one of", stderr.getvalue())
        self.assertFalse(ledger_path.exists())

    def test_run_local_batch_malformed_ledger_returns_nonzero_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            ledger_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            ledger_contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(ledger_contents, "not-json")

    def test_run_demo_command_still_prints_parseable_result(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                [
                    "run-demo",
                    "--node-id",
                    "local-demo-node",
                    "--message",
                    "hello mesh",
                    "--include-ledger",
                ]
            )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(stdout.getvalue(), json.dumps(payload, sort_keys=True) + "\n")
        self.assertEqual(set(payload), {"result", "validation", "ledger_summary"})
        self.assertEqual(payload["result"]["node_id"], "local-demo-node")
        self.assertEqual(payload["result"]["output"], "hello mesh")
        self.assertEqual(payload["validation"]["valid"], True)
        self.assertEqual(payload["validation"]["reason"], "ok")
        self.assertEqual(payload["ledger_summary"]["total_contribution_units"], 1)

    def test_run_demo_default_output_shape_stays_single_result(self) -> None:
        stdout = io.StringIO()

        with contextlib.redirect_stdout(stdout):
            exit_code = main(
                ["run-demo", "--node-id", "local-demo-node", "--message", "hello mesh"]
            )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(stdout.getvalue(), json.dumps(payload, sort_keys=True) + "\n")
        self.assertEqual(
            set(payload),
            {"job_id", "node_id", "status", "output", "error", "contribution_units"},
        )
        self.assertEqual(payload["job_id"], "demo-echo")
        self.assertEqual(payload["node_id"], "local-demo-node")
        self.assertEqual(payload["output"], "hello mesh")
        self.assertNotIn("validation", payload)
        self.assertNotIn("ledger_summary", payload)

    def test_run_demo_defaults_to_deterministic_machine_node_id(self) -> None:
        stdout = io.StringIO()

        with (
            patch(
                "aethermesh_core.cli.deterministic_machine_node_id",
                return_value="local-stable-machine",
            ),
            patch(
                "aethermesh_core.cli.deterministic_machine_node_name",
                return_value="lucid-beacon-tensor-vault_localx",
            ),
            contextlib.redirect_stdout(stdout),
        ):
            exit_code = main(["run-demo", "--message", "hello mesh"])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["node_id"], "local-stable-machine")
        self.assertEqual(payload["node_name"], "lucid-beacon-tensor-vault_localx")
        self.assertEqual(payload["output"], "hello mesh")

    def test_run_demo_ephemeral_identity_is_opt_in_and_does_not_touch_persistent_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            persistent_stdout = io.StringIO()
            with contextlib.redirect_stdout(persistent_stdout):
                persistent_exit = main(
                    ["run-demo", "--identity-path", str(identity_path)]
                )
            original_identity = identity_path.read_text(encoding="utf-8")
            first_stdout = io.StringIO()
            first_stderr = io.StringIO()
            with (
                contextlib.redirect_stdout(first_stdout),
                contextlib.redirect_stderr(first_stderr),
            ):
                first_exit = main(["run-demo", "--ephemeral-identity"])
            second_stdout = io.StringIO()
            with (
                contextlib.redirect_stdout(second_stdout),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                second_exit = main(["run-demo", "--ephemeral-identity"])
            after_identity = identity_path.read_text(encoding="utf-8")

        first_payload = json.loads(first_stdout.getvalue())
        second_payload = json.loads(second_stdout.getvalue())
        self.assertEqual(persistent_exit, 0)
        self.assertEqual(first_exit, 0)
        self.assertEqual(second_exit, 0)
        self.assertEqual(after_identity, original_identity)
        self.assertTrue(first_payload["ephemeral"])
        self.assertEqual(first_payload["artifact_mode"], "ephemeral_test")
        self.assertRegex(first_payload["node_id"], r"^node-[0-9a-f]{32}$")
        self.assertNotEqual(first_payload["node_id"], second_payload["node_id"])
        self.assertIn("ephemeral test identity mode active", first_stderr.getvalue())

    def test_run_demo_ephemeral_identity_rejects_persistent_identity_path(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            stderr = io.StringIO()
            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(
                    [
                        "run-demo",
                        "--ephemeral-identity",
                        "--identity-path",
                        str(identity_path),
                    ]
                )

            self.assertFalse(identity_path.exists())

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("cannot be combined", stderr.getvalue())

    def test_run_demo_ephemeral_identity_env_flag_is_opt_in(self) -> None:
        stdout = io.StringIO()
        with (
            patch.dict("os.environ", {"AETHERMESH_EPHEMERAL_IDENTITY": "true"}),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(io.StringIO()),
        ):
            exit_code = main(["run-demo"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["ephemeral"])
        self.assertEqual(payload["artifact_mode"], "ephemeral_test")

    def test_run_demo_ephemeral_identity_marks_persisted_ledger_as_test_only(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            stdout = io.StringIO()
            with (
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                exit_code = main(
                    [
                        "run-demo",
                        "--ephemeral-identity",
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertTrue(payload["ephemeral"])
        self.assertTrue(ledger["ephemeral"])
        self.assertEqual(ledger["artifact_mode"], "ephemeral_test")

    def test_run_demo_identity_path_creates_and_reuses_node_id(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            first_stdout = io.StringIO()
            with contextlib.redirect_stdout(first_stdout):
                first_exit = main(
                    [
                        "run-demo",
                        "--identity-path",
                        str(identity_path),
                        "--message",
                        "hello mesh",
                    ]
                )
            second_stdout = io.StringIO()
            with contextlib.redirect_stdout(second_stdout):
                second_exit = main(
                    [
                        "run-demo",
                        "--identity-path",
                        str(identity_path),
                        "--message",
                        "hello again",
                    ]
                )
            persisted = json.loads(identity_path.read_text(encoding="utf-8"))

        first_payload = json.loads(first_stdout.getvalue())
        second_payload = json.loads(second_stdout.getvalue())
        self.assertEqual(first_exit, 0)
        self.assertEqual(second_exit, 0)
        self.assertRegex(first_payload["node_id"], r"^[0-9a-f]{64}$")
        self.assertRegex(
            first_payload["node_name"], r"^[a-z]+-[a-z]+-[a-z]+-[a-z]+_[a-f0-9]{6}$"
        )
        self.assertFalse(first_payload["node_id"].startswith("local-"))
        self.assertEqual(second_payload["node_id"], first_payload["node_id"])
        self.assertEqual(second_payload["node_name"], first_payload["node_name"])
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(persisted["node"]["node_id"], first_payload["node_id"])
        self.assertEqual(persisted["node"]["node_name"], first_payload["node_name"])
        self.assertNotIn("validation", first_payload)
        self.assertNotIn("ledger_summary", first_payload)

    def test_run_demo_node_id_and_identity_path_conflict_returns_cli_error(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            stderr = io.StringIO()

            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(
                    [
                        "run-demo",
                        "--node-id",
                        "local-demo-node",
                        "--identity-path",
                        str(identity_path),
                    ]
                )

            self.assertFalse(identity_path.exists())

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("mutually exclusive", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_run_demo_malformed_identity_path_returns_cli_error_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            identity_path.write_text("not-json", encoding="utf-8")
            stderr = io.StringIO()

            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(["run-demo", "--identity-path", str(identity_path)])

            identity_contents = identity_path.read_text(encoding="utf-8")

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("identity JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(identity_contents, "not-json")

    def test_run_demo_unsupported_identity_version_returns_cli_error_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            original = json.dumps({"version": 2, "node": {"node_id": "local-future"}})
            identity_path.write_text(original, encoding="utf-8")
            stderr = io.StringIO()

            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(["run-demo", "--identity-path", str(identity_path)])

            identity_contents = identity_path.read_text(encoding="utf-8")

        self.assertEqual(cm.exception.code, 2)
        self.assertIn("identity JSON must contain version 1", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(identity_contents, original)

    def test_reset_identity_requires_confirmation_and_records_audit(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            identity_path = Path(temp_dir) / "local-node.json"
            quarantine_dir = Path(temp_dir) / "quarantine"
            main(["run-demo", "--identity-path", str(identity_path)])
            original = json.loads(identity_path.read_text(encoding="utf-8"))
            stderr = io.StringIO()

            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(["reset-identity", "--identity-path", str(identity_path)])

            self.assertEqual(cm.exception.code, 2)
            self.assertIn("requires --confirm-reset", stderr.getvalue())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with (
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                exit_code = main(
                    [
                        "reset-identity",
                        "--identity-path",
                        str(identity_path),
                        "--confirm-reset",
                        "--reason",
                        "local recovery",
                        "--quarantine-dir",
                        str(quarantine_dir),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            reset_document = json.loads(identity_path.read_text(encoding="utf-8"))
            backup_document = json.loads(Path(payload["backup_path"]).read_text())
            receipt_document = json.loads(
                Path(payload["audit_receipt_path"]).read_text()
            )

        self.assertEqual(exit_code, 0)
        self.assertIn(
            "lineage and contribution attribution continuity", stderr.getvalue()
        )
        self.assertEqual(payload["previous_node_id"], original["node"]["node_id"])
        self.assertEqual(payload["new_node_id"], reset_document["node"]["node_id"])
        self.assertEqual(payload["new_node_id"], payload["previous_node_id"])
        self.assertEqual(
            reset_document["node"]["node_name"], original["node"]["node_name"]
        )
        self.assertEqual(backup_document, original)
        receipt = receipt_document["reset_receipts"][0]
        self.assertEqual(receipt["previous_node_id"], payload["previous_node_id"])
        self.assertEqual(receipt["new_node_id"], payload["new_node_id"])
        self.assertEqual(receipt["reason"], "local recovery")

    def test_run_demo_persists_json_ledger_and_accumulates_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            first_stdout = io.StringIO()
            with contextlib.redirect_stdout(first_stdout):
                first_exit = main(
                    [
                        "run-demo",
                        "--node-id",
                        "local-demo-node",
                        "--message",
                        "hello mesh",
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            second_stdout = io.StringIO()
            with contextlib.redirect_stdout(second_stdout):
                second_exit = main(
                    [
                        "run-demo",
                        "--node-id",
                        "local-demo-node",
                        "--message",
                        "hello mesh",
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            persisted = json.loads(ledger_path.read_text(encoding="utf-8"))

        first_payload = json.loads(first_stdout.getvalue())
        second_payload = json.loads(second_stdout.getvalue())
        self.assertEqual(first_exit, 0)
        self.assertEqual(second_exit, 0)
        self.assertEqual(first_payload["result"]["output"], "hello mesh")
        self.assertEqual(first_payload["validation"]["valid"], True)
        self.assertEqual(first_payload["ledger_path"], str(ledger_path))
        self.assertEqual(
            first_payload["persisted_ledger_summary"]["total_result_count"], 1
        )
        self.assertEqual(
            second_payload["persisted_ledger_summary"]["total_result_count"], 2
        )
        self.assertEqual(
            second_payload["persisted_ledger_summary"]["total_contribution_units"], 2
        )
        self.assertEqual(len(persisted["records"]), 2)
        self.assertEqual(persisted["records"][0]["node_id"], "local-demo-node")
        self.assertEqual(persisted["records"][0]["contribution_units"], 1)
        self.assertEqual(persisted["records"][0]["validation_valid"], True)
        self.assertEqual(persisted["records"][0]["validation_reason"], "ok")
        self.assertEqual(persisted["records"][0]["job_type"], "echo")

    def test_run_demo_malformed_ledger_path_returns_cli_error(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text("not-json", encoding="utf-8")
            stderr = io.StringIO()

            with (
                contextlib.redirect_stderr(stderr),
                self.assertRaises(SystemExit) as cm,
            ):
                main(["run-demo", "--ledger-path", str(ledger_path)])

            self.assertEqual(cm.exception.code, 2)
            self.assertIn("ledger JSON is malformed", stderr.getvalue())
            self.assertEqual(ledger_path.read_text(encoding="utf-8"), "not-json")

    def test_ledger_summary_prints_deterministic_json_without_writing(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            original = json.dumps(
                {
                    "version": 1,
                    "records": [
                        {
                            "node_id": "node-b",
                            "job_id": "job-1",
                            "status": "completed",
                            "contribution_units": 10,
                            "message": "ok",
                        },
                        {
                            "node_id": "node-a",
                            "job_id": "job-2",
                            "status": "failed",
                            "contribution_units": 0,
                            "message": "boom",
                        },
                        {
                            "node_id": "node-a",
                            "job_id": "job-3",
                            "status": "completed",
                            "contribution_units": 5,
                            "message": "ok",
                        },
                    ],
                    "owner": "local-dev",
                },
                sort_keys=True,
            )
            ledger_path.write_text(original, encoding="utf-8")
            before_mtime = ledger_path.stat().st_mtime_ns
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

            after_mtime = ledger_path.stat().st_mtime_ns
            after_contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 0)
        self.assertEqual(after_mtime, before_mtime)
        self.assertEqual(after_contents, original)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "ledger_path": str(ledger_path),
                "record_count": 3,
                "completed_result_count": 2,
                "failed_result_count": 1,
                "total_contribution_units": 15,
                "nodes": [
                    {
                        "node_id": "node-a",
                        "record_count": 2,
                        "completed_result_count": 1,
                        "failed_result_count": 1,
                        "total_contribution_units": 5,
                    },
                    {
                        "node_id": "node-b",
                        "record_count": 1,
                        "completed_result_count": 1,
                        "failed_result_count": 0,
                        "total_contribution_units": 10,
                    },
                ],
            },
        )

    def test_ledger_summary_missing_file_returns_nonzero_without_creating_it(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "missing" / "ledger.json"
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

            ledger_exists = ledger_path.exists()
            parent_exists = ledger_path.parent.exists()

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger file does not exist", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertFalse(ledger_exists)
        self.assertFalse(parent_exists)

    def test_ledger_summary_malformed_ledger_returns_nonzero_without_overwrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

            contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(contents, "not-json")

    def test_ledger_summary_non_object_ledger_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            ledger_path.write_text("[]", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger JSON must be an object", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_ledger_summary_unsupported_version_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            original = json.dumps({"version": 2, "records": []})
            ledger_path.write_text(original, encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

            contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger JSON must contain version 1", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(contents, original)

    def test_ledger_summary_invalid_record_returns_nonzero(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            ledger_path = Path(temp_dir) / "ledger.json"
            original = json.dumps(
                {"version": 1, "records": [{"node_id": "node-a"}]}, sort_keys=True
            )
            ledger_path.write_text(original, encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["ledger-summary", "--ledger-path", str(ledger_path)])

            contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger record field 'job_id' must be str", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(contents, original)

    def test_dispatch_local_batch_writes_assignment_only_message_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            ledger_path = Path(temp_dir) / "ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {
                                "node_id": "local-node-a",
                                "capabilities": [
                                    "echo",
                                    "keyword_extract",
                                    "text_chunk",
                                    "text_embed",
                                    "text_stats",
                                ],
                            },
                            {
                                "node_id": "local-node-b",
                                "status": "offline",
                                "capabilities": ["echo"],
                            },
                            {"node_id": "local-node-c", "capabilities": ["echo"]},
                        ],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "one"},
                            },
                            {
                                "job_id": "stats-1",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh"},
                            },
                            {
                                "job_id": "echo-2",
                                "job_type": "echo",
                                "payload": {"message": "two"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "dispatch-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            persisted = json.loads(message_log_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertFalse(ledger_path.exists())
        self.assertEqual(payload["command"], "dispatch-local-batch")
        self.assertEqual(payload["manifest_path"], str(manifest_path))
        self.assertEqual(payload["message_log_path"], str(message_log_path))
        self.assertEqual(payload["job_count"], 3)
        self.assertEqual(payload["assignment_count"], 3)
        self.assertEqual(payload["message_count"], 5)
        self.assertEqual(payload["assigned_node_ids"], ["local-node-a", "local-node-c"])
        self.assertEqual(
            payload["assignments"],
            [
                {"job_id": "echo-1", "node_id": "local-node-a"},
                {"job_id": "stats-1", "node_id": "local-node-a"},
                {"job_id": "echo-2", "node_id": "local-node-c"},
            ],
        )
        self.assertEqual(payload["nodes"][1]["status"], "offline")
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(persisted["metadata"]["source"], "dispatch-local-batch")
        self.assertEqual(persisted["metadata"]["assignment_count"], 3)
        self.assertEqual(
            [message["message_type"] for message in persisted["messages"]],
            [
                "node_heartbeat",
                "node_heartbeat",
                "job_assigned",
                "job_assigned",
                "job_assigned",
            ],
        )
        self.assertEqual(
            [message["sender_node_id"] for message in persisted["messages"][:2]],
            ["local-node-a", "local-node-c"],
        )
        self.assertEqual(
            [
                message["payload"]["capabilities"]
                for message in persisted["messages"][:2]
            ],
            [
                ["echo", "keyword_extract", "text_chunk", "text_embed", "text_stats"],
                ["echo"],
            ],
        )
        self.assertNotIn("job_result_reported", json.dumps(persisted))
        self.assertNotIn("contribution_recorded", json.dumps(persisted))

    def test_dispatch_local_batch_failure_does_not_overwrite_existing_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            original = json.dumps({"version": 1, "messages": [], "keep": True})
            message_log_path.write_text(original, encoding="utf-8")
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {"node_id": "local-node-a", "capabilities": ["echo"]}
                        ],
                        "jobs": [
                            {
                                "job_id": "stats-1",
                                "job_type": "text_stats",
                                "payload": {},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "dispatch-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )
            contents = message_log_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("job_id=stats-1 job_type=text_stats", stderr.getvalue())
        self.assertEqual(contents, original)

    def test_dispatch_local_batch_malformed_manifest_does_not_create_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            manifest_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "dispatch-local-batch",
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("manifest JSON is malformed", stderr.getvalue())
        self.assertFalse(message_log_path.exists())

    def test_dispatch_peer_batch_uses_peer_log_roster_and_ignores_manifest_nodes(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            peer_log_path = Path(temp_dir) / "peer-heartbeats.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {"node_id": "manifest-node", "capabilities": ["text_stats"]}
                        ],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            peer_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "peer-node-a",
                                "recipient_node_id": None,
                                "payload": {
                                    "node_id": "peer-node-a",
                                    "status": "available",
                                    "heartbeat_sequence": 1,
                                    "heartbeat_count": 1,
                                    "capabilities": ["echo"],
                                },
                                "correlation_id": None,
                            },
                            {
                                "message_id": "msg-0002",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "peer-node-b",
                                "recipient_node_id": None,
                                "payload": {
                                    "node_id": "peer-node-b",
                                    "status": "offline",
                                    "heartbeat_sequence": 2,
                                    "heartbeat_count": 1,
                                    "capabilities": ["echo"],
                                },
                                "correlation_id": None,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "dispatch-peer-batch",
                        "--peer-log-path",
                        str(peer_log_path),
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            persisted = json.loads(message_log_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["command"], "dispatch-peer-batch")
        self.assertEqual(payload["peer_log_path"], str(peer_log_path))
        self.assertEqual(payload["manifest_path"], str(manifest_path))
        self.assertEqual(payload["message_log_path"], str(message_log_path))
        self.assertEqual(payload["job_count"], 1)
        self.assertEqual(payload["assignment_count"], 1)
        self.assertEqual(payload["assigned_node_ids"], ["peer-node-a"])
        self.assertEqual(payload["nodes"][0]["node_id"], "peer-node-a")
        self.assertEqual(payload["nodes"][1]["status"], "offline")
        self.assertEqual(persisted["metadata"]["source"], "dispatch-local-batch")
        self.assertNotIn("peer_log_path", persisted["metadata"])
        self.assertNotIn("manifest-node", json.dumps(persisted))
        self.assertEqual(
            [message["message_type"] for message in persisted["messages"]],
            ["node_heartbeat", "job_assigned"],
        )

    def test_dispatch_peer_batch_failure_does_not_overwrite_existing_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            peer_log_path = Path(temp_dir) / "peer-heartbeats.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            original = json.dumps({"version": 1, "messages": [], "keep": True})
            message_log_path.write_text(original, encoding="utf-8")
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["manifest-node"],
                        "jobs": [
                            {
                                "job_id": "stats-1",
                                "job_type": "text_stats",
                                "payload": {},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            peer_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "peer-node-a",
                                "recipient_node_id": None,
                                "payload": {
                                    "node_id": "peer-node-a",
                                    "status": "available",
                                    "heartbeat_sequence": 1,
                                    "heartbeat_count": 1,
                                    "capabilities": ["echo"],
                                },
                                "correlation_id": None,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "dispatch-peer-batch",
                        "--peer-log-path",
                        str(peer_log_path),
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )
            contents = message_log_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("job_id=stats-1 job_type=text_stats", stderr.getvalue())
        self.assertEqual(contents, original)

    def test_dispatch_peer_batch_empty_peer_log_does_not_create_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            peer_log_path = Path(temp_dir) / "peer-heartbeats.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "jobs": [
                            {"job_id": "echo-1", "job_type": "echo", "payload": {}}
                        ],
                    }
                ),
                encoding="utf-8",
            )
            peer_log_path.write_text(
                json.dumps({"version": 1, "messages": []}), encoding="utf-8"
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "dispatch-peer-batch",
                        "--peer-log-path",
                        str(peer_log_path),
                        "--manifest",
                        str(manifest_path),
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("no heartbeat peers", stderr.getvalue())
        self.assertFalse(message_log_path.exists())

    def test_peer_summary_prints_heartbeat_derived_peers(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "messages.json"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "node-b",
                                "recipient_node_id": None,
                                "payload": {
                                    "node_id": "node-b",
                                    "status": "available",
                                    "heartbeat_sequence": 2,
                                    "heartbeat_count": 1,
                                    "capabilities": ["echo"],
                                },
                                "correlation_id": None,
                            },
                            {
                                "message_id": "msg-0002",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "node-a",
                                "recipient_node_id": None,
                                "payload": {
                                    "node_id": "node-a",
                                    "status": "available",
                                    "heartbeat_sequence": 1,
                                    "heartbeat_count": 1,
                                },
                                "correlation_id": None,
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    ["peer-summary", "--message-log-path", str(message_log_path)]
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(
            json.loads(stdout.getvalue()),
            {
                "peers": [
                    {
                        "node_id": "node-a",
                        "status": "available",
                        "heartbeat_count": 1,
                        "last_heartbeat_sequence": 1,
                        "capabilities": [],
                    },
                    {
                        "node_id": "node-b",
                        "status": "available",
                        "heartbeat_count": 1,
                        "last_heartbeat_sequence": 2,
                        "capabilities": ["echo"],
                    },
                ]
            },
        )

    def test_peer_summary_malformed_heartbeat_returns_nonzero_without_rewrite(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "messages.json"
            original = json.dumps(
                {
                    "version": 1,
                    "messages": [
                        {
                            "message_id": "msg-0001",
                            "message_type": "node_heartbeat",
                            "sender_node_id": "node-a",
                            "recipient_node_id": None,
                            "payload": {
                                "node_id": "node-a",
                                "status": "available",
                                "heartbeat_sequence": "1",
                            },
                            "correlation_id": None,
                        }
                    ],
                },
                sort_keys=True,
            )
            message_log_path.write_text(original, encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    ["peer-summary", "--message-log-path", str(message_log_path)]
                )
            contents = message_log_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("heartbeat_sequence", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(contents, original)

    def test_process_local_inbox_consumes_dispatch_log(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-dispatch.json"
            ledger_path = Path(temp_dir) / "local-ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "dispatch-local-batch",
                            "--manifest",
                            str(manifest_path),
                            "--message-log-path",
                            str(message_log_path),
                        ]
                    ),
                    0,
                )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            persisted_ledger = json.loads(ledger_path.read_text(encoding="utf-8"))

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["processed_assignment_count"], 1)
        self.assertEqual(payload["validation_outcomes"][0]["valid"], True)
        self.assertEqual(payload["ledger_summary"]["total_units"], 1)
        self.assertEqual(len(persisted_ledger["records"]), 1)
        self.assertEqual(persisted_ledger["records"][0]["job_id"], "echo-1")

    def test_process_local_inbox_replays_message_log_for_requested_node(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            message_log_path = Path(temp_dir) / "local-messages.json"
            ledger_path = Path(temp_dir) / "local-ledger.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a", "local-node-b"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "one"},
                            },
                            {
                                "job_id": "echo-2",
                                "job_type": "echo",
                                "payload": {"message": "two"},
                            },
                            {
                                "job_id": "text-stats-1",
                                "job_type": "text_stats",
                                "payload": {"text": "hello mesh"},
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "run-local-batch",
                            "--manifest",
                            str(manifest_path),
                            "--message-log-path",
                            str(message_log_path),
                        ]
                    ),
                    0,
                )
            before_message_log = message_log_path.read_text(encoding="utf-8")
            before_mtime = message_log_path.stat().st_mtime_ns
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )

            after_message_log = message_log_path.read_text(encoding="utf-8")
            after_mtime = message_log_path.stat().st_mtime_ns
            persisted_ledger = json.loads(ledger_path.read_text(encoding="utf-8"))

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["command"], "process-local-inbox")
        self.assertEqual(payload["node_id"], "local-node-a")
        self.assertEqual(payload["processed_assignment_count"], 2)
        self.assertEqual(payload["ignored_message_ids"], ["msg-0006", "msg-0014"])
        self.assertEqual(
            payload["emitted_messages"],
            [
                {
                    "id": "msg-0015",
                    "type": "job_result_reported",
                    "sender": "local-node-a",
                    "recipient": "local-ledger",
                },
                {
                    "id": "msg-0016",
                    "type": "job_validated",
                    "sender": "local-node-a",
                    "recipient": "local-ledger",
                },
                {
                    "id": "msg-0017",
                    "type": "contribution_recorded",
                    "sender": "local-ledger",
                    "recipient": "local-node-a",
                },
                {
                    "id": "msg-0018",
                    "type": "job_result_reported",
                    "sender": "local-node-a",
                    "recipient": "local-ledger",
                },
                {
                    "id": "msg-0019",
                    "type": "job_validated",
                    "sender": "local-node-a",
                    "recipient": "local-ledger",
                },
                {
                    "id": "msg-0020",
                    "type": "contribution_recorded",
                    "sender": "local-ledger",
                    "recipient": "local-node-a",
                },
            ],
        )
        self.assertEqual(
            payload["validation_outcomes"],
            [
                {
                    "job_id": "echo-1",
                    "valid": True,
                    "credited_units": 1,
                    "reason": "ok",
                },
                {
                    "job_id": "text-stats-1",
                    "valid": True,
                    "credited_units": 2,
                    "reason": "ok",
                },
            ],
        )
        self.assertEqual(
            payload["ledger_summary"],
            {
                "path": str(ledger_path),
                "total_units": 3,
                "node_units": 3,
                "record_count": 2,
            },
        )
        self.assertEqual(len(persisted_ledger["records"]), 2)
        self.assertEqual(
            [record["node_id"] for record in persisted_ledger["records"]],
            ["local-node-a", "local-node-a"],
        )
        self.assertEqual(after_message_log, before_message_log)
        self.assertEqual(after_mtime, before_mtime)
        self.assertNotIn("output_message_log_path", payload)
        self.assertNotIn("final_message_count", payload)

    def test_process_local_inbox_writes_output_message_log_when_requested(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            output_message_log_path = (
                Path(temp_dir) / "local-node-a-output-messages.json"
            )
            ledger_path = Path(temp_dir) / "local-ledger.json"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "metadata": {"message_count": 3},
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "local-node-a",
                                "recipient_node_id": None,
                                "payload": {"node_id": "local-node-a"},
                                "correlation_id": None,
                            },
                            {
                                "message_id": "msg-0002",
                                "message_type": "job_assigned",
                                "sender_node_id": "local-scheduler",
                                "recipient_node_id": "local-node-a",
                                "payload": {
                                    "job_id": "echo-1",
                                    "job_type": "echo",
                                    "payload": {"message": "hello mesh"},
                                },
                                "correlation_id": "echo-1",
                            },
                            {
                                "message_id": "msg-0003",
                                "message_type": "contribution_recorded",
                                "sender_node_id": "local-ledger",
                                "recipient_node_id": "local-node-a",
                                "payload": {
                                    "job_id": "older-job",
                                    "contribution_units": 1,
                                },
                                "correlation_id": "older-job",
                            },
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                        "--output-message-log-path",
                        str(output_message_log_path),
                    ]
                )
            persisted = json.loads(output_message_log_path.read_text(encoding="utf-8"))

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(
            payload["output_message_log_path"], str(output_message_log_path)
        )
        self.assertEqual(payload["final_message_count"], 6)
        self.assertEqual(payload["processed_assignment_count"], 1)
        self.assertEqual(payload["ignored_message_ids"], ["msg-0003"])
        self.assertEqual(persisted["version"], 1)
        self.assertEqual(persisted["metadata"]["source"], "process-local-inbox")
        self.assertEqual(persisted["metadata"]["node_id"], "local-node-a")
        self.assertEqual(
            persisted["metadata"]["source_message_log_path"], str(message_log_path)
        )
        self.assertEqual(persisted["metadata"]["ledger_path"], str(ledger_path))
        self.assertEqual(persisted["metadata"]["message_count"], 6)
        self.assertEqual(persisted["metadata"]["replayed_message_count"], 3)
        self.assertEqual(persisted["metadata"]["emitted_message_count"], 3)
        self.assertEqual(
            [message["message_id"] for message in persisted["messages"]],
            ["msg-0001", "msg-0002", "msg-0003", "msg-0004", "msg-0005", "msg-0006"],
        )
        self.assertEqual(
            persisted["messages"][3]["message_type"], "job_result_reported"
        )
        self.assertEqual(persisted["messages"][4]["message_type"], "job_validated")
        self.assertEqual(
            persisted["messages"][5]["message_type"], "contribution_recorded"
        )

    def test_process_local_inbox_output_message_log_ordering_is_deterministic(
        self,
    ) -> None:
        document = {
            "version": 1,
            "messages": [
                {
                    "message_id": "msg-0001",
                    "message_type": "job_assigned",
                    "sender_node_id": "local-scheduler",
                    "recipient_node_id": "local-node-a",
                    "payload": {
                        "job_id": "echo-1",
                        "job_type": "echo",
                        "payload": {"message": "one"},
                    },
                    "correlation_id": "echo-1",
                },
                {
                    "message_id": "msg-0002",
                    "message_type": "job_assigned",
                    "sender_node_id": "local-scheduler",
                    "recipient_node_id": "local-node-a",
                    "payload": {
                        "job_id": "echo-2",
                        "job_type": "echo",
                        "payload": {"message": "two"},
                    },
                    "correlation_id": "echo-2",
                },
            ],
        }
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            first_output_path = Path(temp_dir) / "first-output.json"
            second_output_path = Path(temp_dir) / "second-output.json"
            message_log_path.write_text(json.dumps(document), encoding="utf-8")

            stable_version_metadata = capture_version_metadata(
                captured_at="2026-07-08T00:00:00+00:00"
            )
            with patch(
                "aethermesh_core.node_service.capture_version_metadata",
                return_value=stable_version_metadata,
            ):
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(
                        main(
                            [
                                "process-local-inbox",
                                "--node-id",
                                "local-node-a",
                                "--message-log-path",
                                str(message_log_path),
                                "--output-message-log-path",
                                str(first_output_path),
                            ]
                        ),
                        0,
                    )
                with contextlib.redirect_stdout(io.StringIO()):
                    self.assertEqual(
                        main(
                            [
                                "process-local-inbox",
                                "--node-id",
                                "local-node-a",
                                "--message-log-path",
                                str(message_log_path),
                                "--output-message-log-path",
                                str(second_output_path),
                            ]
                        ),
                        0,
                    )
            first = json.loads(first_output_path.read_text(encoding="utf-8"))
            second = json.loads(second_output_path.read_text(encoding="utf-8"))

        self.assertEqual(first["messages"], second["messages"])
        self.assertEqual(
            [message["message_id"] for message in first["messages"]],
            [
                "msg-0001",
                "msg-0002",
                "msg-0003",
                "msg-0004",
                "msg-0005",
                "msg-0006",
                "msg-0007",
                "msg-0008",
            ],
        )

    def test_process_local_inbox_unknown_node_returns_zero_without_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            ledger_path = Path(temp_dir) / "local-ledger.json"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "node_heartbeat",
                                "sender_node_id": "local-node-a",
                                "recipient_node_id": None,
                                "payload": {"node_id": "local-node-a"},
                                "correlation_id": None,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "unknown-node",
                        "--message-log-path",
                        str(message_log_path),
                    ]
                )

        payload = json.loads(stdout.getvalue())
        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["processed_assignment_count"], 0)
        self.assertEqual(payload["ignored_message_ids"], [])
        self.assertEqual(payload["emitted_messages"], [])
        self.assertEqual(payload["validation_outcomes"], [])
        self.assertFalse(ledger_path.exists())
        self.assertNotIn("ledger_summary", payload)

    def test_process_local_inbox_invalid_node_state_returns_nonzero_without_writes(
        self,
    ) -> None:
        cases = [
            ("malformed", "not-json", "node state JSON is malformed"),
            (
                "wrong-node",
                json.dumps(
                    {
                        "version": 1,
                        "node_id": "other-node",
                        "processed_message_ids": [],
                        "processed_assignment_count": 0,
                    }
                ),
                "belongs to node 'other-node'",
            ),
            (
                "unsupported-version",
                json.dumps(
                    {
                        "version": 2,
                        "node_id": "local-node-a",
                        "processed_message_ids": [],
                        "processed_assignment_count": 0,
                    }
                ),
                "must contain version 1",
            ),
        ]
        for name, state_contents, expected_error in cases:
            with (
                self.subTest(name=name),
                tempfile.TemporaryDirectory() as temp_dir,
            ):
                message_log_path = Path(temp_dir) / "local-messages.json"
                ledger_path = Path(temp_dir) / "local-ledger.json"
                output_message_log_path = Path(temp_dir) / "output-messages.json"
                state_path = Path(temp_dir) / "node-state.json"
                message_log_path.write_text(
                    json.dumps(
                        {
                            "version": 1,
                            "messages": [
                                {
                                    "message_id": "msg-0001",
                                    "message_type": "job_assigned",
                                    "sender_node_id": "local-scheduler",
                                    "recipient_node_id": "local-node-a",
                                    "payload": {
                                        "job_id": "echo-1",
                                        "job_type": "echo",
                                        "payload": {"message": "hello mesh"},
                                    },
                                    "correlation_id": "echo-1",
                                }
                            ],
                        }
                    ),
                    encoding="utf-8",
                )
                output_message_log_path.write_text("keep output", encoding="utf-8")
                state_path.write_text(state_contents, encoding="utf-8")
                original_state = state_path.read_text(encoding="utf-8")
                stdout = io.StringIO()
                stderr = io.StringIO()

                with (
                    contextlib.redirect_stdout(stdout),
                    contextlib.redirect_stderr(stderr),
                ):
                    exit_code = main(
                        [
                            "process-local-inbox",
                            "--node-id",
                            "local-node-a",
                            "--message-log-path",
                            str(message_log_path),
                            "--ledger-path",
                            str(ledger_path),
                            "--output-message-log-path",
                            str(output_message_log_path),
                            "--node-state-path",
                            str(state_path),
                        ]
                    )

                self.assertEqual(exit_code, 1)
                self.assertEqual(stdout.getvalue(), "")
                self.assertIn(expected_error, stderr.getvalue())
                self.assertFalse(ledger_path.exists())
                self.assertEqual(
                    output_message_log_path.read_text(encoding="utf-8"),
                    "keep output",
                )
                self.assertEqual(state_path.read_text(encoding="utf-8"), original_state)

    def test_process_local_inbox_with_node_state_resumes_without_duplicate_ledger_records(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            ledger_path = Path(temp_dir) / "local-ledger.json"
            state_path = Path(temp_dir) / "node-state.json"
            message_log_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "messages": [
                            {
                                "message_id": "msg-0001",
                                "message_type": "job_assigned",
                                "sender_node_id": "local-scheduler",
                                "recipient_node_id": "local-node-a",
                                "payload": {
                                    "job_id": "echo-1",
                                    "job_type": "echo",
                                    "payload": {"message": "hello mesh"},
                                },
                                "correlation_id": "echo-1",
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            first_stdout = io.StringIO()
            with contextlib.redirect_stdout(first_stdout):
                first_exit = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                        "--node-state-path",
                        str(state_path),
                    ]
                )
            first_payload = json.loads(first_stdout.getvalue())
            first_state = json.loads(state_path.read_text(encoding="utf-8"))

            second_stdout = io.StringIO()
            with contextlib.redirect_stdout(second_stdout):
                second_exit = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                        "--node-state-path",
                        str(state_path),
                    ]
                )
            second_payload = json.loads(second_stdout.getvalue())
            second_state = json.loads(state_path.read_text(encoding="utf-8"))
            ledger = json.loads(ledger_path.read_text(encoding="utf-8"))

        self.assertEqual(first_exit, 0)
        self.assertEqual(first_payload["processed_assignment_count"], 1)
        self.assertEqual(first_payload["skipped_processed_message_ids"], [])
        self.assertEqual(first_state["node_id"], "local-node-a")
        self.assertEqual(first_state["processed_message_ids"], ["msg-0001"])
        self.assertEqual(first_state["processed_assignment_count"], 1)
        self.assertEqual(second_exit, 0)
        self.assertEqual(second_payload["processed_assignment_count"], 0)
        self.assertEqual(second_payload["ignored_message_ids"], ["msg-0001"])
        self.assertEqual(second_payload["skipped_processed_message_ids"], ["msg-0001"])
        self.assertEqual(second_payload["processed_message_ids"], ["msg-0001"])
        self.assertEqual(second_state, first_state)
        self.assertEqual(len(ledger["records"]), 1)

    def test_process_local_inbox_invalid_message_log_does_not_write_output_log(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            output_message_log_path = Path(temp_dir) / "output-messages.json"
            output_message_log_path.write_text("keep me", encoding="utf-8")
            message_log_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--output-message-log-path",
                        str(output_message_log_path),
                    ]
                )
            output_contents = output_message_log_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("message log JSON is malformed", stderr.getvalue())
        self.assertEqual(output_contents, "keep me")

    def test_process_local_inbox_invalid_message_log_returns_nonzero_without_ledger(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            message_log_path = Path(temp_dir) / "local-messages.json"
            ledger_path = Path(temp_dir) / "local-ledger.json"
            message_log_path.write_text("not-json", encoding="utf-8")
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "process-local-inbox",
                        "--node-id",
                        "local-node-a",
                        "--message-log-path",
                        str(message_log_path),
                        "--ledger-path",
                        str(ledger_path),
                    ]
                )

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("message log JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertFalse(ledger_path.exists())

    def test_run_local_flow_happy_path_with_example_manifest(self) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        manifest_path = repo_root / "examples" / "local-batch.json"
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "flow"
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            payload = json.loads(stdout.getvalue())
            ledger = json.loads(
                (output_dir / "ledger.json").read_text(encoding="utf-8")
            )
            receipts = json.loads(
                (output_dir / "receipts.json").read_text(encoding="utf-8")
            )
            flow_log = json.loads(
                (output_dir / "flow-message-log.json").read_text(encoding="utf-8")
            )
            dispatch_exists = (output_dir / "dispatch-message-log.json").exists()
            flow_log_exists = (output_dir / "flow-message-log.json").exists()
            node_a_state_exists = (
                output_dir / "node-state" / "local-node-a.json"
            ).exists()
            node_c_state_exists = (
                output_dir / "node-state" / "local-node-c.json"
            ).exists()
            node_a_log_exists = (
                output_dir / "worker-message-logs" / "local-node-a.json"
            ).exists()
            node_c_log_exists = (
                output_dir / "worker-message-logs" / "local-node-c.json"
            ).exists()
            offline_state_exists = (
                output_dir / "node-state" / "local-node-b.json"
            ).exists()
            offline_log_exists = (
                output_dir / "worker-message-logs" / "local-node-b.json"
            ).exists()

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["command"], "run-local-flow")
        self.assertEqual(payload["manifest_path"], str(manifest_path))
        self.assertEqual(payload["output_dir"], str(output_dir))
        self.assertEqual(
            payload["available_node_ids"], ["local-node-a", "local-node-c"]
        )
        self.assertEqual(payload["offline_node_ids"], ["local-node-b"])
        self.assertEqual(
            payload["processed_node_ids"], ["local-node-a", "local-node-c"]
        )
        self.assertEqual(payload["processed_assignment_count"], 6)
        self.assertEqual(payload["skipped_processed_assignment_count"], 0)
        self.assertEqual(payload["total_contribution_units"], 12)
        self.assertEqual(
            payload["flow_message_log_path"], str(output_dir / "flow-message-log.json")
        )
        self.assertEqual(payload["receipts_path"], str(output_dir / "receipts.json"))
        self.assertNotIn("transport_dir", payload)
        self.assertNotIn("transport_inbox_count", payload)
        self.assertNotIn("transport_inbox_paths", payload)
        self.assertEqual(payload["receipt_count"], 6)
        self.assertEqual(payload["flow_message_count"], 26)
        self.assertEqual(payload["flow_emitted_message_count"], 18)
        self.assertEqual(payload["ledger_summary"]["record_count"], 6)
        self.assertEqual(len(payload["node_results"]), 2)
        self.assertTrue(dispatch_exists)
        self.assertTrue(flow_log_exists)
        self.assertTrue(node_a_state_exists)
        self.assertTrue(node_c_state_exists)
        self.assertTrue(node_a_log_exists)
        self.assertTrue(node_c_log_exists)
        self.assertFalse(offline_state_exists)
        self.assertFalse(offline_log_exists)
        self.assertEqual(len(ledger["records"]), 6)
        self.assertEqual(receipts["version"], 1)
        self.assertEqual(receipts["run_source"], "run-local-flow")
        self.assertEqual(
            receipts["version_metadata_ref"],
            receipts["receipts"][0]["version_metadata_ref"],
        )
        self.assertIn("runtime_version", receipts["version_metadata"])
        self.assertEqual(len(receipts["receipts"]), 6)
        self.assertEqual(
            [receipt["assignment_message_id"] for receipt in receipts["receipts"]],
            ["msg-0003", "msg-0005", "msg-0006", "msg-0007", "msg-0008", "msg-0004"],
        )
        first_receipt_hash = receipts["receipts"][0]["result_hash"]
        self.assertRegex(first_receipt_hash, r"^[0-9a-f]{64}$")
        self.assertEqual(
            receipts["receipts"][0],
            {
                "job_id": "echo-1",
                "job_type": "echo",
                "node_id": "local-node-a",
                "assignment_message_id": "msg-0003",
                "correlation_id": "echo-1",
                "result_message_id": "msg-0009",
                "validation_message_id": "msg-0010",
                "contribution_message_id": "msg-0011",
                "result_status": "completed",
                "result_hash": first_receipt_hash,
                "validation": {"valid": True, "reason": "ok"},
                "version_metadata_ref": receipts["version_metadata_ref"],
                "credited_units": 1,
                "output_summary": {"value": "hello mesh"},
            },
        )
        self.assertEqual(
            receipts["receipts"][2]["output_summary"],
            {
                "character_count": 63,
                "chunk_count": 3,
                "chunks": [
                    {"character_count": 20, "index": 0, "text": "AetherMesh prepares "},
                    {
                        "character_count": 22,
                        "index": 1,
                        "text": "local text chunks for ",
                    },
                    {
                        "character_count": 21,
                        "index": 2,
                        "text": "future AI processing.",
                    },
                ],
            },
        )
        self.assertEqual(flow_log["metadata"]["source"], "run-local-flow")
        self.assertEqual(flow_log["metadata"]["manifest_path"], str(manifest_path))
        self.assertEqual(flow_log["metadata"]["dispatch_message_count"], 8)
        self.assertEqual(flow_log["metadata"]["emitted_message_count"], 18)
        self.assertEqual(flow_log["metadata"]["message_count"], 26)
        self.assertEqual(
            flow_log["metadata"]["available_node_ids"], ["local-node-a", "local-node-c"]
        )
        self.assertEqual(flow_log["metadata"]["offline_node_ids"], ["local-node-b"])
        self.assertEqual(flow_log["metadata"]["processed_assignment_count"], 6)
        self.assertEqual(flow_log["metadata"]["skipped_processed_assignment_count"], 0)
        self.assertEqual(
            [message["message_type"] for message in flow_log["messages"]],
            [
                "node_heartbeat",
                "node_heartbeat",
                "job_assigned",
                "job_assigned",
                "job_assigned",
                "job_assigned",
                "job_assigned",
                "job_assigned",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
                "job_result_reported",
                "job_validated",
                "contribution_recorded",
            ],
        )

    def test_run_local_flow_ephemeral_identity_marks_receipts_and_uses_fresh_roster(
        self,
    ) -> None:
        repo_root = Path(__file__).resolve().parents[1]
        manifest_path = repo_root / "examples" / "local-batch.json"
        with tempfile.TemporaryDirectory() as temp_dir:
            first_output_dir = Path(temp_dir) / "first-flow"
            second_output_dir = Path(temp_dir) / "second-flow"
            first_stdout = io.StringIO()
            first_stderr = io.StringIO()
            with (
                contextlib.redirect_stdout(first_stdout),
                contextlib.redirect_stderr(first_stderr),
            ):
                first_exit = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(first_output_dir),
                        "--ephemeral-identity",
                    ]
                )
            second_stdout = io.StringIO()
            with (
                contextlib.redirect_stdout(second_stdout),
                contextlib.redirect_stderr(io.StringIO()),
            ):
                second_exit = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(second_output_dir),
                        "--ephemeral-identity",
                    ]
                )
            first_receipts = json.loads(
                (first_output_dir / "receipts.json").read_text(encoding="utf-8")
            )
            first_ledger = json.loads(
                (first_output_dir / "ledger.json").read_text(encoding="utf-8")
            )
            first_flow_log = json.loads(
                (first_output_dir / "flow-message-log.json").read_text(encoding="utf-8")
            )
            first_dispatch_log = json.loads(
                (first_output_dir / "dispatch-message-log.json").read_text(
                    encoding="utf-8"
                )
            )
            first_payload = json.loads(first_stdout.getvalue())
            first_worker_log = json.loads(
                (
                    first_output_dir
                    / "worker-message-logs"
                    / f"{first_payload['available_node_ids'][0]}.json"
                ).read_text(encoding="utf-8")
            )

        second_payload = json.loads(second_stdout.getvalue())
        self.assertEqual(first_exit, 0)
        self.assertEqual(second_exit, 0)
        self.assertTrue(first_payload["ephemeral"])
        self.assertEqual(first_payload["artifact_mode"], "ephemeral_test")
        self.assertEqual(first_payload["roster_source"], "ephemeral_test_identity")
        self.assertNotEqual(
            first_payload["available_node_ids"], second_payload["available_node_ids"]
        )
        self.assertTrue(first_receipts["ephemeral"])
        self.assertEqual(first_receipts["artifact_mode"], "ephemeral_test")
        self.assertTrue(first_receipts["receipts"][0]["ephemeral"])
        self.assertEqual(
            first_receipts["receipts"][0]["artifact_mode"], "ephemeral_test"
        )
        self.assertTrue(first_ledger["ephemeral"])
        self.assertEqual(first_ledger["artifact_mode"], "ephemeral_test")
        self.assertRegex(
            first_receipts["receipts"][0]["node_id"], r"^node-[0-9a-f]{32}$"
        )
        self.assertNotIn("local-node-a", first_payload["available_node_ids"])
        self.assertEqual(
            first_flow_log["metadata"]["available_node_ids"],
            first_payload["available_node_ids"],
        )
        for message_log in (first_dispatch_log, first_flow_log, first_worker_log):
            self.assertTrue(message_log["metadata"]["ephemeral"])
            self.assertEqual(message_log["metadata"]["artifact_mode"], "ephemeral_test")
        self.assertIn("ephemeral test identity mode active", first_stderr.getvalue())

    def test_run_local_flow_skips_offline_nodes_but_reports_them(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            output_dir = Path(temp_dir) / "flow"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": [
                            {"node_id": "local-node-a", "status": "offline"},
                            {"node_id": "local-node-b", "status": "available"},
                        ],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "one"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )

            payload = json.loads(stdout.getvalue())
            available_state_exists = (
                output_dir / "node-state" / "local-node-b.json"
            ).exists()
            offline_state_exists = (
                output_dir / "node-state" / "local-node-a.json"
            ).exists()
            offline_log_exists = (
                output_dir / "worker-message-logs" / "local-node-a.json"
            ).exists()

        self.assertEqual(exit_code, 0)
        self.assertEqual(payload["available_node_ids"], ["local-node-b"])
        self.assertEqual(payload["offline_node_ids"], ["local-node-a"])
        self.assertEqual(payload["processed_node_ids"], ["local-node-b"])
        self.assertEqual(payload["processed_assignment_count"], 1)
        self.assertTrue(available_state_exists)
        self.assertFalse(offline_state_exists)
        self.assertFalse(offline_log_exists)

    def test_run_local_flow_rerun_is_idempotent_with_node_state(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            output_dir = Path(temp_dir) / "flow"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            first_stdout = io.StringIO()
            with contextlib.redirect_stdout(first_stdout):
                first_exit = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            second_stdout = io.StringIO()
            with contextlib.redirect_stdout(second_stdout):
                second_exit = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            first_payload = json.loads(first_stdout.getvalue())
            first_receipts = json.loads(
                (output_dir / "receipts.json").read_text(encoding="utf-8")
            )
            second_payload = json.loads(second_stdout.getvalue())
            ledger = json.loads(
                (output_dir / "ledger.json").read_text(encoding="utf-8")
            )
            flow_log = json.loads(
                (output_dir / "flow-message-log.json").read_text(encoding="utf-8")
            )
            second_receipts = json.loads(
                (output_dir / "receipts.json").read_text(encoding="utf-8")
            )

        self.assertEqual(first_exit, 0)
        self.assertEqual(first_payload["processed_assignment_count"], 1)
        self.assertEqual(first_payload["skipped_processed_assignment_count"], 0)
        self.assertEqual(second_exit, 0)
        self.assertEqual(second_payload["processed_assignment_count"], 0)
        self.assertEqual(second_payload["skipped_processed_assignment_count"], 1)
        self.assertEqual(second_payload["receipt_count"], 1)
        self.assertEqual(second_payload["flow_emitted_message_count"], 0)
        self.assertEqual(second_payload["flow_message_count"], 2)
        self.assertEqual(second_payload["node_results"][0]["ignored_message_count"], 1)
        self.assertEqual(len(ledger["records"]), 1)
        self.assertEqual(second_receipts, first_receipts)
        self.assertEqual(len(second_receipts["receipts"]), 1)
        self.assertEqual(flow_log["metadata"]["emitted_message_count"], 0)
        self.assertEqual(flow_log["metadata"]["message_count"], 2)

    def test_run_local_flow_malformed_existing_receipts_do_not_overwrite_dispatch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            output_dir = Path(temp_dir) / "flow"
            output_dir.mkdir()
            dispatch_path = output_dir / "dispatch-message-log.json"
            flow_log_path = output_dir / "flow-message-log.json"
            receipts_path = output_dir / "receipts.json"
            dispatch_original = json.dumps({"version": 1, "messages": [], "keep": True})
            flow_log_original = json.dumps({"version": 1, "messages": [], "keep": True})
            dispatch_path.write_text(dispatch_original, encoding="utf-8")
            flow_log_path.write_text(flow_log_original, encoding="utf-8")
            receipts_path.write_text("not-json", encoding="utf-8")
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            dispatch_contents = dispatch_path.read_text(encoding="utf-8")
            flow_log_contents = flow_log_path.read_text(encoding="utf-8")
            receipts_contents = receipts_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("receipt JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(dispatch_contents, dispatch_original)
        self.assertEqual(flow_log_contents, flow_log_original)
        self.assertEqual(receipts_contents, "not-json")

    def test_run_local_flow_malformed_existing_ledger_does_not_overwrite_dispatch(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            output_dir = Path(temp_dir) / "flow"
            output_dir.mkdir()
            dispatch_path = output_dir / "dispatch-message-log.json"
            flow_log_path = output_dir / "flow-message-log.json"
            ledger_path = output_dir / "ledger.json"
            dispatch_original = json.dumps({"version": 1, "messages": [], "keep": True})
            flow_log_original = json.dumps({"version": 1, "messages": [], "keep": True})
            dispatch_path.write_text(dispatch_original, encoding="utf-8")
            flow_log_path.write_text(flow_log_original, encoding="utf-8")
            ledger_path.write_text("not-json", encoding="utf-8")
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            dispatch_contents = dispatch_path.read_text(encoding="utf-8")
            flow_log_contents = flow_log_path.read_text(encoding="utf-8")
            ledger_contents = ledger_path.read_text(encoding="utf-8")

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("ledger JSON is malformed", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertEqual(dispatch_contents, dispatch_original)
        self.assertEqual(flow_log_contents, flow_log_original)
        self.assertEqual(ledger_contents, "not-json")

    def test_audit_local_flow_prints_success_json(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            manifest_path = Path(temp_dir) / "local-batch.json"
            output_dir = Path(temp_dir) / "flow"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            run_stdout = io.StringIO()
            with contextlib.redirect_stdout(run_stdout):
                run_exit = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            stdout = io.StringIO()

            with contextlib.redirect_stdout(stdout):
                exit_code = main(["audit-local-flow", "--output-dir", str(output_dir)])

        self.assertEqual(run_exit, 0)
        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["ok"], True)
        self.assertEqual(payload["output_dir"], str(output_dir))
        self.assertEqual(
            payload["artifacts"],
            {
                "dispatch_message_log": str(output_dir / "dispatch-message-log.json"),
                "flow_message_log": str(output_dir / "flow-message-log.json"),
                "ledger": str(output_dir / "ledger.json"),
                "receipts": str(output_dir / "receipts.json"),
            },
        )
        self.assertEqual(
            payload["counts"],
            {
                "dispatch_messages": 2,
                "flow_messages": 5,
                "receipts": 1,
                "ledger_records": 1,
                "total_contribution_units": 1,
                "credited_receipt_units": 1,
            },
        )

    def test_audit_local_flow_missing_receipts_returns_nonzero_without_traceback(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "flow"
            output_dir.mkdir()
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(["audit-local-flow", "--output-dir", str(output_dir)])

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("error:", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())

    def test_aggregate_local_flow_writes_artifact_and_prints_summary(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "manifest.json"
            output_dir = root / "flow"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(
                    main(
                        [
                            "run-local-flow",
                            "--manifest",
                            str(manifest_path),
                            "--output-dir",
                            str(output_dir),
                        ]
                    ),
                    0,
                )
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    ["aggregate-local-flow", "--output-dir", str(output_dir)]
                )
            payload = json.loads(stdout.getvalue())
            aggregate_path = output_dir / "aggregate-result.json"
            aggregate_exists = aggregate_path.exists()

        self.assertEqual(exit_code, 0)
        self.assertEqual(stderr.getvalue(), "")
        self.assertEqual(stdout.getvalue(), json.dumps(payload, sort_keys=True) + "\n")
        self.assertEqual(payload["command"], "aggregate-local-flow")
        self.assertEqual(payload["aggregate_path"], str(aggregate_path))
        self.assertEqual(payload["counts"]["accepted_results"], 1)
        self.assertTrue(aggregate_exists)

    def test_aggregate_local_flow_audit_failure_returns_nonzero_without_writing(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            output_dir = Path(temp_dir) / "flow"
            output_dir.mkdir()
            aggregate_path = output_dir / "aggregate-result.json"
            stdout = io.StringIO()
            stderr = io.StringIO()

            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                exit_code = main(
                    ["aggregate-local-flow", "--output-dir", str(output_dir)]
                )
            aggregate_exists = aggregate_path.exists()

        self.assertEqual(exit_code, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertIn("error:", stderr.getvalue())
        self.assertNotIn("Traceback", stderr.getvalue())
        self.assertFalse(aggregate_exists)

    def test_cli_json_stdout_is_sorted_for_core_commands(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "batch.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            commands = [
                ["run-demo", "--node-id", "node-z", "--message", "hello"],
                ["simulate-local"],
                ["run-local-batch", "--manifest", str(manifest_path)],
                [
                    "dispatch-local-batch",
                    "--manifest",
                    str(manifest_path),
                    "--message-log-path",
                    str(root / "dispatch.json"),
                ],
            ]
            for command in commands:
                with self.subTest(command=command):
                    stdout = io.StringIO()
                    with contextlib.redirect_stdout(stdout):
                        exit_code = main(command)
                    self.assertEqual(exit_code, 0)
                    payload = json.loads(stdout.getvalue())
                    self.assertEqual(
                        stdout.getvalue(), json.dumps(payload, sort_keys=True) + "\n"
                    )

    def test_run_local_flow_stdout_and_artifact_contracts_are_stable(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            manifest_path = root / "manifest.json"
            output_dir = root / "deep" / "flow"
            manifest_path.write_text(
                json.dumps(
                    {
                        "version": 1,
                        "nodes": ["local-node-a"],
                        "jobs": [
                            {
                                "job_id": "echo-1",
                                "job_type": "echo",
                                "payload": {"message": "hello mesh"},
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            stdout = io.StringIO()
            with contextlib.redirect_stdout(stdout):
                exit_code = main(
                    [
                        "run-local-flow",
                        "--manifest",
                        str(manifest_path),
                        "--output-dir",
                        str(output_dir),
                    ]
                )
            payload = json.loads(stdout.getvalue())
            expected_files = [
                output_dir / "dispatch-message-log.json",
                output_dir / "flow-message-log.json",
                output_dir / "ledger.json",
                output_dir / "receipts.json",
                output_dir / "node-state" / "local-node-a.json",
                output_dir / "worker-message-logs" / "local-node-a.json",
            ]

            self.assertEqual(exit_code, 0)
            self.assertEqual(
                stdout.getvalue(), json.dumps(payload, sort_keys=True) + "\n"
            )
            self.assertEqual(
                set(payload),
                {
                    "available_node_ids",
                    "command",
                    "dispatch_message_log_path",
                    "dispatch_summary",
                    "flow_emitted_message_count",
                    "flow_message_count",
                    "flow_message_log_path",
                    "ignored_message_count",
                    "ledger_path",
                    "ledger_summary",
                    "manifest_path",
                    "node_results",
                    "offline_node_ids",
                    "output_dir",
                    "processed_assignment_count",
                    "processed_node_ids",
                    "receipt_count",
                    "receipts_path",
                    "skipped_processed_assignment_count",
                    "total_contribution_units",
                },
            )
            self.assertEqual(
                payload["dispatch_message_log_path"], str(expected_files[0])
            )
            self.assertEqual(payload["flow_message_log_path"], str(expected_files[1]))
            self.assertEqual(payload["ledger_path"], str(expected_files[2]))
            self.assertEqual(payload["receipts_path"], str(expected_files[3]))
            self.assertEqual(
                payload["node_results"][0]["node_state_path"], str(expected_files[4])
            )
            self.assertEqual(
                payload["node_results"][0]["worker_message_log_path"],
                str(expected_files[5]),
            )
            for path in expected_files:
                self.assertTrue(path.exists(), path)
            self.assertEqual(
                sorted(path.name for path in output_dir.iterdir()),
                [
                    "dispatch-message-log.json",
                    "flow-message-log.json",
                    "ledger.json",
                    "node-state",
                    "receipts.json",
                    "worker-message-logs",
                ],
            )
            self.assertEqual(
                sorted(path.name for path in (output_dir / "node-state").iterdir()),
                ["local-node-a.json"],
            )
            self.assertEqual(
                sorted(
                    path.name for path in (output_dir / "worker-message-logs").iterdir()
                ),
                ["local-node-a.json"],
            )


if __name__ == "__main__":
    unittest.main()
