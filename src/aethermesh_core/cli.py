"""Command-line interface for the local AetherMesh prototype."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from urllib.parse import quote

from aethermesh_core.aggregation import AggregationError, aggregate_local_flow
from aethermesh_core.contribution import score_validated_contribution
from aethermesh_core.dispatch import dispatch_local_batch
from aethermesh_core.flow_audit import FlowAuditError, audit_local_flow
from aethermesh_core.identity import (
    IdentityPersistenceError,
    deterministic_machine_node_id,
    deterministic_machine_node_name,
    load_or_create_identity,
    reset_identity,
)
from aethermesh_core.job_manifest import (
    ManifestError,
    load_job_manifest,
    load_manifest_jobs,
)
from aethermesh_core.ledger import (
    ContributionLedger,
    LedgerPersistenceError,
    load_existing_ledger_document,
    load_ledger_document,
    save_ledger_document,
)
from aethermesh_core.local_transport import (
    LocalTransportError,
    collect_local_outboxes,
    load_local_inbox,
    local_inbox_path,
    materialize_local_inboxes,
    write_local_inbox,
    write_local_outbox,
)
from aethermesh_core.message_bus import LocalMessageBus
from aethermesh_core.message_log import (
    MessageLogPersistenceError,
    build_dispatch_message_log_document,
    build_flow_message_log_document,
    build_message_log_document,
    build_replayed_message_log_document,
    load_message_log_messages,
    load_worker_emitted_messages,
    write_message_log,
)
from aethermesh_core.messages import MeshMessage
from aethermesh_core.models import Job, NodeIdentity
from aethermesh_core.node_announcement import NodeAnnouncementError, announce_local_node
from aethermesh_core.node_service import InboxProcessResult, LocalNodeService
from aethermesh_core.node_state import (
    LocalNodeProcessingState,
    NodeStatePersistenceError,
    load_node_processing_state,
    save_node_processing_state,
)
from aethermesh_core.peer_registry import (
    PeerRegistryError,
    peer_summary_document,
    scheduled_nodes_from_peer_log,
)
from aethermesh_core.receipts import (
    ReceiptPersistenceError,
    build_receipt_document,
    load_receipt_document_if_exists,
    write_receipt_document,
)
from aethermesh_core.release_update import (
    ReleaseUpdateError,
    update_from_latest_release,
)
from aethermesh_core.runner import LocalRunner
from aethermesh_core.runtime import (
    LocalRestartError,
    LocalShutdownError,
    LocalStartupError,
    LocalValidationError,
    restart_local_node_runtime,
    start_local_node_runtime,
    stop_local_node_runtime,
    validate_local_node_results,
)
from aethermesh_core.scheduler import LocalScheduler, NodeStatus, ScheduledNode
from aethermesh_core.simulation import run_local_simulation
from aethermesh_core.validation import validate_job_result
from aethermesh_core.version_metadata import capture_version_metadata

start_local_node = start_local_node_runtime


@dataclass(frozen=True)
class InboxReplayRequest:
    node_id: str
    message_log_path: str | None = None
    transport_dir: str | None = None
    ledger_path: str | None = None
    output_message_log_path: str | None = None
    node_state_path: str | None = None
    write_transport_outbox: bool = False
    ephemeral_identity: bool = False
    version_metadata: dict[str, object] | None = None


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="aethermesh-core")
    subcommands = parser.add_subparsers(dest="command", required=True)

    demo = subcommands.add_parser(
        "run-demo", help="Run one local echo job and print its JSON result."
    )
    demo.add_argument(
        "--node-id",
        default=None,
        help="Node id to use for the demo. Defaults to a deterministic machine id.",
    )
    demo.add_argument(
        "--identity-path",
        default=None,
        help="Opt in to JSON-file-backed local node identity persistence.",
    )
    demo.add_argument(
        "--ephemeral-identity",
        action="store_true",
        help="Use a fresh test-only node identity for this run without touching persistent identity files.",
    )
    demo.add_argument(
        "--message",
        default="hello mesh",
        help="Message payload for the local echo job.",
    )
    demo.add_argument(
        "--include-ledger",
        action="store_true",
        help="Include an in-memory contribution summary for the demo result.",
    )
    demo.add_argument(
        "--ledger-path",
        default=None,
        help="Opt in to JSON-file-backed local contribution ledger persistence.",
    )

    reset = subcommands.add_parser(
        "reset-identity",
        help="Explicitly reset a persisted local node identity after quarantining the old one.",
    )
    reset.add_argument(
        "--identity-path",
        required=True,
        help="Path to the existing persisted local node identity JSON file.",
    )
    reset.add_argument(
        "--confirm-reset",
        action="store_true",
        help="Required acknowledgement that lineage and attribution continuity may be affected.",
    )
    reset.add_argument(
        "--reason",
        default=None,
        help="Optional local audit reason recorded in the reset receipt.",
    )
    reset.add_argument(
        "--quarantine-dir",
        default=None,
        help="Optional directory for quarantined previous identity material and receipts.",
    )
    reset.add_argument(
        "--audit-receipt-path",
        default=None,
        help="Optional path for the local identity reset audit receipt JSON.",
    )
    reset.add_argument(
        "--rotate-creator-identity",
        action="store_true",
        help="Also rotate creator_node_id; default preserves creator identity for attribution continuity.",
    )

    subcommands.add_parser(
        "simulate-local",
        help="Run a deterministic local multi-node simulation and print JSON.",
    )

    update = subcommands.add_parser(
        "update",
        help="Install the latest AetherMesh wheel from the newest GitHub release.",
    )
    update.add_argument(
        "--dry-run",
        action="store_true",
        help="Download and verify the latest release wheel without installing it.",
    )
    update.add_argument(
        "--release-url",
        default=None,
        help="Override the GitHub latest-release API URL for update discovery.",
    )

    startup = subcommands.add_parser(
        "start-local-node",
        help="Initialize one local-only node runtime with identity, manifest, receipt, and lineage artifacts.",
    )
    startup.add_argument(
        "--runtime-dir",
        required=True,
        help="Local runtime directory for identity, manifests, receipts, logs, work, and lineage artifacts.",
    )
    startup.add_argument(
        "--reset-creator-identity",
        action="store_true",
        help="Explicitly rotate the creator identity before startup; normal startup preserves it.",
    )

    shutdown = subcommands.add_parser(
        "shutdown-local-node",
        help="Gracefully stop a local-only node runtime and persist final state.",
    )
    shutdown.add_argument(
        "--runtime-dir",
        required=True,
        help="Local runtime directory containing identity, manifests, receipts, logs, work, and lineage artifacts.",
    )
    shutdown.add_argument(
        "--timeout-seconds",
        type=float,
        default=5.0,
        help="Bounded graceful shutdown timeout for local persistence checks.",
    )

    restart = subcommands.add_parser(
        "restart-local-node",
        help="Cleanly stop and restart a local-only node runtime from persisted state.",
    )
    restart.add_argument(
        "--runtime-dir",
        required=True,
        help="Local runtime directory containing identity, manifests, receipts, logs, work, and lineage artifacts.",
    )
    restart.add_argument(
        "--timeout-seconds",
        type=float,
        default=5.0,
        help="Bounded graceful shutdown timeout before local restart.",
    )

    batch = subcommands.add_parser(
        "run-local-batch",
        help="Run a manifest-backed local multi-node job batch and print JSON.",
    )
    batch.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 local job-batch JSON manifest.",
    )
    batch.add_argument(
        "--ledger-path",
        default=None,
        help="Opt in to JSON-file-backed local contribution ledger persistence.",
    )
    batch.add_argument(
        "--message-log-path",
        default=None,
        help="Opt in to overwriting a local JSON audit log of deterministic mesh messages.",
    )

    dispatch = subcommands.add_parser(
        "dispatch-local-batch",
        help="Write assignment-only local dispatch messages for a manifest batch.",
    )
    dispatch.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 local job-batch JSON manifest.",
    )
    dispatch.add_argument(
        "--message-log-path",
        required=True,
        help="Path to write the version 1 assignment-only local message log.",
    )

    peer_dispatch = subcommands.add_parser(
        "dispatch-peer-batch",
        help="Write local dispatch messages using heartbeat-discovered peers.",
    )
    peer_dispatch.add_argument(
        "--peer-log-path",
        required=True,
        help="Path to an existing version 1 local heartbeat message log.",
    )
    peer_dispatch.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 manifest whose jobs should be dispatched.",
    )
    peer_dispatch.add_argument(
        "--message-log-path",
        required=True,
        help="Path to write the version 1 assignment-only local message log.",
    )

    local_flow = subcommands.add_parser(
        "run-local-flow",
        help="Run dispatch plus all available local worker inboxes for a manifest.",
    )
    local_flow.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 local job-batch JSON manifest.",
    )
    local_flow.add_argument(
        "--output-dir",
        required=True,
        help="Directory for deterministic local flow artifacts.",
    )
    local_flow.add_argument(
        "--transport-dir",
        default=None,
        help="Opt in to file-backed local transport inboxes for worker processing.",
    )
    local_flow.add_argument(
        "--ephemeral-identity",
        action="store_true",
        help="Replace manifest worker node IDs with fresh test-only IDs for this flow run.",
    )

    local_transport_flow = subcommands.add_parser(
        "run-local-transport-flow",
        help=(
            "Run the local flow using file-backed transport inboxes with a "
            "default transport directory."
        ),
    )
    local_transport_flow.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 local job-batch JSON manifest.",
    )
    local_transport_flow.add_argument(
        "--output-dir",
        required=True,
        help="Directory for deterministic local flow artifacts.",
    )
    local_transport_flow.add_argument(
        "--transport-dir",
        default=None,
        help="Override the default file-backed local transport directory.",
    )

    peer_transport_flow = subcommands.add_parser(
        "run-peer-transport-flow",
        help=(
            "Run local file transport using heartbeat-discovered peers as the "
            "worker roster."
        ),
    )
    peer_transport_flow.add_argument(
        "--peer-log-path",
        required=True,
        help="Path to an existing version 1 local heartbeat peer message log.",
    )
    peer_transport_flow.add_argument(
        "--manifest",
        required=True,
        help="Path to a version 1 manifest whose jobs should be run.",
    )
    peer_transport_flow.add_argument(
        "--output-dir",
        required=True,
        help="Directory for deterministic local peer transport artifacts.",
    )
    peer_transport_flow.add_argument(
        "--transport-dir",
        default=None,
        help="Override the default file-backed local transport directory.",
    )

    audit_local = subcommands.add_parser(
        "audit-local-flow",
        help="Read and verify a completed run-local-flow artifact directory.",
    )
    audit_local.add_argument(
        "--output-dir",
        required=True,
        help="Directory containing deterministic local flow artifacts to audit.",
    )

    aggregate_local = subcommands.add_parser(
        "aggregate-local-flow",
        help="Audit and aggregate a completed local flow artifact directory.",
    )
    aggregate_local.add_argument(
        "--output-dir",
        required=True,
        help="Directory containing deterministic local flow artifacts to aggregate.",
    )
    aggregate_local.add_argument(
        "--aggregate-path",
        default=None,
        help="Path to write the aggregate result JSON. Defaults to <output-dir>/aggregate-result.json.",
    )

    validate_local = subcommands.add_parser(
        "validate-local-results",
        help="Replay local assignment/result logs and write a validation report.",
    )
    validate_local.add_argument(
        "--assignment-log-path",
        required=True,
        help="Path to an existing version 1 dispatch/assignment message log.",
    )
    validate_local.add_argument(
        "--result-log-path",
        required=True,
        help="Path to an existing version 1 worker/result message log.",
    )
    validate_local.add_argument(
        "--validation-log-path",
        required=True,
        help="New path to write the deterministic local validation report.",
    )

    ledger_summary = subcommands.add_parser(
        "ledger-summary",
        help="Inspect an existing local contribution ledger and print JSON totals.",
    )
    ledger_summary.add_argument(
        "--ledger-path",
        required=True,
        help="Path to an existing version 1 local contribution ledger JSON file.",
    )

    peer_summary = subcommands.add_parser(
        "peer-summary",
        help="Inspect heartbeat-derived peers from an existing local message log.",
    )
    peer_summary.add_argument(
        "--message-log-path",
        required=True,
        help="Path to an existing version 1 local message log.",
    )

    announce = subcommands.add_parser(
        "announce-local-node",
        help="Write one local node heartbeat announcement message log.",
    )
    announce.add_argument(
        "--node-id",
        required=True,
        help="Local node id to announce.",
    )
    announce.add_argument(
        "--message-log-path",
        required=True,
        help="New path to write the version 1 local announcement message log.",
    )
    announce.add_argument(
        "--status",
        default=NodeStatus.AVAILABLE.value,
        choices=[status.value for status in NodeStatus],
        help="Local node status to announce. Defaults to available.",
    )
    announce.add_argument(
        "--capability",
        action="append",
        default=None,
        help="Capability to announce. May be supplied multiple times; defaults to local capabilities.",
    )

    materialize = subcommands.add_parser(
        "materialize-local-inboxes",
        help="Materialize addressed message-log entries into file-backed local inboxes.",
    )
    materialize.add_argument(
        "--message-log-path",
        required=True,
        help="Path to a version 1 local dispatch/message log.",
    )
    materialize.add_argument(
        "--transport-dir",
        required=True,
        help="Directory where per-node local transport inboxes should be written.",
    )

    collect_outboxes = subcommands.add_parser(
        "collect-local-outboxes",
        help="Collect per-node local transport outboxes into one message log.",
    )
    collect_outboxes.add_argument(
        "--transport-dir",
        required=True,
        help="Directory containing per-node local transport outboxes.",
    )
    collect_outboxes.add_argument(
        "--message-log-path",
        required=True,
        help="Path to write the collected version 1 local message log.",
    )

    inbox = subcommands.add_parser(
        "process-local-inbox",
        help="Replay a local message log or local transport inbox for one node's work.",
    )
    inbox.add_argument(
        "--node-id",
        required=True,
        help="Local node id whose replayed inbox should be processed.",
    )
    inbox.add_argument(
        "--message-log-path",
        default=None,
        help="Path to a version 1 local message log produced by run-local-batch.",
    )
    inbox.add_argument(
        "--transport-dir",
        default=None,
        help="Read this node's file-backed local transport inbox instead of a message log.",
    )
    inbox.add_argument(
        "--ledger-path",
        default=None,
        help="Opt in to persisting validation-gated contribution records.",
    )
    inbox.add_argument(
        "--output-message-log-path",
        default=None,
        help="Opt in to writing replayed plus emitted worker messages as a local message log.",
    )
    inbox.add_argument(
        "--node-state-path",
        default=None,
        help="Opt in to JSON-file-backed local processed-assignment state for resume/idempotency.",
    )
    inbox.add_argument(
        "--write-transport-outbox",
        action="store_true",
        help="Opt in to writing emitted worker messages to this node's local transport outbox.",
    )

    return parser


def _mark_ephemeral_artifact(payload: dict[str, object], enabled: bool) -> None:
    if enabled:
        payload["artifact_mode"] = "ephemeral_test"
        payload["ephemeral"] = True


def _mark_ephemeral_message_log(document: dict[str, object], enabled: bool) -> None:
    if not enabled:
        return
    metadata = document.get("metadata")
    if not isinstance(metadata, dict):
        raise ValueError("message log metadata must be an object")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    metadata["artifact_mode"] = "ephemeral_test"
    metadata["ephemeral"] = True


def _use_ephemeral_identity(flag_enabled: bool) -> bool:
    if flag_enabled:
        return True
    return os.environ.get("AETHERMESH_EPHEMERAL_IDENTITY", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def _log_ephemeral_identity_active(command: str) -> None:
    print(
        f"info: ephemeral test identity mode active for {command}; persistent identity files will not be used or modified",
        file=sys.stderr,
    )


def run_demo(
    node_id: str | None,
    message: str,
    include_ledger: bool = False,
    ledger_path: str | None = None,
    identity_path: str | None = None,
    ephemeral_identity: bool = False,
) -> dict[str, object]:
    if ephemeral_identity and (node_id or identity_path):
        raise IdentityPersistenceError(
            "--ephemeral-identity cannot be combined with --node-id or --identity-path"
        )
    if node_id and identity_path:
        raise IdentityPersistenceError(
            "--node-id and --identity-path are mutually exclusive"
        )
    if ephemeral_identity:
        identity = NodeIdentity.ephemeral()
    elif identity_path is not None:
        identity = load_or_create_identity(identity_path)
    else:
        resolved_node_id = node_id if node_id else deterministic_machine_node_id()
        identity = NodeIdentity(
            node_id=resolved_node_id,
            node_name=(
                None
                if node_id
                else deterministic_machine_node_name(node_id=resolved_node_id)
            ),
        )
    job = Job(job_id="demo-echo", job_type="echo", payload={"message": message})
    result = LocalRunner(identity).run(job)
    result_dict = result.to_dict()
    _mark_ephemeral_artifact(result_dict, ephemeral_identity)
    if identity.node_name is not None:
        result_dict["node_name"] = identity.node_name
    if not include_ledger and ledger_path is None:
        return result_dict

    validation = validate_job_result(job, result)
    credited_units = (
        score_validated_contribution(job, result) if validation.valid else 0
    )
    record_result = replace(result, contribution_units=credited_units)
    result_dict = record_result.to_dict()
    _mark_ephemeral_artifact(result_dict, ephemeral_identity)
    if identity.node_name is not None:
        result_dict["node_name"] = identity.node_name
    if ledger_path is None:
        ledger = ContributionLedger()
        ledger.record(
            record_result,
            validation_valid=validation.valid,
            validation_reason=validation.reason,
            job_type=job.job_type,
        )
        payload: dict[str, object] = {
            "result": result_dict,
            "validation": validation.to_dict(),
            "ledger_summary": ledger.summary_for_node(identity.node_id).to_dict(),
        }
        _mark_ephemeral_artifact(payload, ephemeral_identity)
        return payload

    ledger, extra_fields = load_ledger_document(ledger_path)
    ledger.record(
        record_result,
        validation_valid=validation.valid,
        validation_reason=validation.reason,
        job_type=job.job_type,
    )
    if ephemeral_identity:
        extra_fields["artifact_mode"] = "ephemeral_test"
        extra_fields["ephemeral"] = True
    save_ledger_document(ledger_path, ledger, extra_fields)
    payload = {
        "result": result_dict,
        "validation": validation.to_dict(),
        "ledger_path": ledger_path,
        "persisted_ledger_summary": ledger.summary_for_node(identity.node_id).to_dict(),
    }
    _mark_ephemeral_artifact(payload, ephemeral_identity)
    return payload


def run_default_local_simulation() -> dict[str, object]:
    """Run the fixed local simulation demo used by the CLI command."""

    jobs = [
        Job(job_id="echo-1", job_type="echo", payload={"message": "hello mesh one"}),
        Job(
            job_id="text-stats-1",
            job_type="text_stats",
            payload={"text": "hello mesh\nhello node"},
        ),
        Job(
            job_id="keyword-extract-1",
            job_type="keyword_extract",
            payload={
                "text": "AetherMesh nodes process useful local work for the mesh.",
                "limit": 5,
            },
        ),
        Job(job_id="echo-2", job_type="echo", payload={"message": "hello mesh two"}),
        Job(job_id="echo-3", job_type="echo", payload={"message": "hello mesh three"}),
    ]
    return run_local_simulation(
        node_ids=["local-node-a", "local-node-b"], jobs=jobs
    ).to_dict()


def run_local_batch(
    manifest_path: str,
    ledger_path: str | None = None,
    message_log_path: str | None = None,
) -> dict[str, object]:
    """Run a local simulation from a validated JSON manifest."""

    batch = load_job_manifest(manifest_path)
    simulation = run_local_simulation(node_ids=batch.nodes, jobs=batch.jobs)
    result = simulation.to_dict()
    if ledger_path is not None:
        ledger, extra_fields = load_ledger_document(ledger_path)
        for job, accounted_result, validation in zip(
            batch.jobs, simulation.accounted_results, simulation.validations
        ):
            ledger.record(
                accounted_result,
                validation_valid=validation.valid,
                validation_reason=validation.reason,
                job_type=job.job_type,
                manifest_ref=manifest_path,
            )
        save_ledger_document(ledger_path, ledger, extra_fields)
        result["ledger_path"] = ledger_path
        result["persisted_ledger_summaries"] = [
            ledger.summary_for_node(node_id).to_dict() for node_id in batch.node_ids
        ]

    if message_log_path is not None:
        message_log_document = build_message_log_document(
            simulation=simulation,
            jobs=batch.jobs,
            manifest_path=manifest_path,
        )
        write_message_log(message_log_path, message_log_document)
        result["message_log_path"] = message_log_path

    return result


def dispatch_local_batch_command(
    manifest_path: str,
    message_log_path: str,
) -> dict[str, object]:
    """Dispatch a manifest batch to a local message log without execution."""

    batch = load_job_manifest(manifest_path)
    dispatch = dispatch_local_batch(
        manifest_path=manifest_path,
        message_log_path=message_log_path,
        nodes=batch.nodes,
        jobs=batch.jobs,
    )
    message_log_document = build_dispatch_message_log_document(
        messages=dispatch.messages,
        jobs=dispatch.jobs,
        nodes=dispatch.nodes,
        assignments=dispatch.assignments,
        manifest_path=manifest_path,
    )
    write_message_log(message_log_path, message_log_document)
    return dispatch.to_dict()


def dispatch_peer_batch_command(
    peer_log_path: str,
    manifest_path: str,
    message_log_path: str,
) -> dict[str, object]:
    """Dispatch manifest jobs to heartbeat-derived peers without execution."""

    nodes = scheduled_nodes_from_peer_log(peer_log_path)
    jobs = load_manifest_jobs(manifest_path)
    dispatch = dispatch_local_batch(
        manifest_path=manifest_path,
        message_log_path=message_log_path,
        nodes=nodes,
        jobs=jobs,
        command="dispatch-peer-batch",
        peer_log_path=peer_log_path,
    )
    message_log_document = build_dispatch_message_log_document(
        messages=dispatch.messages,
        jobs=dispatch.jobs,
        nodes=dispatch.nodes,
        assignments=dispatch.assignments,
        manifest_path=manifest_path,
    )
    write_message_log(message_log_path, message_log_document)
    return dispatch.to_dict()


def summarize_ledger(ledger_path: str) -> dict[str, object]:
    """Load an existing ledger and return read-only aggregate totals."""

    ledger, _extra_fields = load_existing_ledger_document(ledger_path)
    return ledger.summary_document(ledger_path)


def summarize_peers(message_log_path: str) -> dict[str, object]:
    """Load an existing message log and return a read-only peer roster."""

    return peer_summary_document(message_log_path)


def _ephemeral_roster(nodes: list[ScheduledNode]) -> list[ScheduledNode]:
    return [
        ScheduledNode(
            node_id=NodeIdentity.ephemeral().node_id,
            status=node.status,
            capabilities=node.capabilities,
        )
        for node in nodes
    ]


def run_local_flow(
    manifest_path: str,
    output_dir: str,
    transport_dir: str | None = None,
    ephemeral_identity: bool = False,
) -> dict[str, object]:
    """Run dispatch and all available local worker inboxes as one local flow."""

    batch = load_job_manifest(manifest_path)
    nodes = _ephemeral_roster(batch.nodes) if ephemeral_identity else batch.nodes
    return _run_local_flow_with_roster(
        manifest_path=manifest_path,
        jobs=batch.jobs,
        nodes=nodes,
        output_dir=output_dir,
        transport_dir=transport_dir,
        command="run-local-flow",
        ephemeral_identity=ephemeral_identity,
    )


def run_local_transport_flow(
    manifest_path: str, output_dir: str, transport_dir: str | None = None
) -> dict[str, object]:
    """Run the existing local flow with file-backed transport enabled."""

    effective_transport_dir = transport_dir or str(Path(output_dir) / "transport")
    return run_local_flow(
        manifest_path=manifest_path,
        output_dir=output_dir,
        transport_dir=effective_transport_dir,
    )


def run_peer_transport_flow(
    peer_log_path: str,
    manifest_path: str,
    output_dir: str,
    transport_dir: str | None = None,
) -> dict[str, object]:
    """Run local file transport with workers discovered from heartbeat peers."""

    nodes = scheduled_nodes_from_peer_log(peer_log_path)
    jobs = load_manifest_jobs(manifest_path)
    # Validate the peer roster can actually receive the manifest jobs before any
    # output directory or artifact path is created or overwritten.
    LocalScheduler(nodes).assign_jobs(jobs)
    effective_transport_dir = transport_dir or str(Path(output_dir) / "transport")
    return _run_local_flow_with_roster(
        manifest_path=manifest_path,
        jobs=jobs,
        nodes=nodes,
        output_dir=output_dir,
        transport_dir=effective_transport_dir,
        command="run-peer-transport-flow",
        peer_log_path=peer_log_path,
    )


def _run_local_flow_with_roster(
    *,
    manifest_path: str,
    jobs: list[Job],
    nodes: list[ScheduledNode],
    output_dir: str,
    transport_dir: str | None,
    command: str,
    peer_log_path: str | None = None,
    ephemeral_identity: bool = False,
) -> dict[str, object]:
    output_path = Path(output_dir)
    try:
        output_path.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        raise ValueError(f"could not create output directory: {exc}") from exc

    dispatch_message_log_path = output_path / "dispatch-message-log.json"
    flow_message_log_path = output_path / "flow-message-log.json"
    ledger_path = output_path / "ledger.json"
    receipts_path = output_path / "receipts.json"
    node_state_dir = output_path / "node-state"
    worker_log_dir = output_path / "worker-message-logs"

    available_node_ids = [
        node.node_id for node in nodes if node.status.value == "available"
    ]
    run_version_metadata = capture_version_metadata()
    offline_node_ids = [
        node.node_id for node in nodes if node.status.value == "offline"
    ]

    # Validate existing resumable inputs before overwriting any flow artifacts.
    load_ledger_document(ledger_path)
    existing_receipt_document = load_receipt_document_if_exists(receipts_path)
    for node_id in available_node_ids:
        load_node_processing_state(
            _node_artifact_path(node_state_dir, node_id), expected_node_id=node_id
        )

    dispatch = dispatch_local_batch(
        manifest_path=manifest_path,
        message_log_path=str(dispatch_message_log_path),
        nodes=nodes,
        jobs=jobs,
        command=(
            "dispatch-peer-batch"
            if command == "run-peer-transport-flow"
            else "dispatch-local-batch"
        ),
        peer_log_path=peer_log_path,
    )
    message_log_document = build_dispatch_message_log_document(
        messages=dispatch.messages,
        jobs=dispatch.jobs,
        nodes=dispatch.nodes,
        assignments=dispatch.assignments,
        manifest_path=manifest_path,
    )
    if peer_log_path is not None:
        message_log_document["metadata"]["source"] = "dispatch-peer-batch"
        message_log_document["metadata"]["peer_log_path"] = peer_log_path
        message_log_document["metadata"]["roster_source"] = "heartbeat_peer_log"
    _mark_ephemeral_message_log(message_log_document, ephemeral_identity)
    write_message_log(dispatch_message_log_path, message_log_document)
    dispatch_payload = dispatch.to_dict()

    transport_payload: dict[str, object] | None = None
    if transport_dir is not None:
        transport_payload = materialize_local_inboxes(
            message_log_path=dispatch_message_log_path,
            transport_dir=transport_dir,
        )
        materialized_inbox_paths = transport_payload.get("inbox_paths", {})
        if not isinstance(materialized_inbox_paths, dict):
            raise ValueError("materialize-local-inboxes returned invalid inbox paths")
        for node_id in available_node_ids:
            if node_id not in materialized_inbox_paths:
                write_local_inbox(
                    transport_dir=transport_dir,
                    node_id=node_id,
                    messages=[],
                    source_message_log_path=dispatch_message_log_path,
                )

    per_node_results: list[dict[str, object]] = []
    processed_assignments = []
    emitted_messages_by_node: dict[str, list[MeshMessage]] = {}
    worker_message_log_paths: dict[str, str | Path] = {}
    for node_id in available_node_ids:
        node_state_path = _node_artifact_path(node_state_dir, node_id)
        worker_message_log_path = _node_artifact_path(worker_log_dir, node_id)
        worker_message_log_paths[node_id] = worker_message_log_path
        node_payload, inbox_result = _process_local_inbox(
            InboxReplayRequest(
                node_id=node_id,
                message_log_path=(
                    str(dispatch_message_log_path) if transport_dir is None else None
                ),
                transport_dir=transport_dir,
                ledger_path=str(ledger_path),
                output_message_log_path=str(worker_message_log_path),
                node_state_path=str(node_state_path),
                ephemeral_identity=ephemeral_identity,
                version_metadata=run_version_metadata,
            )
        )
        processed_assignments.extend(inbox_result.processed)
        raw_processed_count = node_payload["processed_assignment_count"]
        if not isinstance(raw_processed_count, int):
            raise ValueError("process-local-inbox returned invalid processed count")  # noqa: TRY004 - justification: public validation callers catch ValueError.
        processed_count = raw_processed_count
        skipped_ids = node_payload.get("skipped_processed_message_ids", [])
        if not isinstance(skipped_ids, list):
            raise ValueError("process-local-inbox returned invalid skipped id list")  # noqa: TRY004 - justification: public validation callers catch ValueError.
        ignored_ids = node_payload["ignored_message_ids"]
        if not isinstance(ignored_ids, list):
            raise ValueError("process-local-inbox returned invalid ignored id list")  # noqa: TRY004 - justification: public validation callers catch ValueError.
        per_node_results.append(
            {
                "node_id": node_id,
                "node_state_path": str(node_state_path),
                "worker_message_log_path": str(worker_message_log_path),
                "processed_assignment_count": processed_count,
                "skipped_processed_assignment_count": len(skipped_ids),
                "ignored_message_count": len(ignored_ids),
                "ledger_summary": node_payload.get("ledger_summary"),
            }
        )
        emitted_messages_by_node[node_id] = load_worker_emitted_messages(
            worker_message_log_path
        )

    if ephemeral_identity:
        ledger, extra_fields = load_ledger_document(ledger_path)
        extra_fields["artifact_mode"] = "ephemeral_test"
        extra_fields["ephemeral"] = True
        save_ledger_document(ledger_path, ledger, extra_fields)
    ledger_summary = summarize_ledger(str(ledger_path))
    processed_node_ids = [str(result["node_id"]) for result in per_node_results]
    processed_assignment_count = sum(
        _require_int_result_field(result, "processed_assignment_count")
        for result in per_node_results
    )
    skipped_processed_assignment_count = sum(
        _require_int_result_field(result, "skipped_processed_assignment_count")
        for result in per_node_results
    )
    flow_message_log_document = build_flow_message_log_document(
        dispatch_messages=load_message_log_messages(dispatch_message_log_path),
        emitted_messages_by_node=emitted_messages_by_node,
        manifest_path=manifest_path,
        dispatch_message_log_path=dispatch_message_log_path,
        ledger_path=ledger_path,
        worker_message_log_paths=worker_message_log_paths,
        available_node_ids=available_node_ids,
        offline_node_ids=offline_node_ids,
        processed_node_ids=processed_node_ids,
        processed_assignment_count=processed_assignment_count,
        skipped_processed_assignment_count=skipped_processed_assignment_count,
        total_contribution_units=int(str(ledger_summary["total_contribution_units"])),
        source=command,
        peer_log_path=peer_log_path,
    )
    _mark_ephemeral_message_log(flow_message_log_document, ephemeral_identity)
    write_message_log(flow_message_log_path, flow_message_log_document)
    receipt_document = build_receipt_document(
        processed_assignments,
        existing_document=existing_receipt_document,
        artifact_mode="ephemeral_test" if ephemeral_identity else None,
        version_metadata=run_version_metadata,
    )
    write_receipt_document(receipts_path, receipt_document)
    result: dict[str, object] = {
        "command": command,
        "manifest_path": manifest_path,
        "output_dir": output_dir,
        "dispatch_message_log_path": str(dispatch_message_log_path),
        "flow_message_log_path": str(flow_message_log_path),
        "receipts_path": str(receipts_path),
        "receipt_count": len(receipt_document["receipts"]),
        "flow_message_count": flow_message_log_document["metadata"]["message_count"],
        "flow_emitted_message_count": flow_message_log_document["metadata"][
            "emitted_message_count"
        ],
        "ledger_path": str(ledger_path),
        "available_node_ids": available_node_ids,
        "offline_node_ids": offline_node_ids,
        "processed_node_ids": processed_node_ids,
        "processed_assignment_count": processed_assignment_count,
        "skipped_processed_assignment_count": skipped_processed_assignment_count,
        "ignored_message_count": sum(
            _require_int_result_field(result, "ignored_message_count")
            for result in per_node_results
        ),
        "total_contribution_units": ledger_summary["total_contribution_units"],
        "ledger_summary": ledger_summary,
        "dispatch_summary": dispatch_payload,
        "node_results": per_node_results,
    }
    if peer_log_path is not None:
        result["peer_log_path"] = peer_log_path
        result["roster_source"] = "heartbeat_peer_log"
    if ephemeral_identity:
        result["artifact_mode"] = "ephemeral_test"
        result["ephemeral"] = True
        result["roster_source"] = "ephemeral_test_identity"
    if transport_payload is not None:
        transport_inbox_paths = transport_payload.get("inbox_paths", {})
        if not isinstance(transport_inbox_paths, dict):
            raise ValueError("materialize-local-inboxes returned invalid inbox paths")
        transport_dir_value = str(transport_dir)
        result["transport_dir"] = str(transport_dir)
        result["transport_inbox_count"] = transport_payload["inbox_count"]
        result["transport_inbox_paths"] = {
            node_id: str(local_inbox_path(transport_dir_value, node_id))
            for node_id in sorted(available_node_ids)
            if node_id in transport_inbox_paths
        }
    return result


def _require_int_result_field(result: dict[str, object], field_name: str) -> int:
    value = result.get(field_name)
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"node result field must be an integer: {field_name}")  # noqa: TRY004 - justification: public validation callers catch ValueError.
    return value


def _node_artifact_path(directory: Path, node_id: str) -> Path:
    return directory / f"{_node_artifact_filename(node_id)}.json"


def _node_artifact_filename(node_id: str) -> str:
    """Return a deterministic, non-merging filename for one manifest node id."""

    if not isinstance(node_id, str) or node_id == "":
        raise ValueError("node id must be a non-empty string")
    return quote(node_id, safe="-._~")


def process_local_inbox(
    *,
    node_id: str,
    message_log_path: str | None = None,
    transport_dir: str | None = None,
    ledger_path: str | None = None,
    output_message_log_path: str | None = None,
    node_state_path: str | None = None,
    write_transport_outbox: bool = False,
) -> dict[str, object]:
    """Replay a saved local message log or local transport inbox for one node."""

    if (message_log_path is None) == (transport_dir is None):
        raise ValueError("provide exactly one of --message-log-path or --transport-dir")
    payload, _inbox_result = _process_local_inbox(
        InboxReplayRequest(
            node_id=node_id,
            message_log_path=message_log_path,
            transport_dir=transport_dir,
            ledger_path=ledger_path,
            output_message_log_path=output_message_log_path,
            node_state_path=node_state_path,
            write_transport_outbox=write_transport_outbox,
        )
    )
    return payload


def _process_local_inbox(
    request: InboxReplayRequest,
) -> tuple[dict[str, object], InboxProcessResult]:
    """Replay saved local messages and return payload plus structured result."""

    if (request.message_log_path is None) == (request.transport_dir is None):
        raise ValueError("provide exactly one of --message-log-path or --transport-dir")
    if request.write_transport_outbox and request.transport_dir is None:
        raise ValueError("--write-transport-outbox requires --transport-dir")
    node_state = (
        load_node_processing_state(
            request.node_state_path, expected_node_id=request.node_id
        )
        if request.node_state_path is not None
        else None
    )
    if request.transport_dir is not None:
        messages = load_local_inbox(
            transport_dir=request.transport_dir, node_id=request.node_id
        )
        source_message_path = str(
            local_inbox_path(request.transport_dir, request.node_id)
        )
    else:
        if request.message_log_path is None:
            raise ValueError("message log path is required")
        messages = load_message_log_messages(request.message_log_path)
        source_message_path = request.message_log_path
    ledger, extra_fields = (
        load_ledger_document(request.ledger_path)
        if request.ledger_path is not None
        else (ContributionLedger(), {})
    )
    message_bus = LocalMessageBus()
    for registered_node_id in _node_ids_from_replayed_messages(
        messages, request.node_id
    ):
        message_bus.register_node(registered_node_id)
    for message in messages:
        message_bus.send(message)

    service = LocalNodeService(
        identity=NodeIdentity(node_id=request.node_id),
        message_bus=message_bus,
        runner=LocalRunner(NodeIdentity(node_id=request.node_id)),
        ledger=ledger,
        processed_message_ids=(
            list(node_state.processed_message_ids) if node_state is not None else None
        ),
        version_metadata=request.version_metadata,
    )
    inbox_result = service.process_inbox()
    if request.ledger_path is not None:
        save_ledger_document(request.ledger_path, ledger, extra_fields)
    payload = _inbox_process_result_to_dict(inbox_result, ledger, request.ledger_path)
    emitted_messages = _emitted_messages_from_inbox_result(inbox_result)
    if request.output_message_log_path is not None:
        output_document = build_replayed_message_log_document(
            replayed_messages=messages,
            emitted_messages=emitted_messages,
            node_id=request.node_id,
            source_message_log_path=source_message_path,
            ledger_path=request.ledger_path,
            processed_assignment_count=len(inbox_result.processed),
            ignored_message_ids=list(inbox_result.ignored_message_ids),
        )
        _mark_ephemeral_message_log(output_document, request.ephemeral_identity)
        write_message_log(request.output_message_log_path, output_document)
        payload["output_message_log_path"] = request.output_message_log_path
        payload["final_message_count"] = len(messages) + len(emitted_messages)
    if request.write_transport_outbox:
        if request.transport_dir is None:
            raise ValueError("--write-transport-outbox requires --transport-dir")
        outbox_messages = [
            message
            for message in emitted_messages
            if message.sender_node_id == request.node_id
        ]
        outbox_path = write_local_outbox(
            transport_dir=request.transport_dir,
            node_id=request.node_id,
            source_inbox_path=source_message_path,
            processed_assignment_count=len(inbox_result.processed),
            messages=outbox_messages,
        )
        payload["transport_outbox_path"] = str(outbox_path)
        payload["transport_outbox_message_count"] = len(outbox_messages)
    if request.node_state_path is not None and node_state is not None:
        updated_state = LocalNodeProcessingState(
            node_id=request.node_id,
            processed_message_ids=list(inbox_result.processed_message_ids),
            extra_fields=node_state.extra_fields,
        )
        save_node_processing_state(request.node_state_path, updated_state)
        payload["node_state_path"] = request.node_state_path
        payload["processed_message_ids"] = list(updated_state.processed_message_ids)
        payload["skipped_processed_message_ids"] = list(
            inbox_result.skipped_processed_message_ids
        )
    return payload, inbox_result


def _emitted_messages_from_inbox_result(
    inbox_result: InboxProcessResult,
) -> list[MeshMessage]:
    return [
        message
        for assignment in inbox_result.processed
        for message in assignment.emitted_messages
    ]


def _node_ids_from_replayed_messages(
    messages: Sequence[MeshMessage], node_id: str
) -> list[str]:
    node_ids = {node_id, "local-ledger"}
    for message in messages:
        sender = message.sender_node_id
        recipient = message.recipient_node_id
        node_ids.add(sender)
        if recipient is not None:
            node_ids.add(recipient)
    return sorted(node_ids)


def _inbox_process_result_to_dict(
    inbox_result: InboxProcessResult,
    ledger: ContributionLedger,
    ledger_path: str | None,
) -> dict[str, object]:
    emitted_messages = [
        {
            "id": message.message_id,
            "type": message.message_type,
            "sender": message.sender_node_id,
            "recipient": message.recipient_node_id,
        }
        for assignment in inbox_result.processed
        for message in assignment.emitted_messages
    ]
    validation_outcomes = [
        {
            "job_id": assignment.job.job_id,
            "valid": assignment.validation.valid,
            "credited_units": assignment.contribution_record.contribution_units,
            "reason": assignment.validation.reason,
        }
        for assignment in inbox_result.processed
    ]
    payload: dict[str, object] = {
        "command": "process-local-inbox",
        "node_id": inbox_result.node_id,
        "processed_assignment_count": len(inbox_result.processed),
        "ignored_message_ids": list(inbox_result.ignored_message_ids),
        "emitted_messages": emitted_messages,
        "validation_outcomes": validation_outcomes,
    }
    if ledger_path is not None:
        node_summary = ledger.summary_for_node(inbox_result.node_id)
        node_ids = ledger.node_ids()
        payload["ledger_summary"] = {
            "path": ledger_path,
            "total_units": sum(
                ledger.summary_for_node(summary_node_id).total_contribution_units
                for summary_node_id in node_ids
            ),
            "node_units": node_summary.total_contribution_units,
            "record_count": sum(
                ledger.summary_for_node(summary_node_id).total_result_count
                for summary_node_id in node_ids
            ),
        }
    return payload


FlowCommandError = (
    ManifestError,
    PeerRegistryError,
    MessageLogPersistenceError,
    LocalTransportError,
    LedgerPersistenceError,
    NodeStatePersistenceError,
    ValueError,
)


def _run_json_command(
    operation: Callable[[], dict[str, object]],
    handled_errors: tuple[type[Exception], ...],
) -> int:
    try:
        payload = operation()
    except handled_errors as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(payload, sort_keys=True))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "run-demo":
        ephemeral_identity = _use_ephemeral_identity(args.ephemeral_identity)
        if ephemeral_identity:
            _log_ephemeral_identity_active(args.command)
        try:
            payload = run_demo(
                args.node_id,
                args.message,
                args.include_ledger,
                args.ledger_path,
                args.identity_path,
                ephemeral_identity,
            )
        except (IdentityPersistenceError, LedgerPersistenceError) as exc:
            parser.error(str(exc))
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "reset-identity":
        if not args.confirm_reset:
            parser.error(
                "reset-identity requires --confirm-reset because lineage and attribution continuity may be affected"
            )
        try:
            result = reset_identity(
                args.identity_path,
                reason=args.reason,
                quarantine_dir=args.quarantine_dir,
                audit_receipt_path=args.audit_receipt_path,
                rotate_creator_identity=args.rotate_creator_identity,
            )
        except IdentityPersistenceError as exc:
            parser.error(str(exc))
        print(result.warning, file=sys.stderr)
        print(json.dumps(result.to_dict(), sort_keys=True))
        return 0

    if args.command == "simulate-local":
        print(json.dumps(run_default_local_simulation(), sort_keys=True))
        return 0

    if args.command == "update":
        try:
            payload = update_from_latest_release(
                dry_run=args.dry_run, release_url=args.release_url
            ).to_dict()
        except ReleaseUpdateError as exc:
            print(f"error: update failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "start-local-node":
        try:
            payload = start_local_node(
                args.runtime_dir,
                reset_creator_identity=args.reset_creator_identity,
            ).to_dict()
        except LocalStartupError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "shutdown-local-node":
        try:
            payload = stop_local_node_runtime(
                args.runtime_dir, timeout_seconds=args.timeout_seconds
            ).to_dict()
        except LocalShutdownError as exc:
            print(json.dumps({"error": exc.to_dict()}, sort_keys=True), file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "restart-local-node":
        try:
            payload = restart_local_node_runtime(
                args.runtime_dir, timeout_seconds=args.timeout_seconds
            ).to_dict()
        except LocalRestartError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "run-local-batch":
        try:
            payload = run_local_batch(
                args.manifest, args.ledger_path, args.message_log_path
            )
        except ManifestError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except (LedgerPersistenceError, MessageLogPersistenceError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except ValueError as exc:
            print(f"error: local batch execution failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "dispatch-local-batch":
        try:
            payload = dispatch_local_batch_command(args.manifest, args.message_log_path)
        except ManifestError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except MessageLogPersistenceError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except ValueError as exc:
            print(f"error: local batch dispatch failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "dispatch-peer-batch":
        try:
            payload = dispatch_peer_batch_command(
                args.peer_log_path, args.manifest, args.message_log_path
            )
        except (ManifestError, PeerRegistryError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except MessageLogPersistenceError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        except ValueError as exc:
            print(f"error: peer batch dispatch failed: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "run-local-flow":
        ephemeral_identity = _use_ephemeral_identity(args.ephemeral_identity)
        if ephemeral_identity:
            _log_ephemeral_identity_active(args.command)
        return _run_json_command(
            lambda: run_local_flow(
                args.manifest,
                args.output_dir,
                args.transport_dir,
                ephemeral_identity,
            ),
            FlowCommandError,
        )

    if args.command == "run-local-transport-flow":
        return _run_json_command(
            lambda: run_local_transport_flow(
                args.manifest, args.output_dir, args.transport_dir
            ),
            FlowCommandError,
        )

    if args.command == "run-peer-transport-flow":
        return _run_json_command(
            lambda: run_peer_transport_flow(
                args.peer_log_path, args.manifest, args.output_dir, args.transport_dir
            ),
            FlowCommandError,
        )

    if args.command == "audit-local-flow":
        try:
            payload = audit_local_flow(args.output_dir)
        except (
            FlowAuditError,
            MessageLogPersistenceError,
            LedgerPersistenceError,
            ReceiptPersistenceError,
            ValueError,
        ) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "aggregate-local-flow":
        try:
            payload = aggregate_local_flow(args.output_dir, args.aggregate_path)
        except (
            AggregationError,
            FlowAuditError,
            MessageLogPersistenceError,
            LedgerPersistenceError,
            ReceiptPersistenceError,
            ValueError,
        ) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "validate-local-results":
        try:
            payload = validate_local_node_results(
                assignment_log_path=args.assignment_log_path,
                result_log_path=args.result_log_path,
                validation_log_path=args.validation_log_path,
            )
        except LocalValidationError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "ledger-summary":
        try:
            payload = summarize_ledger(args.ledger_path)
        except LedgerPersistenceError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "peer-summary":
        try:
            payload = summarize_peers(args.message_log_path)
        except PeerRegistryError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "announce-local-node":
        try:
            payload = announce_local_node(
                node_id=args.node_id,
                message_log_path=args.message_log_path,
                status=args.status,
                capabilities=args.capability,
            )
        except NodeAnnouncementError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "materialize-local-inboxes":
        try:
            payload = materialize_local_inboxes(
                message_log_path=args.message_log_path,
                transport_dir=args.transport_dir,
            )
        except (MessageLogPersistenceError, LocalTransportError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "collect-local-outboxes":
        try:
            payload = collect_local_outboxes(
                transport_dir=args.transport_dir,
                message_log_path=args.message_log_path,
            )
        except (LocalTransportError, MessageLogPersistenceError) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    if args.command == "process-local-inbox":
        try:
            payload = process_local_inbox(
                node_id=args.node_id,
                message_log_path=args.message_log_path,
                transport_dir=args.transport_dir,
                ledger_path=args.ledger_path,
                output_message_log_path=args.output_message_log_path,
                node_state_path=args.node_state_path,
                write_transport_outbox=args.write_transport_outbox,
            )
        except (
            MessageLogPersistenceError,
            LocalTransportError,
            LedgerPersistenceError,
            NodeStatePersistenceError,
            ValueError,
        ) as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 1
        print(json.dumps(payload, sort_keys=True))
        return 0

    parser.error(f"Unsupported command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
