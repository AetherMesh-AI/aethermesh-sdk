# Phase 1 Local API Boundary

Phase 1 exposes a deliberately small, local-only boundary for a runnable prototype. Here, “API” means the supported Python function, localhost route, and CLI contracts plus their local JSON artifacts. It does not promise remote peers or a platform control plane.

The headless HTTP app is a separate localhost development surface. Its status routes are read-only (except local process shutdown and restart signals). `POST /api/jobs` is the single local work-submission exception; it records a request but does not execute it, issue receipts, or award contribution credit.

## Scope and locality

Every path in this document is an operator-selected local filesystem path. Artifact references written by the runtime are relative to the selected runtime directory. The supported runtime mode is `local-only-no-p2p`; no operation contacts an external service.

Phase 1 supports these operations only:

| Boundary | Python function | CLI command | Purpose |
| --- | --- | --- | --- |
| Create or load node | `start_local_node_runtime(runtime_dir, reset_creator_identity=False)` | `start-local-node --runtime-dir <dir>` | Create a new local runtime or load its preserved identity and startup artifacts. |
| Inspect node provenance | `inspect_local_node_runtime(runtime_dir)` and `local_node_status(runtime_dir)` | none | Read identity, manifest, receipt, lineage, and attribution references without mutation. |
| Stop or restart node | `stop_local_node_runtime(...)`, `restart_local_node_runtime(...)` | `shutdown-local-node`, `restart-local-node` | Persist local lifecycle evidence while retaining identity and audit links. |
| Submit or inspect one local job | `NodeRuntimeService.submit_local_job(request)`, `get_local_job_status(job_id)`, `trace_local_job_attribution(job_id)`, and `execute_submitted_local_job(job_id, worker_node_id)` | none | Record a local submission, inspect its lifecycle or validation-gated attribution chain, or run it through the deterministic local helper. HTTP exposes submission at `POST /api/jobs`, status inspection at `GET /api/jobs/{job_id}`, and attribution tracing at `GET /api/jobs/{job_id}/trace`. |
| Submit deterministic local batch work | `dispatch_local_batch_command(manifest_path, message_log_path)` or `run_local_flow(manifest_path, output_dir, ...)` | `dispatch-local-batch` or `run-local-flow` | Validate a version 1 batch manifest, then dispatch or execute it locally. |
| Validate completed local work | `validate_local_node_results(assignment_log_path, result_log_path, validation_log_path)` | `validate-local-results` | Replay local assignment/result logs and write a deterministic validation report. |
| Read validated result history | `audit_local_flow(output_dir)` | `audit-local-flow` | Verify a completed local-flow artifact set. |
| Inspect local job audit evidence | `NodeRuntimeService.inspect_local_audit_events(...)` | none | Read paginated submission/execution evidence without writing artifacts. |

The `run-local-batch` simulation command is also supported as a local deterministic execution helper, but it is not a daemon queue or network submission endpoint.

## Capability listing response contract

`GET /capabilities` (also `/api/capabilities`) and `NodeRuntimeService.list_capabilities()` return a local, versioned capability response. Its `capabilities` entries each contain `identifier`, `description`, `status` (`enabled` or `disabled`), and `schema_version`. Work entries are enabled only when their work type is configured in the local runtime's `capabilities.enabled_work_types`; provenance entries are registered local artifact contracts.

The list explicitly reports local creator-node-ID handling, manifests, validation receipts, lineage references, and contribution-attribution metadata. `provenance.creator_node_id` is enabled only when identity persistence is enabled in the current local runtime configuration. `provenance.end_to_end_runtime_lineage` is deliberately `disabled`: the planned runtime-flow binding described below has not been implemented. The response has `network_mode: local-only-no-p2p` and `advertised: false`; it neither contacts external services nor claims peer discovery, consensus, or decentralization.

## Node create/load contract

Request:

```text
runtime_dir: local directory
reset_creator_identity: false (default)
```

A successful create/load returns a `LocalStartupResult` JSON object with at least:

```json
{
  "node_id": "local node ID",
  "creator_node_id": "preserved creator node ID",
  "identity_path": "identity/creator-node.json",
  "manifest_path": "manifests/local-node-manifest.json",
  "manifest_hash": "sha256:<hash>",
  "validation_receipt_path": "receipts/startup-validation-<timestamp>-<index>.json",
  "lineage_path": "lineage/startup-lineage-<timestamp>-<index>.json",
  "runtime_directories": {},
  "validation_result": "passed",
  "network_mode": "local-only-no-p2p"
}
```

For an existing runtime, normal startup loads the persisted identity and preserves both `node_id` and `creator_node_id`. It fails closed rather than regenerating a missing preserved creator identity or startup manifest. `reset_creator_identity=True` (or `--reset-creator-identity`) is the explicit exception and records reset evidence; it is not part of ordinary loading.

## Manifest and artifact contract

There are two scoped manifest types; callers must not treat either as a generic remote job API.

1. The startup manifest is runtime-owned and created/read by the local startup boundary. It requires `version`, `manifest_type`, `node.node_id`, `node.creator_node_id`, `runtime_version`, `capabilities`, `work_directories`, and fail-closed local validation flags. It binds the runtime to its preserved creator identity.
2. A version 1 local job-batch manifest is caller-provided and read by `dispatch-local-batch`, `run-local-batch`, or `run-local-flow`. It requires a non-empty `nodes` roster and non-empty `jobs` list. Every job requires `job_id`, `job_type`, and an object `payload`; `payload` is the work input.

Work output is not written back into the input manifest. Local execution writes result messages and deterministic flow artifacts. A work record is complete only when the local artifacts together contain:

| Required information | Source of record |
| --- | --- |
| Work type and input | Job-batch `jobs[].job_type` and `jobs[].payload` |
| Output and output hash | Result record and receipt `output_summary` / `result_hash` |
| Timestamps | Local mesh messages and startup/lineage/receipt records |
| Worker identity | Job-batch node roster and result/receipt `node_id` |
| Manifest link | Startup receipt `manifest_ref` and `manifest_hash`; flow message-log manifest reference |
| Startup lineage link | Startup lineage record plus its local `receipt_ref` and startup-manifest link; this is not currently linked to batch work |
| Validation link | Startup receipt or flow receipt `validation_message_id` and `validation` result |
| Contribution attribution | Validated flow receipt `contribution_message_id` / `credited_units`; startup attribution is a separate lifecycle record |

The runtime owns startup manifests, startup receipts, and startup lineage records. Batch manifests are input-only. Receipt and lineage writers validate before atomic creation/replacement; callers must not edit an accepted artifact in place to alter provenance.

## Submission, acceptance, and rejection

`POST /api/jobs` is a localhost-only submission route backed by `NodeRuntimeService.submit_local_job(request)`. Its version-1 request is a JSON object with required integer `schema_version: 1`, non-empty `job_type`, object `payload`, non-empty `creator_node_id`, non-empty `requested_validation_mode`, required `lineage_parent_refs` (a list of non-empty strings; empty for root lineage), and required object `attribution_metadata` (empty allowed). It writes one immutable `data/job-submissions/<job-id>.json` manifest only after validation and returns `schema_version: 1`, `job_id`, `status: accepted_pending_execution`, `manifest_ref`, `next_validation_expectation: pending_requested_local_validation`, and `network_mode: local-only-no-p2p`. Submission does not dispatch work, create a receipt, complete validation, contact peers, or award credit.

`GET /api/jobs/{job_id}` and `get_local_job_status(job_id)` return the immutable submission provenance plus the current local lifecycle projection. Queued jobs have no invented worker, result, or receipt. `GET /api/jobs/{job_id}/trace` and `trace_local_job_attribution(job_id)` read the completed local job, result, accepted validation receipt, contribution attribution, manifest, and preserved creator identity as one ordered, validation-gated chain; missing or mismatched evidence is rejected rather than inferred. The local-only `execute_submitted_local_job(job_id, worker_node_id)` helper persists separate result, validation-receipt, and status records through the existing deterministic runner and validator; completed responses then include the worker, relative result reference and summary, validation receipt/pass-fail metadata, lineage, and validation-gated attribution. Unknown IDs return the stable `{job_id, status: "not_found", error: "local job not found"}` response. This is not a daemon queue or remote dispatch API.

A local batch submission is an invocation of `dispatch-local-batch` or `run-local-flow` with a version 1 job-batch manifest. It is accepted only after manifest parsing confirms the version, non-empty unique node roster, non-empty jobs, and each job’s non-empty `job_id`/`job_type` plus object payload. Execution additionally rejects unsupported job types.

Accepted work is linked by the deterministic assignment/result/validation/contribution message IDs. `run-local-flow` writes its result artifacts under its supplied output directory and produces a receipt document whose entries include `job_id`, `job_type`, `node_id`, `assignment_message_id`, `result_message_id`, `validation_message_id`, `contribution_message_id`, `result_hash`, validation state, and credited units.

## Local audit inspection

`GET /api/audit-events` and `NodeRuntimeService.inspect_local_audit_events(...)` provide a read-only, local-only inspection surface for submitted local jobs. Optional filters are integer Unix-second `start_time` and `end_time`; non-empty `event_type` (`job_submitted` or `job_executed`), `node_id`, `manifest_id`, `receipt_id`, `lineage_id`, and `contribution_attribution_id`; and pagination with `limit` (1 through 100, default 50) and non-negative `offset` (default 0).

The response is versioned and contains the normalized query, `total_matching`, and a newest-first event page. Every event contains its timestamp, event type, actor and creator node IDs, validation status, relative artifact references, lineage parent references, and preserved contribution attribution. `manifest_id` is the local job ID; `lineage_id` and `contribution_attribution_id` are stable local IDs derived from it. Pending submissions have no receipt; executed entries preserve receipt ID/reference and passed or failed validation. Inspection only reads existing submission, status, and receipt JSON: it does not create, edit, delete, normalize, or append audit logs, manifests, receipts, lineage records, or attribution data. Invalid filters, reversed time ranges, malformed local evidence, and unknown event types return a clear local error; this is local evidence, not consensus.

Invalid manifests, malformed existing receipt documents, and invalid runtime identity/manifest relationships fail with a concrete error before writing the dependent manifest, dispatch log, or receipt. In particular, an existing persisted runtime with a missing creator identity or startup manifest is rejected rather than silently replaced.

## Validation response contract

`validate-local-results` accepts only existing local assignment and result logs plus a new local validation-report path. Its response/report is deterministic and identifies each checked result by assignment ID, result ID, job ID, correlation ID, and result sender. The report's `kind: local_validation_report` identifies the local replay validator boundary; it does not currently carry a node validator identity or create/link a flow receipt.

During `run-local-flow`, the emitted `job_validated` message carries `validator_id` (the local worker node ID). A validation-linked flow receipt expresses the result as:

```json
{
  "job_id": "job identifier",
  "node_id": "worker node identifier",
  "result_hash": "sha256 or deterministic result hash",
  "validation": {
    "valid": true,
    "reason": "validation detail"
  },
  "validation_message_id": "local validator evidence reference",
  "contribution_message_id": "local attribution reference",
  "credited_units": 1
}
```

For startup validation, the receipt additionally records `timestamp`, `creator_node_id`, `manifest_ref`, `manifest_hash`, and `validation_result` with `accepted`, `status`, and `fail_closed`. For flow validation, validator identity is read from the linked `job_validated` message rather than duplicated in the receipt. It is the local worker node ID, not an independent validator or remote consensus participant. Evidence is a local message ID and result hash; failure detail is the validation reason/error stored in the local report or flow receipt. Startup validation is a separate lifecycle check tied to the startup manifest hash.

A failed validation is returned as failed/invalid evidence with its reason and must not receive validated contribution credit. A receipt is evidence of local validation only, not consensus.

## Traceability rule and required planned binding

`audit-local-flow` can follow a work output from the batch-manifest reference and assignment through its result, flow receipt, validation message, contribution message, and ledger record. Separately, `inspect_local_node_runtime` can follow a startup runtime through its preserved creator node ID, startup manifest, startup receipt, startup lineage, and startup attribution references.

No current function proves that a `run-local-flow` worker is the same preserved creator identity inspected by `inspect_local_node_runtime`, and flow receipts do not contain startup-lineage references. Callers must not combine those two audit chains or claim end-to-end creator/lineage traceability.

Phase 1 therefore reserves one planned function/CLI boundary, not yet exposed: `run_local_runtime_flow(runtime_dir, manifest_path, output_dir)` / `run-local-runtime-flow`. It must load the preserved identity and accepted startup manifest from `runtime_dir`, require the selected batch worker to match that local node, and return the normal flow summary plus `creator_node_id`, `startup_manifest_ref`, `startup_manifest_hash`, `startup_lineage_ref`, and `startup_receipt_ref`. The same immutable references must be copied into each flow receipt so an output has one audit chain to its manifest, creator identity, validation evidence, lineage, and contribution record.

Before writing assignments, results, receipts, or contribution records, this boundary must reject a missing or mismatched identity, startup manifest, startup receipt, startup lineage record, worker node, or existing output artifact set. `audit-local-flow` must then verify all runtime references and hashes. Until that implementation and its failure-without-mutation tests exist, the currently exposed operations satisfy the two separate audit chains only; they do not satisfy the planned end-to-end traceability boundary.

## Explicitly out of scope

Phase 1 does not expose or imply:

- HTTP endpoints to create nodes, upload manifests, or mutate receipts; `POST /api/jobs` is the one local-only submission route documented above;
- remote transport, peer discovery, remote consensus, or distributed validation;
- tokenomics, reward math, balances, payouts, or public reputation;
- public dashboards or external-facing administration;
- production networking, authentication/authorization, multi-tenant isolation, or production durability guarantees;
- claims that the prototype is decentralized, an AER router, or a production REVA pipeline.

Later phases may add new versioned boundaries. They must not reinterpret this local artifact contract as proof of network-wide acceptance.

## Verification mapping

The boundary is exercised by focused tests:

- `tests/test_local_startup.py`: startup creates local identity, manifest, receipt, lineage, work directories, and preserves the creator ID; invalid existing identity/manifest state fails without replacement.
- `tests/test_runtime.py`: runtime start/stop/restart preserves identity, manifest, receipt, lineage, and attribution references; status reports local-only provenance.
- `tests/test_cli.py`: malformed manifests and malformed existing receipt artifacts fail without overwriting dependent dispatch/receipt artifacts; local flow produces deterministic receipts.
- `tests/test_receipts.py` and `tests/test_flow_audit.py`: receipt structure, validation-gated credit, deterministic ordering, and flow-artifact audit. They do not test a startup creator/lineage binding because that boundary does not exist yet.
