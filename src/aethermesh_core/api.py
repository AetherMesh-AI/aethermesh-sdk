"""Headless local FastAPI app for AetherMesh node integrations."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Annotated, Any
from uuid import uuid4

from fastapi import Body, FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from aethermesh_core.runtime_service import (
    NodeRuntimeService,
    ResultReportNotFoundError,
    RuntimeServiceError,
    ValidationReceiptNotFoundError,
)

logger = logging.getLogger(__name__)


def _request_id(request: Request) -> str:
    return getattr(request.state, "request_id", "unavailable")


def _error_response(
    request: Request,
    *,
    status_code: int,
    code: str,
    message: str,
) -> JSONResponse:
    """Return the stable, deliberately non-provenance API error envelope."""

    return JSONResponse(
        status_code=status_code,
        content={
            "error": {"code": code, "message": message, "details": {}},
            "request_id": _request_id(request),
        },
    )


def _runtime_error_code(
    request: Request, error: RuntimeServiceError
) -> tuple[int, str, str]:
    """Classify expected local runtime failures without exposing their text."""

    diagnostic = str(error).lower()
    if request.url.path == "/api/validation-receipts":
        return 400, "VALIDATION_FAILURE", "Local validation evidence could not be read."
    if (
        request.url.path == "/api/audit-events"
        and "manifest_id must be a local job id" in diagnostic
    ):
        return 400, "INVALID_INPUT", "The request is invalid."
    if "contribution_attribution" in diagnostic:
        return (
            400,
            "CONTRIBUTION_ATTRIBUTION_FAILURE",
            "Contribution attribution could not be read.",
        )
    if "trace" in diagnostic:
        return (
            400,
            "TRACE_LINEAGE_FAILURE",
            "Local attribution trace evidence is incomplete.",
        )
    if "lineage" in diagnostic:
        return 400, "LINEAGE_LOOKUP_FAILURE", "Lineage evidence could not be read."
    if "manifest" in diagnostic:
        return 404, "MISSING_MANIFEST", "A required local manifest is unavailable."
    return 400, "INVALID_INPUT", "The request is invalid."


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    service: NodeRuntimeService = app.state.service
    service.start_node_runtime()
    try:
        yield
    finally:
        service.mark_runtime_stopped()


def create_app(service: NodeRuntimeService | None = None) -> FastAPI:
    """Create the headless localhost API used by SDK integrations."""

    runtime_service = service or NodeRuntimeService.default()
    app = FastAPI(
        title="AetherMesh Local Node API",
        version="0.2.0-alpha",
        lifespan=_lifespan,
    )
    app.state.service = runtime_service

    @app.middleware("http")
    async def assign_request_id(request: Request, call_next: Any) -> Any:
        request.state.request_id = uuid4().hex
        return await call_next(request)

    @app.exception_handler(RequestValidationError)
    async def invalid_input_handler(
        request: Request, error: RequestValidationError
    ) -> JSONResponse:
        logger.warning(
            "local API invalid input request_id=%s path=%s errors=%s",
            _request_id(request),
            request.url.path,
            error.errors(),
        )
        return _error_response(
            request,
            status_code=400,
            code="INVALID_INPUT",
            message="The request is invalid.",
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, error: StarletteHTTPException
    ) -> JSONResponse:
        code = "NOT_FOUND" if error.status_code == 404 else "INVALID_INPUT"
        message = (
            "The local API route was not found."
            if code == "NOT_FOUND"
            else "The request is invalid."
        )
        logger.warning(
            "local API HTTP failure request_id=%s path=%s status=%s",
            _request_id(request),
            request.url.path,
            error.status_code,
        )
        return _error_response(
            request,
            status_code=error.status_code,
            code=code,
            message=message,
        )

    @app.exception_handler(ValidationReceiptNotFoundError)
    async def missing_receipt_handler(
        request: Request, error: ValidationReceiptNotFoundError
    ) -> JSONResponse:
        logger.warning(
            "local API validation receipt missing request_id=%s path=%s error=%s",
            _request_id(request),
            request.url.path,
            error,
        )
        return _error_response(
            request,
            status_code=404,
            code="VALIDATION_FAILURE",
            message="Local validation evidence was not found.",
        )

    @app.exception_handler(ResultReportNotFoundError)
    async def missing_result_report_handler(
        request: Request, error: ResultReportNotFoundError
    ) -> JSONResponse:
        logger.warning(
            "local API result report missing request_id=%s path=%s error=%s",
            _request_id(request),
            request.url.path,
            error,
        )
        return _error_response(
            request,
            status_code=404,
            code="RESULT_REPORT_NOT_FOUND",
            message="Local result report was not found.",
        )

    @app.exception_handler(RuntimeServiceError)
    async def runtime_error_handler(
        request: Request, error: RuntimeServiceError
    ) -> JSONResponse:
        status_code, code, message = _runtime_error_code(request, error)
        logger.warning(
            "local API runtime failure request_id=%s path=%s code=%s error=%s",
            _request_id(request),
            request.url.path,
            code,
            error,
        )
        return _error_response(
            request, status_code=status_code, code=code, message=message
        )

    @app.exception_handler(Exception)
    async def internal_error_handler(
        request: Request, error: Exception
    ) -> JSONResponse:
        logger.exception(
            "local API internal failure request_id=%s path=%s",
            _request_id(request),
            request.url.path,
        )
        return _error_response(
            request,
            status_code=500,
            code="INTERNAL_ERROR",
            message="An internal local API error occurred.",
        )

    @app.get("/health")
    def health() -> dict[str, Any]:
        return runtime_service.health()

    @app.get("/status")
    @app.get("/api/status")
    def status() -> dict[str, Any]:
        return runtime_service.get_node_status()

    @app.get("/version")
    def version() -> dict[str, Any]:
        return runtime_service.package_info()

    @app.get("/node")
    @app.get("/api/node")
    def node() -> dict[str, Any]:
        return runtime_service.get_node_status()

    @app.get("/peers")
    @app.get("/api/peers")
    def peers() -> dict[str, Any]:
        return runtime_service.list_peers()

    @app.get("/api/jobs")
    def jobs() -> dict[str, Any]:
        return runtime_service.list_jobs()

    @app.post("/api/jobs")
    def submit_job(request: dict[str, Any]) -> dict[str, Any]:
        """Submit one local-only job for later execution and validation."""

        return runtime_service.submit_local_job_status(request)

    @app.get("/api/jobs/{job_id}")
    def job_status(job_id: str) -> dict[str, Any]:
        """Return one local submitted job's lifecycle and preserved evidence."""

        if not runtime_service._is_local_job_id(job_id):
            raise RuntimeServiceError("job_id must be a local job ID")
        return runtime_service.get_local_job_status(job_id)

    @app.get("/api/jobs/{job_id}/result")
    def job_result(job_id: str) -> dict[str, Any]:
        """Read one persisted local result report without changing job state."""

        return runtime_service.get_local_job_result(job_id)

    @app.get("/api/jobs/{job_id}/trace")
    def job_attribution_trace(job_id: str) -> dict[str, Any]:
        """Read one validation-gated local attribution chain without mutation."""

        return runtime_service.trace_local_job_attribution(job_id)

    @app.get("/api/result-reports")
    def result_reports() -> dict[str, Any]:
        """List deterministic, metadata-only summaries of local result reports."""

        return runtime_service.list_local_job_results()

    @app.post("/api/result-reports/preflight")
    def preflight_result_report(report: Annotated[Any, Body()]) -> dict[str, Any]:
        """Reject malformed report candidates before local validation can begin."""

        return runtime_service.preflight_local_result_report(report)

    @app.get("/api/contributions")
    def contributions() -> dict[str, Any]:
        """Return read-only local contribution attribution and validation evidence."""

        return runtime_service.contribution_summary()

    @app.get("/api/validation-receipts")
    def validation_receipts(
        receipt_id: str | None = None,
        work_id: str | None = None,
        latest: str | None = None,
    ) -> dict[str, Any]:
        """List receipts, or retain the legacy query detail lookup.

        List response v1 contains stable receipt ID, validation status/timestamp,
        work and provenance references, creator, attribution, and summary. Detail
        responses are read-only persisted-evidence projections; neither creates,
        recomputes, or changes validation outcomes.
        """

        if latest not in (None, "true"):
            raise RuntimeServiceError("latest must be true when provided")
        if receipt_id is None and work_id is None and latest is None:
            return runtime_service.list_local_validation_receipts()
        return runtime_service.get_local_validation_receipt(
            receipt_id=receipt_id, work_id=work_id, latest=latest == "true"
        )

    @app.get("/api/validation-receipts/{receipt_id}")
    def validation_receipt_detail(receipt_id: str) -> dict[str, Any]:
        """Read the full persisted local receipt for one stable receipt ID only."""

        return runtime_service.get_local_validation_receipt(receipt_id=receipt_id)

    @app.get("/api/audit-events")
    def audit_events(
        start_time: int | None = None,
        end_time: int | None = None,
        event_type: str | None = None,
        node_id: str | None = None,
        manifest_id: str | None = None,
        receipt_id: str | None = None,
        lineage_id: str | None = None,
        contribution_attribution_id: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> dict[str, Any]:
        """Read local audit evidence; this route never writes runtime artifacts."""
        return runtime_service.inspect_local_audit_events(
            start_time=start_time,
            end_time=end_time,
            event_type=event_type,
            node_id=node_id,
            manifest_id=manifest_id,
            receipt_id=receipt_id,
            lineage_id=lineage_id,
            contribution_attribution_id=contribution_attribution_id,
            limit=limit,
            offset=offset,
        )

    @app.get("/capabilities")
    @app.get("/api/capabilities")
    def capabilities() -> dict[str, Any]:
        return runtime_service.list_capabilities()

    @app.get("/api/capability-records")
    def capability_records() -> dict[str, Any]:
        """Inspect persisted local capability records without modifying them."""

        return runtime_service.inspect_capability_records()

    @app.get("/api/model-manifests")
    def model_manifests() -> dict[str, Any]:
        """Inspect redacted summaries of local model/expert manifests."""

        return runtime_service.inspect_model_manifests()

    @app.get("/api/package")
    def package() -> dict[str, Any]:
        return runtime_service.package_info()

    @app.get("/api/network")
    def network() -> dict[str, Any]:
        return runtime_service.network_health()

    @app.get("/logs")
    @app.get("/api/logs")
    def logs() -> dict[str, Any]:
        return runtime_service.recent_logs()

    @app.get("/api/events")
    def events() -> dict[str, Any]:
        return runtime_service.recent_logs()

    @app.post("/shutdown")
    def shutdown() -> dict[str, Any]:
        app.state.shutdown_requested = True
        return {"shutdown_requested": True, "restart_requested": False}

    @app.post("/restart")
    def restart() -> dict[str, Any]:
        app.state.restart_requested = True
        return {"shutdown_requested": True, "restart_requested": True}

    @app.get("/")
    def service_index() -> dict[str, Any]:
        """Describe the local service and its machine-readable entry points."""

        health = runtime_service.health()
        return {
            "service": health["service"],
            "version": health["version"],
            "status": health["status"],
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
        }

    return app
