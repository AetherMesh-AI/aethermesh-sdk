"""Public one-session downloads of bounded opaque bytes with explicit publication."""

from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from .client import PeerClient
from .config import PeerEndpoint, TLSIdentity, validate_fingerprint, validate_project_id
from .content_files import require_absent, selected_parent
from .errors import PeerError, RequestTimeout
from .transfer import TransferManifest
from .transfer_files import publish_transfer, resume_download


@dataclass(frozen=True)
class DownloadProgress:
    """Verified checkpoint facts, never wire-byte estimates or publisher approval."""

    phase: Literal["manifest", "chunk", "verified"]
    manifest: TransferManifest
    verified_bytes: int
    verified_chunks: int
    chunks_fetched: int
    chunks_reused: int


@dataclass(frozen=True)
class DownloadResult:
    """Completed publication; no Git validity, execution or release authorization."""

    manifest: TransferManifest
    output: Path
    chunks_fetched: int
    chunks_reused: int
    integrity_verified: Literal[True] = True
    publisher_authorized: Literal[False] = False


class DownloadError(PeerError):
    """Stable local failure; publication_attempted means output may have committed.

    No output rollback or deletion is attempted. The chained cause is local
    diagnostic data and may contain paths or callback-provided sensitive text.
    """

    def __init__(
        self,
        code: Literal[
            "output_exists",
            "invalid_state",
            "local_io",
            "callback_failed",
            "publication_failed",
        ],
        *,
        publication_attempted: bool = False,
    ) -> None:
        super().__init__(f"Download failed: {code}.")
        self.code = code
        self.publication_attempted = publication_attempted


async def download_transfer(
    identity: TLSIdentity,
    endpoint: PeerEndpoint,
    *,
    project_id: str,
    manifest_sha256: str,
    staging: Path,
    output: Path,
    resume: bool = False,
    timeout: float = 30.0,
    on_progress: Callable[[DownloadProgress], Awaitable[None]] | None = None,
) -> DownloadResult:
    """Own one session, private checkpoints and no-overwrite output publication.

    Callbacks are awaited serially before publication, under the total timeout;
    they must cooperate with cancellation and never block the event loop. Cancel
    this coroutine's task to stop its session only. Checkpoints are retained and
    resume requires a new explicit call with the same manifest and staging path.
    Synchronous file operations are byte-bounded, not hard time/CPU bounded.
    """
    validate_project_id(project_id)
    validate_fingerprint(manifest_sha256)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (float, int))
        or not math.isfinite(timeout)
        or not 0 < timeout <= 3600
        or type(resume) is not bool
        or not isinstance(staging, Path)
        or not isinstance(output, Path)
        or (on_progress is not None and not callable(on_progress))
    ):
        raise ValueError("invalid download configuration")
    deadline = asyncio.get_running_loop().time() + timeout

    async def notify(progress: DownloadProgress) -> None:
        if on_progress is not None:
            try:
                await on_progress(progress)
            except Exception as exc:
                raise DownloadError("callback_failed") from exc
        # Deliver cancellation even for synchronous-completing callbacks/cache hits.
        await asyncio.sleep(0)

    publication_attempted = False
    try:
        with selected_parent(output) as (directory, name):
            require_absent(directory, name)
            # Connection cleanup remains owned by the context manager, even when
            # the total operation timeout expires or a callback fails/cancels.
            async with PeerClient(identity, project_id=project_id) as client:
                async with asyncio.timeout_at(deadline):
                    await client.connect(endpoint)
                    manifest = await client.fetch_manifest(
                        manifest_sha256, timeout=timeout
                    )
                    await notify(DownloadProgress("manifest", manifest, 0, 0, 0, 0))

                    async def chunk(index: int, fetched: int) -> None:
                        count = index + 1
                        await notify(
                            DownloadProgress(
                                "chunk",
                                manifest,
                                min(count * 32768, manifest.size),
                                count,
                                fetched,
                                count - fetched,
                            )
                        )

                    data, fetched = await resume_download(
                        client,
                        manifest,
                        staging,
                        resume=resume,
                        timeout=timeout,
                        on_chunk=chunk,
                    )
                    await notify(
                        DownloadProgress(
                            "verified",
                            manifest,
                            manifest.size,
                            len(manifest.chunks),
                            fetched,
                            len(manifest.chunks) - fetched,
                        )
                    )
            result = DownloadResult(
                manifest, output, fetched, len(manifest.chunks) - fetched
            )
            await asyncio.sleep(0)
            if asyncio.get_running_loop().time() >= deadline:
                raise RequestTimeout("Download exceeded its deadline.")
            # No await or application callback after this point. Link publication
            # is irreversible; fsync/cleanup failures conservatively report uncertainty.
            publication_attempted = True
            publish_transfer(directory, name, data)
        return result
    except TimeoutError as exc:
        if publication_attempted:
            raise DownloadError(
                "publication_failed", publication_attempted=True
            ) from exc
        raise RequestTimeout("Download exceeded its deadline.") from exc
    except FileExistsError as exc:
        raise DownloadError(
            "output_exists", publication_attempted=publication_attempted
        ) from exc
    except OSError as exc:
        code: Literal["publication_failed", "local_io"] = (
            "publication_failed" if publication_attempted else "local_io"
        )
        raise DownloadError(code, publication_attempted=publication_attempted) from exc
    except ValueError as exc:
        raise DownloadError(
            "invalid_state", publication_attempted=publication_attempted
        ) from exc
