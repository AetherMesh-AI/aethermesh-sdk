"""Explicit trusted-local export only; no untrusted pack ingestion or ref movement.

Git operates in our scratch bare repository, not the source configuration. Its
built-in pack-objects command reads an explicitly trusted complete object store.
Process limits are containment aids for trusted export, not a hostile-code sandbox.
"""

from __future__ import annotations

import hashlib
import math
import os
import shutil
import stat

# Subprocesses are fixed trusted-local Git builtins, with no shell or peer code.
import subprocess  # nosec B404
import sys
import tempfile
import time
from pathlib import Path

from .bundle import BundleInfo, inspect_bundle, validate_branch, validate_oid
from .config import validate_project_id
from .content_files import read_selected, require_absent, selected_parent
from .transfer import MAX_TRANSFER_BYTES
from .transfer_files import publish_transfer

# Exec the trusted Git binary in the same process/group. No preexec_fn, detached
# session, shell, persistent settings, or downloaded helper is involved.
_LIMIT_EXEC = """import os,resource,sys
size,cpu=int(sys.argv[1]),int(sys.argv[2])
resource.setrlimit(resource.RLIMIT_FSIZE,(size,size))
resource.setrlimit(resource.RLIMIT_CPU,(cpu,cpu))
resource.setrlimit(resource.RLIMIT_NOFILE,(64,64))
os.execve(sys.argv[3],sys.argv[3:],dict(os.environ))
"""


class _Git:
    def __init__(self, scratch: Path, timeout: float) -> None:
        binary = shutil.which("git", path=os.defpath)
        if binary is None:
            raise ValueError("a trusted system Git executable is required")
        self.binary = binary
        self.scratch = scratch
        self.deadline = time.monotonic() + timeout
        self.environment = {
            "PATH": os.defpath,
            "LC_ALL": "C",
            "GIT_CONFIG_NOSYSTEM": "1",
            "GIT_CONFIG_SYSTEM": os.devnull,
            "GIT_CONFIG_GLOBAL": os.devnull,
            "GIT_TERMINAL_PROMPT": "0",
            "GIT_NO_REPLACE_OBJECTS": "1",
            "GIT_NO_LAZY_FETCH": "1",
        }
        for name in ("objects", "refs", "hooks"):
            (scratch / name).mkdir()
        (scratch / "HEAD").write_text("ref: refs/heads/unused\n", encoding="ascii")
        (scratch / "config").write_text(
            "[core]\nrepositoryformatversion=0\nbare=true\n", encoding="ascii"
        )

    def run(self, *args: str, data: bytes = b"", limit: int = 65536) -> bytes:
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError("bundle export deadline expired")
        command = [
            sys.executable,
            "-I",
            "-c",
            _LIMIT_EXEC,
            str(limit),
            str(max(1, math.ceil(remaining))),
            self.binary,
            "--no-pager",
            f"--git-dir={self.scratch}",
            "-c",
            f"core.hooksPath={self.scratch / 'hooks'}",
            "-c",
            "core.fsmonitor=false",
            "-c",
            "protocol.allow=never",
            "-c",
            "gc.auto=0",
            "-c",
            "maintenance.auto=false",
            *args,
        ]
        with tempfile.TemporaryFile() as output, tempfile.TemporaryFile() as errors:
            try:
                # Fixed executable/operations, scrubbed environment and validated OID stdin.
                result = subprocess.run(  # nosec B603
                    command,
                    input=data,
                    stdout=output,
                    stderr=errors,
                    env=self.environment,
                    timeout=remaining,
                    check=False,
                )
            except subprocess.TimeoutExpired as exc:
                raise TimeoutError("bundle export deadline expired") from exc
            output.seek(0)
            content = output.read(limit + 1)
            if result.returncode or len(content) > limit:
                raise ValueError("bounded Git operation failed")
            return content


def _source(path: Path) -> Path:
    # Validate all explicit path components; linked worktrees are outside v1.
    with selected_parent(path / ".sentinel") as (directory, _):
        try:
            info = os.stat(".git", dir_fd=directory, follow_symlinks=False)
        except FileNotFoundError:
            return path
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("linked or symlinked Git directories are unsupported")
    return path / ".git"


def _objects(source: Path) -> None:
    for name in (
        "shallow",
        "commondir",
        "info/grafts",
        "objects/info/alternates",
        "objects/info/http-alternates",
    ):
        if os.path.lexists(source / name):
            raise ValueError("incomplete or external object sources are unsupported")
    count = 0

    def scan(directory: int, depth: int) -> None:
        nonlocal count
        with os.scandir(directory) as entries:
            for entry in entries:
                count += 1
                info = entry.stat(follow_symlinks=False)
                if (
                    count > 10000
                    or not (stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode))
                    or entry.name.endswith(".promisor")
                ):
                    raise ValueError("unsupported or excessive object store entries")
                if stat.S_ISDIR(info.st_mode):
                    if depth >= 1:
                        raise ValueError("object directory nesting exceeds the bound")
                    child = os.open(
                        entry.name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=directory,
                    )
                    try:
                        scan(child, depth + 1)
                    finally:
                        os.close(child)

    with selected_parent(source / "objects" / ".sentinel") as (directory, _):
        scan(directory, 0)


def _selected_oid(source: Path, ref: str) -> str:
    try:
        value = read_selected(source / ref, limit=64).decode("ascii").removesuffix("\n")
    except FileNotFoundError:
        value = None
    if value is not None:
        validate_oid(value)
    try:
        packed = read_selected(source / "packed-refs", limit=1024 * 1024).decode(
            "ascii"
        )
    except FileNotFoundError:
        packed = ""
    matches = []
    for line in packed.splitlines():
        if not line.startswith(("#", "^")) and line.endswith(" " + ref):
            candidate = line.split(" ")[0]
            validate_oid(candidate)
            if line != f"{candidate} {ref}":
                raise ValueError("ambiguous packed ref encoding")
            matches.append(candidate)
    if len(matches) > 1:
        raise ValueError("ambiguous packed refs")
    oid = value if value is not None else (matches[0] if matches else "")
    validate_oid(oid)
    return oid


def export_bundle(
    source: Path,
    output: Path,
    *,
    project_id: str,
    ref: str,
    expected_oid: str,
    timeout: float = 15.0,
) -> BundleInfo:
    """Export trusted complete SHA-1 objects pinned to one explicit branch/OID.

    Source directories must be trusted and stable; hostile same-user filesystem
    races and hard memory isolation are not provided. Never call on peer stores.
    """
    validate_project_id(project_id)
    validate_branch(ref)
    validate_oid(expected_oid)
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(timeout)
        or not 0 < timeout <= 30
    ):
        raise ValueError("export timeout must be finite and at most 30 seconds")
    with selected_parent(output) as (directory, name):
        require_absent(directory, name)
        repository = _source(source)
        if output.absolute().is_relative_to(repository.absolute()):
            raise ValueError("bundle output must be outside source Git metadata")
        _objects(repository)
        if _selected_oid(repository, ref) != expected_oid:
            raise ValueError("selected ref does not match the expected OID")
        configuration = read_selected(repository / "config", limit=65536)
        with tempfile.TemporaryDirectory(prefix="aethermesh-export-") as temporary:
            scratch = Path(temporary)
            git = _Git(scratch, timeout)
            config_copy = scratch / "source-config"
            config_copy.write_bytes(configuration)
            entries = git.run(
                "config",
                "--file",
                str(config_copy),
                "--no-includes",
                "--null",
                "--list",
            )
            for entry in entries.rstrip(b"\0").split(b"\0"):
                key, _, value = entry.partition(b"\n")
                if (
                    key.startswith((b"include.", b"includeif.", b"extensions."))
                    or key.endswith((b".promisor", b".partialclonefilter"))
                    or (key == b"core.repositoryformatversion" and value != b"0")
                ):
                    raise ValueError("unsupported source configuration")
            git.environment["GIT_OBJECT_DIRECTORY"] = str(
                (repository / "objects").absolute()
            )
            if git.run("cat-file", "-t", expected_oid).strip() != b"commit":
                raise ValueError("expected OID must identify a commit")
            header = f"# v2 git bundle\n{expected_oid} {ref}\n\n".encode("ascii")
            # No --thin, exclusions, filters, revision expressions or source refs.
            pack = git.run(
                "pack-objects",
                "--stdout",
                "--revs",
                "--threads=1",
                "--window-memory=32m",
                data=(expected_oid + "\n").encode("ascii"),
                limit=MAX_TRANSFER_BYTES - len(header),
            )
        data = header + pack
        info = inspect_bundle(
            data,
            project_id=project_id,
            expected_sha256=hashlib.sha256(data).hexdigest(),
        )
        publish_transfer(directory, name, data)
        return info
