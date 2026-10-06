#!/usr/bin/env python3
"""Portable local validation runner for AetherMesh SDK.

The automation loop uses this script before opening a PR so local validation
matches the repository's GitHub quality gates closely enough to catch failures
early. ``fast`` is intended for normal autonomous-loop cycles; ``full`` adds
slower parity checks such as security, repeatability, and packaging. Mutation
testing is an explicit, early-phase opt-in rather than a normal blocking gate.
"""

from __future__ import annotations

import argparse
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from pathlib import Path
from typing import IO, Literal, TypedDict, Unpack

ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class Check:
    """One validation command and its scheduling constraints."""

    name: str
    command: tuple[str, ...]
    required: bool = True
    env: dict[str, str] | None = None
    dependencies: tuple[str, ...] = ()
    resources: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True)
class CheckResult:
    """Buffered result of one validation command."""

    check: Check
    returncode: int
    output: str
    canceled: bool = False
    skipped: bool = False


Runner = Callable[[Check, threading.Event], CheckResult]


class _TextPopenKwargs(TypedDict):
    """Keyword arguments for the validation runner's text-mode child process."""

    cwd: Path
    env: dict[str, str]
    stdout: IO[str]
    stderr: int
    text: Literal[True]
    start_new_session: bool
    creationflags: int


class _ProcessRegistry:
    """Track child process groups so fail-fast and SIGINT can clean them up."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._processes: set[subprocess.Popen[str]] = set()

    def spawn(
        self,
        cancel_event: threading.Event,
        command: Sequence[str],
        **kwargs: Unpack[_TextPopenKwargs],
    ) -> subprocess.Popen[str] | None:
        """Atomically check cancellation, spawn, and register a child."""

        with self._lock:
            if cancel_event.is_set():
                return None
            process = subprocess.Popen(command, **kwargs)
            self._processes.add(process)
            return process

    def discard(self, process: subprocess.Popen[str]) -> None:
        with self._lock:
            self._processes.discard(process)

    def terminate_all(self) -> None:
        with self._lock:
            processes = list(self._processes)
        for process in processes:
            _terminate_process_group(process)


_ACTIVE_PROCESSES = _ProcessRegistry()


def _python(*args: str) -> tuple[str, ...]:
    return (sys.executable, *args)


def _base_checks(base: str) -> list[Check]:
    return [
        Check(
            "pytest",
            _python("-m", "pytest", "-q", "tests"),
            env=_quiet_py_env("full-test-pytest"),
            resources=frozenset({"pytest-main"}),
        ),
        Check(
            "branch coverage",
            _python(
                "-m",
                "pytest",
                "-q",
                "--cov=aethermesh_core",
                "--cov-branch",
                "--cov-report=term-missing",
                "--cov-fail-under=100",
                "tests",
            ),
            env=_quiet_py_env("full-test-coverage", coverage=True),
            resources=frozenset({"pytest-coverage"}),
        ),
        Check(
            "ruff check",
            ("ruff", "check", "src", "tests", "scripts"),
            resources=frozenset({"ruff"}),
        ),
        Check(
            "ruff format",
            ("ruff", "format", "--check", "src", "tests", "scripts"),
            resources=frozenset({"ruff"}),
        ),
        Check(
            "mypy",
            ("mypy", "--strict", "src", "scripts"),
            resources=frozenset({"mypy"}),
        ),
        Check(
            "test integrity",
            _python("scripts/ci_quality_gates.py", "test-integrity"),
        ),
        Check(
            "dependency review",
            _python("scripts/ci_quality_gates.py", "dependency-review", "--base", base),
        ),
        Check(
            "workflow security",
            _python("scripts/ci_quality_gates.py", "workflow-security"),
        ),
        Check(
            "PR size",
            _python(
                "scripts/ci_quality_gates.py",
                "pr-size",
                "--base",
                base,
                "--max-files",
                "80",
                "--max-lines",
                "10000",
                "--max-binary-files",
                "0",
                "--exclude-path-prefix",
                "graphify-out/",
                "--exclude-path-prefix",
                "wordlists/node-names/",
            ),
        ),
    ]


def _full_extra_checks(*, include_mutation: bool) -> list[Check]:
    checks = [
        Check(
            "duplicate code",
            (
                "npx",
                "--yes",
                "jscpd@5.0.11",
                "src",
                "--format",
                "python",
                "--threshold",
                "0",
                "--reporters",
                "console",
                "--ignore",
                "**/__pycache__/**",
                "--no-tips",
            ),
            resources=frozenset({"node"}),
        ),
        Check(
            "complexity", ("radon", "cc", "src", "tests", "scripts", "-s", "-n", "E")
        ),
        Check(
            "dead code",
            ("vulture", "src", "tests", "scripts", "--min-confidence", "100"),
        ),
        Check("bandit", ("bandit", "-q", "-r", "src")),
        Check("pip audit", _python("scripts/ci_quality_gates.py", "dependency-audit")),
        Check(
            "build",
            _python("-m", "build"),
            resources=frozenset({"build-dist"}),
        ),
        Check(
            "install smoke",
            _python("scripts/ci_quality_gates.py", "install-smoke", "--dist", "dist"),
            dependencies=("build",),
            resources=frozenset({"build-dist"}),
        ),
        Check(
            "artifact provenance",
            _python(
                "scripts/ci_quality_gates.py", "artifact-provenance", "--dist", "dist"
            ),
            dependencies=("build",),
            resources=frozenset({"build-dist"}),
        ),
        Check(
            "secret scan",
            (
                "gitleaks",
                "detect",
                "--no-banner",
                "--source",
                ".",
                "--redact",
                "--verbose",
            ),
        ),
        Check("flaky tests", _python("scripts/ci_quality_gates.py", "flaky-tests")),
    ]
    if include_mutation:
        checks.extend(
            [
                Check(
                    "mutation",
                    _python("scripts/mutmut_early_fail.py", "--max-children", "4"),
                    dependencies=("pytest", "branch coverage"),
                    resources=frozenset({"mutation"}),
                ),
                Check(
                    "mutation score",
                    _python(
                        "scripts/ci_quality_gates.py",
                        "mutation-score",
                        "--minimum",
                        "95",
                    ),
                    dependencies=("mutation",),
                    resources=frozenset({"mutation"}),
                ),
            ]
        )
    return checks


def _quiet_py_env(cache_name: str, *, coverage: bool = False) -> dict[str, str]:
    cache_option = f"-o cache_dir=.pytest_cache/{cache_name}"
    inherited_options = os.environ.get("PYTEST_ADDOPTS", "")
    env = {
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTEST_ADDOPTS": f"{inherited_options} {cache_option}".strip(),
    }
    if coverage:
        env["COVERAGE_FILE"] = ".coverage"
    return env


def _command_available(command: str) -> bool:
    if command == sys.executable:
        return True
    return shutil.which(command) is not None


def _missing_required_tools(checks: Iterable[Check]) -> list[str]:
    missing: list[str] = []
    for check in checks:
        executable = check.command[0]
        if check.required and not _command_available(executable):
            missing.append(f"{check.name}: {executable}")
    return missing


def _merged_env(overrides: dict[str, str] | None) -> dict[str, str]:
    env = os.environ.copy()
    if overrides:
        env.update(overrides)
    return env


def _terminate_process_group(process: subprocess.Popen[str]) -> None:
    """Force-stop and reap one child in its owned process group.

    Built-in validation commands must keep descendants in the session/process
    group created at spawn; commands that escape it are unsupported.
    """

    if process.poll() is not None:
        return
    if os.name == "posix":
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except (PermissionError, ProcessLookupError):
            pass
    else:  # pragma: no cover - Windows portability path
        subprocess.run(
            ["taskkill", "/PID", str(process.pid), "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    process.wait()


def _run_check(check: Check, cancel_event: threading.Event) -> CheckResult:
    with tempfile.TemporaryFile(
        mode="w+", encoding="utf-8", errors="replace"
    ) as output:
        process = _ACTIVE_PROCESSES.spawn(
            cancel_event,
            check.command,
            cwd=ROOT,
            env=_merged_env(check.env),
            stdout=output,
            stderr=subprocess.STDOUT,
            text=True,
            start_new_session=os.name == "posix",
            creationflags=(
                getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
                if os.name == "nt"
                else 0
            ),
        )
        if process is None:
            return CheckResult(check, -signal.SIGKILL, "", canceled=True)
        try:
            returncode = int(process.wait())
        finally:
            _ACTIVE_PROCESSES.discard(process)
            output.flush()
            output.seek(0)
            buffered = output.read()

    return CheckResult(
        check,
        returncode,
        buffered,
        canceled=cancel_event.is_set() and returncode != 0,
    )


def _checks_for_mode(
    mode: str, base: str, *, include_mutation: bool = False
) -> list[Check]:
    checks = _base_checks(base)
    if mode == "full":
        checks.extend(_full_extra_checks(include_mutation=include_mutation))
    return checks


def _validate_dag(checks: Sequence[Check]) -> None:
    names = [check.name for check in checks]
    if len(names) != len(set(names)):
        raise ValueError("validation check names must be unique")
    known = set(names)
    completed: set[str] = set()
    remaining = list(checks)
    while remaining:
        ready = [check for check in remaining if set(check.dependencies) <= completed]
        if not ready:
            unknown = sorted(
                {dependency for check in remaining for dependency in check.dependencies}
                - known
            )
            detail = (
                f"unknown dependencies: {', '.join(unknown)}" if unknown else "cycle"
            )
            raise ValueError(f"invalid validation DAG ({detail})")
        for check in ready:
            completed.add(check.name)
            remaining.remove(check)


def _ready_checks(
    pending: Sequence[Check], completed: set[str], active_resources: set[str]
) -> list[Check]:
    return [
        check
        for check in pending
        if set(check.dependencies) <= completed
        and not (check.resources & active_resources)
    ]


def _print_result(result: CheckResult) -> None:
    check = result.check
    print(f"\n==> {check.name}")
    print("$ " + " ".join(check.command))
    if result.output and not result.skipped:
        print(result.output, end="" if result.output.endswith("\n") else "\n")
    if result.skipped:
        print(f"SKIPPED: {check.name} ({result.output})")
    elif result.canceled:
        print(f"CANCELLED: {check.name}")
    elif result.returncode == 0:
        print(f"PASS: {check.name}")
    else:
        print(f"FAIL: {check.name} exited {result.returncode}")


def _result_succeeded(result: CheckResult) -> bool:
    return result.returncode == 0 and not result.canceled and not result.skipped


def run_checks(
    checks: Sequence[Check],
    *,
    keep_going: bool,
    jobs: int,
    runner: Runner = _run_check,
) -> int:
    """Run a bounded validation DAG and report buffered results deterministically."""
    _validate_dag(checks)
    missing = _missing_required_tools(checks)
    if missing:
        print("Missing required validation tools:", file=sys.stderr)
        for item in missing:
            print(f"- {item}", file=sys.stderr)
        return 127

    cancel_event = threading.Event()
    pending = list(checks)
    completed: set[str] = set()
    active_resources: set[str] = set()
    running: dict[Future[CheckResult], Check] = {}
    results: dict[str, CheckResult] = {}
    fail_fast_triggered = False
    interrupted = False

    executor = ThreadPoolExecutor(max_workers=jobs, thread_name_prefix="validation")
    try:
        while pending or running:
            while True:
                blocked = [
                    check
                    for check in pending
                    if set(check.dependencies) <= completed
                    and any(
                        not _result_succeeded(results[dependency])
                        for dependency in check.dependencies
                    )
                ]
                if not blocked:
                    break
                for check in blocked:
                    unsuccessful = ", ".join(
                        dependency
                        for dependency in check.dependencies
                        if not _result_succeeded(results[dependency])
                    )
                    result = CheckResult(
                        check,
                        0,
                        f"unsuccessful prerequisites: {unsuccessful}",
                        skipped=True,
                    )
                    pending.remove(check)
                    completed.add(check.name)
                    results[check.name] = result

            capacity = jobs - len(running)
            if capacity and not fail_fast_triggered:
                while capacity:
                    ready = _ready_checks(pending, completed, active_resources)
                    if not ready:
                        break
                    check = ready[0]
                    pending.remove(check)
                    active_resources.update(check.resources)
                    running[executor.submit(runner, check, cancel_event)] = check
                    capacity -= 1

            if not running:
                break

            done, _ = wait(running, return_when=FIRST_COMPLETED)
            for future in done:
                check = running.pop(future)
                active_resources.difference_update(check.resources)
                try:
                    result = future.result()
                except Exception as exc:  # noqa: BLE001 - justification: runner failures must be reported without aborting other checks.
                    result = CheckResult(
                        check,
                        1,
                        f"runner raised {type(exc).__name__}: {exc}\n",
                    )
                results[check.name] = result
                completed.add(check.name)
                if result.returncode != 0 and not result.canceled and not keep_going:
                    fail_fast_triggered = True

            if fail_fast_triggered:
                cancel_event.set()
                _ACTIVE_PROCESSES.terminate_all()
                pending.clear()
    except KeyboardInterrupt:
        interrupted = True
        cancel_event.set()
        _ACTIVE_PROCESSES.terminate_all()
        pending.clear()
    finally:
        executor.shutdown(wait=True, cancel_futures=True)

    ordered_results: list[CheckResult] = []
    for check in checks:
        maybe_result = results.get(check.name)
        if maybe_result is not None:
            ordered_results.append(maybe_result)
            _print_result(maybe_result)

    failures = [
        check_result
        for check_result in ordered_results
        if check_result.returncode != 0 and not check_result.canceled
    ]
    if interrupted:
        print("\nValidation interrupted.", file=sys.stderr)
        return 130
    if failures:
        print("\nValidation failed:", file=sys.stderr)
        for result in failures:
            print(f"- {result.check.name}: exit {result.returncode}", file=sys.stderr)
        return 1

    print("\nValidation passed.")
    return 0


def _positive_jobs(value: str) -> int:
    jobs = int(value)
    if jobs < 1:
        raise argparse.ArgumentTypeError("--jobs must be at least 1")
    return jobs


def _default_jobs() -> int:
    return min(4, max(1, os.cpu_count() or 1))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--mode",
        choices=("fast", "full"),
        default="fast",
        help=(
            "Validation set to run. fast runs the ordinary quick gates; full adds "
            "ordinary security, repeatability, and packaging gates but not mutation."
        ),
    )
    parser.add_argument(
        "--include-mutation",
        action="store_true",
        help=(
            "Explicit opt-in for the early-phase, optional/nonblocking mutation and "
            "mutation-score checks; valid only with --mode full."
        ),
    )
    parser.add_argument(
        "--base",
        default="origin/main",
        help="Base ref for diff-aware checks such as dependency review and PR size.",
    )
    parser.add_argument(
        "--keep-going",
        action="store_true",
        help="Run remaining checks after a failure and report all failures at the end.",
    )
    parser.add_argument(
        "--jobs",
        type=_positive_jobs,
        default=_default_jobs(),
        help="Maximum concurrent checks (default: up to 4 based on available CPUs).",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the checks that would run, then exit.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.include_mutation and args.mode != "full":
        parser.error("--include-mutation requires --mode full")
    checks = _checks_for_mode(
        args.mode, args.base, include_mutation=args.include_mutation
    )
    if args.list:
        for check in checks:
            print(f"{check.name}: {' '.join(check.command)}")
        return 0
    started = time.monotonic()
    result = run_checks(checks, keep_going=args.keep_going, jobs=args.jobs)
    print(f"Validation wall time: {time.monotonic() - started:.2f}s")
    return result


if __name__ == "__main__":
    raise SystemExit(main())
