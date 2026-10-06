#!/usr/bin/env python3
"""Small CI guardrails that are easier to keep correct in Python than shell."""

from __future__ import annotations

import argparse
import base64
import hashlib
import os
import re
import subprocess
import sys
import tempfile
import tomllib
import venv
import zipfile
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PYPROJECT = ROOT / "pyproject.toml"
FORBIDDEN_SUPPRESSION_RE = re.compile(
    r"(pytest\.mark\.skip|pytest\.mark\.xfail|unittest\.skip|\bskip\(|\bxfail\(|type:\s*ignore|#\s*noqa|pragma:\s*no cover|coverage:\s*ignore)",
    re.IGNORECASE,
)


def run(
    args: list[str],
    *,
    cwd: Path = ROOT,
    input_text: str | None = None,
    env: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        args,
        cwd=cwd,
        input=input_text,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )


def load_pyproject(path: Path = PYPROJECT) -> dict[str, Any]:
    return tomllib.loads(path.read_text(encoding="utf-8"))


def project_dependencies(pyproject: dict[str, Any]) -> set[str]:
    project = pyproject.get("project", {})
    deps = project.get("dependencies", [])
    if not isinstance(deps, list):
        raise SystemExit("[project].dependencies must be a list")
    return {str(dep).strip() for dep in deps if str(dep).strip()}


def dependency_justifications(pyproject: dict[str, Any]) -> dict[str, str]:
    table = (
        pyproject.get("tool", {})
        .get("aethermesh", {})
        .get("dependency_justifications", {})
    )
    if not isinstance(table, dict):
        raise SystemExit("[tool.aethermesh.dependency_justifications] must be a table")
    return {str(key): str(value).strip() for key, value in table.items()}


def changed_pyproject_from_base(base_ref: str) -> dict[str, Any] | None:
    result = run(["git", "show", f"{base_ref}:pyproject.toml"])
    if result.returncode != 0:
        return None
    return tomllib.loads(result.stdout)


def command_test_integrity(_: argparse.Namespace) -> int:
    violations: list[str] = []
    for directory in (ROOT / "src", ROOT / "tests"):
        for path in sorted(directory.rglob("*.py")):
            rel = path.relative_to(ROOT)
            for line_number, line in enumerate(
                path.read_text(encoding="utf-8").splitlines(), 1
            ):
                if (
                    FORBIDDEN_SUPPRESSION_RE.search(line)
                    and "justification:" not in line.lower()
                ):
                    violations.append(
                        f"{rel}:{line_number}: suppression requires inline justification: {line.strip()}"
                    )
    if violations:
        print("Unjustified test/type/lint/coverage suppressions found:")
        print("\n".join(violations))
        return 1
    print(
        "No unjustified skip/xfail/type-ignore/noqa/coverage-ignore suppressions found."
    )
    return 0


def command_dependency_review(args: argparse.Namespace) -> int:
    current = load_pyproject()
    previous = changed_pyproject_from_base(args.base)
    if previous is None:
        print(
            f"Base pyproject not found at {args.base}; validating current dependencies only."
        )
        previous_deps: set[str] = set()
    else:
        previous_deps = project_dependencies(previous)
    current_deps = project_dependencies(current)
    additions = sorted(current_deps - previous_deps)
    justifications = dependency_justifications(current)
    missing = [dep for dep in additions if not justifications.get(dep)]
    if missing:
        print(
            "New runtime dependencies require [tool.aethermesh.dependency_justifications] entries:"
        )
        print("\n".join(f"- {dep}" for dep in missing))
        return 1
    if additions:
        print("New runtime dependencies are justified:")
        for dep in additions:
            print(f"- {dep}: {justifications[dep]}")
    else:
        print("No new runtime dependencies detected.")
    return 0


def command_dependency_audit(_: argparse.Namespace) -> int:
    deps = sorted(project_dependencies(load_pyproject()))
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", suffix=".txt", delete=False
    ) as handle:
        req_path = Path(handle.name)
        handle.write("\n".join(deps))
        if deps:
            handle.write("\n")
    try:
        result = run(
            [sys.executable, "-m", "pip_audit", "--strict", "-r", str(req_path)]
        )
        print(result.stdout, end="")
        return result.returncode
    finally:
        req_path.unlink(missing_ok=True)


def command_mutation_score(args: argparse.Namespace) -> int:
    result = run(["mutmut", "results", "--all", "true"])
    print(result.stdout, end="")
    text = result.stdout.lower()
    killed = len(re.findall(r"\bkilled\b", text))
    survived = len(re.findall(r"\bsurvived\b", text))
    timeout = len(re.findall(r"\btimeout\b", text))
    suspicious = len(re.findall(r"\bsuspicious\b", text))
    total = killed + survived + timeout + suspicious
    if total == 0:
        print("Could not parse mutmut results; no mutants found in output.")
        return 1
    score = killed / total * 100.0
    print(f"Mutation score: {score:.2f}% ({killed}/{total} killed)")
    if score < args.minimum:
        print(
            f"Mutation score below required threshold: {score:.2f}% < {args.minimum:.2f}%"
        )
        return 1
    return 0


def command_workflow_security(_: argparse.Namespace) -> int:
    workflow_dir = ROOT / ".github" / "workflows"
    violations: list[str] = []
    if not workflow_dir.exists():
        print("No GitHub workflow directory found.")
        return 0
    for path in sorted(workflow_dir.glob("*.y*ml")):
        rel = path.relative_to(ROOT)
        text = path.read_text(encoding="utf-8")
        lowered = text.lower()
        is_pr_workflow = "pull_request:" in lowered
        if "pull_request_target:" in lowered:
            violations.append(
                f"{rel}: pull_request_target is not allowed for PR checks"
            )
        if re.search(r"permissions:\s*write-all\b", lowered):
            violations.append(f"{rel}: permissions: write-all is not allowed")
        for line_number, line in enumerate(text.splitlines(), 1):
            stripped = line.strip().lower()
            if is_pr_workflow and re.match(r"[a-z-]+:\s*write(?:\s|$)", stripped):
                violations.append(
                    f"{rel}:{line_number}: write permission is not allowed in PR workflows: {line.strip()}"
                )
        if re.search(r"curl\b[^\n|]*\|\s*(?:sh|bash)\b", text):
            violations.append(f"{rel}: curl-to-shell install pattern is not allowed")
        if "${{ secrets." in text and is_pr_workflow:
            violations.append(f"{rel}: PR workflows must not expose repository secrets")
        for line_number, line in enumerate(text.splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("uses:") and "@" not in stripped:
                violations.append(
                    f"{rel}:{line_number}: action reference must include a version"
                )
            if stripped.startswith("uses:"):
                action_ref = stripped.removeprefix("uses:").strip().strip("\"'")
                if not action_ref.startswith("actions/"):
                    violations.append(
                        f"{rel}:{line_number}: third-party action requires explicit review/allowlist: {action_ref}"
                    )
    if violations:
        print("Workflow security violations found:")
        print("\n".join(violations))
        return 1
    print("Workflow security check passed.")
    return 0


def command_pr_size(args: argparse.Namespace) -> int:
    result = run(["git", "diff", "--numstat", f"{args.base}...HEAD"])
    if result.returncode != 0:
        print(result.stdout, end="")
        return result.returncode
    excluded_prefixes = tuple(args.exclude_path_prefix or ())
    changed_files = 0
    additions = 0
    deletions = 0
    binary_files = 0
    ignored_files = 0
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 3:
            continue
        added, deleted, path = parts[:3]
        if excluded_prefixes and path.startswith(excluded_prefixes):
            ignored_files += 1
            continue
        changed_files += 1
        if added == "-" or deleted == "-":
            binary_files += 1
            continue
        additions += int(added)
        deletions += int(deleted)
    changed_lines = additions + deletions
    print(
        f"PR size: files={changed_files}, additions={additions}, deletions={deletions}, "
        f"changed_lines={changed_lines}, binary_files={binary_files}, ignored_files={ignored_files}"
    )
    failures = []
    if changed_files > args.max_files:
        failures.append(f"files {changed_files} > {args.max_files}")
    if changed_lines > args.max_lines:
        failures.append(f"changed_lines {changed_lines} > {args.max_lines}")
    if binary_files > args.max_binary_files:
        failures.append(f"binary_files {binary_files} > {args.max_binary_files}")
    if failures:
        print("PR size check failed: " + "; ".join(failures))
        return 1
    return 0


def _urlsafe_b64_digest(data: bytes) -> str:
    return (
        base64.urlsafe_b64encode(hashlib.sha256(data).digest())
        .rstrip(b"=")
        .decode("ascii")
    )


def _verify_wheel_record(wheel: Path) -> list[str]:
    violations: list[str] = []
    with zipfile.ZipFile(wheel) as archive:
        names = archive.namelist()
        record_names = [name for name in names if name.endswith(".dist-info/RECORD")]
        if len(record_names) != 1:
            return [
                f"{wheel.name}: expected exactly one RECORD file, found {len(record_names)}"
            ]
        record = archive.read(record_names[0]).decode("utf-8").splitlines()
        recorded_paths = set()
        for row in record:
            columns = row.split(",")
            if len(columns) < 3:
                violations.append(f"{wheel.name}: malformed RECORD row: {row}")
                continue
            path, digest, size = columns[:3]
            recorded_paths.add(path)
            if path == record_names[0]:
                continue
            if path not in names:
                violations.append(
                    f"{wheel.name}: RECORD references missing file {path}"
                )
                continue
            data = archive.read(path)
            expected_digest = "sha256=" + _urlsafe_b64_digest(data)
            if digest != expected_digest:
                violations.append(f"{wheel.name}: digest mismatch for {path}")
            if size and int(size) != len(data):
                violations.append(f"{wheel.name}: size mismatch for {path}")
        missing_record = sorted(set(names) - recorded_paths)
        if missing_record:
            violations.append(
                f"{wheel.name}: files missing from RECORD: {', '.join(missing_record[:10])}"
            )
    return violations


def command_install_smoke(args: argparse.Namespace) -> int:
    wheels = sorted((ROOT / args.dist).glob("*.whl"))
    if len(wheels) != 1:
        print(f"Install smoke requires exactly one wheel, found {len(wheels)}.")
        return 1

    with tempfile.TemporaryDirectory(prefix="aethermesh-install-") as directory:
        environment = Path(directory)
        venv.EnvBuilder(with_pip=True).create(environment)
        scripts = environment / ("Scripts" if os.name == "nt" else "bin")
        python = scripts / ("python.exe" if os.name == "nt" else "python")
        cli = scripts / (
            "aethermesh-core.exe" if os.name == "nt" else "aethermesh-core"
        )
        install = run(
            [str(python), "-m", "pip", "install", "--force-reinstall", str(wheels[0])]
        )
        print(install.stdout, end="")
        if install.returncode != 0:
            return install.returncode
        smoke = run([str(cli), "--help"])
        print(smoke.stdout, end="")
        return smoke.returncode


def command_flaky_tests(_: argparse.Namespace) -> int:
    for seed in ("1", "2", "3"):
        print(f"Flaky-test pass with PYTHONHASHSEED={seed}")
        env = os.environ.copy()
        env.update(
            {
                "PYTHONHASHSEED": seed,
                "PYTHONDONTWRITEBYTECODE": "1",
                "PYTEST_ADDOPTS": f"-o cache_dir=.pytest_cache/full-test-flaky-{seed}",
            }
        )
        result = run([sys.executable, "-m", "pytest", "-q", "tests"], env=env)
        print(result.stdout, end="")
        if result.returncode != 0:
            return result.returncode
    return 0


def command_artifact_provenance(args: argparse.Namespace) -> int:
    dist = ROOT / args.dist
    if not dist.exists():
        print(f"Artifact directory does not exist: {dist}")
        return 1
    github_sha = os.environ.get("GITHUB_SHA")
    github_workflow = os.environ.get("GITHUB_WORKFLOW")
    github_run_id = os.environ.get("GITHUB_RUN_ID")
    if github_sha:
        head = run(["git", "rev-parse", "HEAD"])
        if head.returncode != 0:
            print(head.stdout, end="")
            return head.returncode
        if head.stdout.strip() != github_sha:
            print(
                f"Artifact provenance check failed: GITHUB_SHA {github_sha} does not match HEAD {head.stdout.strip()}"
            )
            return 1
        print(f"source_commit={github_sha}")
    if github_workflow:
        print(f"workflow={github_workflow}")
    if github_run_id:
        print(f"workflow_run_id={github_run_id}")
    artifacts = sorted(path for path in dist.iterdir() if path.is_file())
    wheels = [path for path in artifacts if path.suffix == ".whl"]
    sdists = [path for path in artifacts if path.name.endswith(".tar.gz")]
    violations: list[str] = []
    if len(wheels) != 1:
        violations.append(f"expected exactly one wheel, found {len(wheels)}")
    if len(sdists) != 1:
        violations.append(f"expected exactly one sdist, found {len(sdists)}")
    for artifact in artifacts:
        digest = hashlib.sha256(artifact.read_bytes()).hexdigest()
        print(f"{artifact.name} sha256={digest}")
    for wheel in wheels:
        violations.extend(_verify_wheel_record(wheel))
    if violations:
        print("Artifact provenance check failed:")
        print("\n".join(violations))
        return 1
    print("Artifact provenance check passed.")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)

    test_integrity = sub.add_parser("test-integrity")
    test_integrity.set_defaults(func=command_test_integrity)

    dep_review = sub.add_parser("dependency-review")
    dep_review.add_argument("--base", default="origin/main")
    dep_review.set_defaults(func=command_dependency_review)

    dep_audit = sub.add_parser("dependency-audit")
    dep_audit.set_defaults(func=command_dependency_audit)

    mutation = sub.add_parser("mutation-score")
    mutation.add_argument("--minimum", type=float, default=95.0)
    mutation.set_defaults(func=command_mutation_score)

    workflow_security = sub.add_parser("workflow-security")
    workflow_security.set_defaults(func=command_workflow_security)

    pr_size = sub.add_parser("pr-size")
    pr_size.add_argument("--base", default="origin/main")
    pr_size.add_argument("--max-files", type=int, default=80)
    pr_size.add_argument("--max-lines", type=int, default=3000)
    pr_size.add_argument("--max-binary-files", type=int, default=0)
    pr_size.add_argument("--exclude-path-prefix", action="append", default=[])
    pr_size.set_defaults(func=command_pr_size)

    install_smoke = sub.add_parser("install-smoke")
    install_smoke.add_argument("--dist", default="dist")
    install_smoke.set_defaults(func=command_install_smoke)

    flaky_tests = sub.add_parser("flaky-tests")
    flaky_tests.set_defaults(func=command_flaky_tests)

    provenance = sub.add_parser("artifact-provenance")
    provenance.add_argument("--dist", default="dist")
    provenance.set_defaults(func=command_artifact_provenance)

    args = parser.parse_args()
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
