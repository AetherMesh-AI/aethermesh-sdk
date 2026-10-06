#!/usr/bin/env python3
"""Small, testable policy helpers for numbered prerelease publication."""

from __future__ import annotations

import argparse
import json
import re
from collections.abc import Mapping
from pathlib import Path

TAG_PATTERN = re.compile(r"^v0\.2\.0-alpha\.(\d+)$")
FIRST_BUILD = 116
REQUIRED_RELEASE_CHECKS = (
    "Python tests with 100% coverage",
    "Duplicate code with 0% threshold",
    "Test",
    "Line Coverage",
    "Branch Coverage",
    "Ruff Check",
    "Ruff Format",
    "Strict Type Check",
    "Duplicate Code",
    "Complexity",
    "Dead Code",
    "Bandit",
    "Pip Audit",
    "Secret Scan",
    "Test Integrity",
    "Dependency Review",
    "Build Install",
    "Python - Workflow Security Check",
    "Python - PR Size Check",
    "Python - Flaky Test Check",
    "Python - Artifact Provenance Check",
)


def numeric_build(tag: str) -> int | None:
    match = TAG_PATTERN.fullmatch(tag.strip())
    return int(match.group(1)) if match else None


def next_build(tags: list[str]) -> int:
    builds = [build for tag in tags if (build := numeric_build(tag)) is not None]
    return max(builds, default=FIRST_BUILD - 1) + 1


def latest_numbered_tag(tags: list[str]) -> str | None:
    numbered = [(build, tag.strip()) for tag in tags if (build := numeric_build(tag))]
    return max(numbered, default=(0, ""))[1] or None


def _object_list(value: object) -> list[object]:
    return value if isinstance(value, list) else []


def required_checks_document() -> dict[str, list[str]]:
    """Return the versioned checks required before release promotion."""

    return {"contexts": list(REQUIRED_RELEASE_CHECKS)}


def verify_required_checks(
    required: Mapping[str, object],
    check_runs_payload: Mapping[str, object],
    statuses_payload: Mapping[str, object],
) -> list[str]:
    contexts = {str(item) for item in _object_list(required.get("contexts"))}
    contexts.update(
        str(item["context"])
        for item in _object_list(required.get("checks"))
        if isinstance(item, dict) and "context" in item
    )
    check_runs: dict[str, str] = {}
    for run in _object_list(check_runs_payload.get("check_runs")):
        if isinstance(run, dict) and run.get("name") in contexts:
            check_runs.setdefault(str(run.get("name")), str(run.get("conclusion")))
    # The API returns newest statuses first, so retain the first status per context.
    statuses: dict[str, str] = {}
    for status in _object_list(statuses_payload.get("statuses")):
        if isinstance(status, dict):
            statuses.setdefault(str(status.get("context")), str(status.get("state")))

    failures: list[str] = []
    for context in sorted(contexts):
        conclusion = check_runs.get(context)
        state = statuses.get(context)
        if conclusion != "success" and state != "success":
            failures.append(
                f"{context}: check={conclusion or 'missing'}, status={state or 'missing'}"
            )
    return failures


def command_next_build(_: argparse.Namespace) -> int:
    import sys

    print(next_build(sys.stdin.read().splitlines()))
    return 0


def command_latest_tag(_: argparse.Namespace) -> int:
    import sys

    print(latest_numbered_tag(sys.stdin.read().splitlines()) or "")
    return 0


def command_required_checks(_: argparse.Namespace) -> int:
    print(json.dumps(required_checks_document(), sort_keys=True))
    return 0


def command_verify_checks(args: argparse.Namespace) -> int:
    required = json.loads(Path(args.required).read_text(encoding="utf-8"))
    check_runs = json.loads(Path(args.check_runs).read_text(encoding="utf-8"))
    statuses = json.loads(Path(args.statuses).read_text(encoding="utf-8"))
    failures = verify_required_checks(required, check_runs, statuses)
    if failures:
        raise SystemExit(
            "required checks are not successful for the selected SHA:\n"
            + "\n".join(failures)
        )
    print("all required checks are successful for the selected SHA")
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("next-build").set_defaults(func=command_next_build)
    subparsers.add_parser("latest-tag").set_defaults(func=command_latest_tag)
    subparsers.add_parser("required-checks").set_defaults(func=command_required_checks)
    verify = subparsers.add_parser("verify-checks")
    verify.add_argument("--required", required=True)
    verify.add_argument("--check-runs", required=True)
    verify.add_argument("--statuses", required=True)
    verify.set_defaults(func=command_verify_checks)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
