#!/usr/bin/env python3
"""Run mutmut and fail as soon as the mutation score target is impossible."""

from __future__ import annotations

import argparse
import math
import re
import subprocess
import time
from collections.abc import Sequence

ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")
MUTMUT_STATUS_RE = re.compile(r"(?P<done>\d+)\s*/\s*(?P<total>\d+)(?=\s+🎉)")
COUNT_RE = {
    "killed": re.compile(r"(?:🎉|killed)\s*(?P<count>\d+)", re.IGNORECASE),
    "timeout": re.compile(r"(?:⏰|timeout)\s*(?P<count>\d+)", re.IGNORECASE),
    "suspicious": re.compile(r"(?:🤔|suspicious)\s*(?P<count>\d+)", re.IGNORECASE),
    "survived": re.compile(r"(?:🙁|survived)\s*(?P<count>\d+)", re.IGNORECASE),
}


def parse_mutmut_counts(
    text: str,
) -> tuple[int | None, int | None, dict[str, int]]:
    """Return latest mutmut progress counters from text."""

    clean = ANSI_RE.sub("", text)
    status_matches = list(MUTMUT_STATUS_RE.finditer(clean))
    done: int | None = None
    total: int | None = None
    if status_matches:
        latest = status_matches[-1]
        done = int(latest.group("done"))
        total = int(latest.group("total"))

    counts: dict[str, int] = {}
    for name, pattern in COUNT_RE.items():
        matches = list(pattern.finditer(clean))
        counts[name] = int(matches[-1].group("count")) if matches else 0
    return done, total, counts


def parse_mutmut_progress(text: str) -> tuple[int | None, int | None, int]:
    """Return (done, total, non_killed) from the latest mutmut progress text."""

    done, total, counts = parse_mutmut_counts(text)
    non_killed = counts["survived"] + counts["timeout"] + counts["suspicious"]
    return done, total, non_killed


def max_non_killed_for_score(total: int, minimum: float) -> int:
    """Maximum non-killed mutants allowed while still reaching minimum percent."""

    required_killed = math.ceil(total * minimum / 100.0)
    return total - required_killed


def format_duration(seconds: float | None) -> str:
    """Format a duration compactly for progress logs."""

    if seconds is None or seconds < 0:
        return "unknown"
    rounded = int(seconds + 0.5)
    hours, remainder = divmod(rounded, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h{minutes:02d}m{secs:02d}s"
    if minutes:
        return f"{minutes}m{secs:02d}s"
    return f"{secs}s"


def format_progress_line(
    *,
    done: int,
    total: int,
    counts: dict[str, int],
    elapsed_seconds: float,
) -> str:
    """Build a stable mutmut progress line with speed and ETA."""

    rate = done / elapsed_seconds if done > 0 and elapsed_seconds > 0 else 0.0
    remaining = total - done
    eta = remaining / rate if rate > 0 else None
    width = 24
    filled = min(width, max(0, round(width * done / total))) if total else 0
    bar = "█" * filled + "░" * (width - filled)
    return (
        f"Mutants [{bar}] {done}/{total} "
        f"🎉 {counts['killed']} "
        f"⏰ {counts['timeout']} "
        f"🤔 {counts['suspicious']} "
        f"🙁 {counts['survived']} "
        f"| Mut/s {rate:.2f} "
        f"| Est. {format_duration(eta)}"
    )


def run_mutmut_with_early_failure(minimum: float, mutmut_args: Sequence[str]) -> int:
    command = ["mutmut", "run", *mutmut_args]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    if process.stdout is None:
        raise RuntimeError("mutmut stdout pipe was not created")

    tail = ""
    start = time.monotonic()
    last_progress_at = 0.0
    last_progress_done: int | None = None
    while True:
        chunk = process.stdout.read(1)
        if chunk:
            print(chunk, end="", flush=True)
            tail = (tail + chunk)[-20_000:]
            done, total, counts = parse_mutmut_counts(tail)
            non_killed = counts["survived"] + counts["timeout"] + counts["suspicious"]
            if total is not None:
                now = time.monotonic()
                if (
                    done is not None
                    and done != last_progress_done
                    and now - last_progress_at >= 5.0
                ):
                    print(
                        "\n"
                        + format_progress_line(
                            done=done,
                            total=total,
                            counts=counts,
                            elapsed_seconds=now - start,
                        ),
                        flush=True,
                    )
                    last_progress_at = now
                    last_progress_done = done
                allowed = max_non_killed_for_score(total, minimum)
                if non_killed > allowed:
                    print(
                        "\nMutation score target is already impossible: "
                        f"{non_killed} non-killed mutants exceeds allowed {allowed} "
                        f"for {minimum:.2f}% of {total} mutants. Stopping mutmut early."
                    )
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait()
                    return 1
            continue
        if process.poll() is not None:
            remaining = process.stdout.read()
            if remaining:
                print(remaining, end="", flush=True)
            return int(process.returncode)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum", type=float, default=95.0)
    parser.add_argument("--max-children", default="4")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return run_mutmut_with_early_failure(
        args.minimum, ("--max-children", str(args.max_children))
    )


if __name__ == "__main__":
    raise SystemExit(main())
