#!/usr/bin/env python3
"""Build deterministic metadata for numbered alpha prereleases."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PACKAGE_SOURCE_PATHS = ("README.md", "pyproject.toml", "src")
ALPHA_RELEASE_TAG_PATTERN = "v0.2.0-alpha.[0-9]*"


@dataclass(frozen=True)
class Commit:
    sha: str
    subject: str
    author: str


@dataclass(frozen=True)
class ReleaseMetadata:
    tag: str
    name: str
    notes: str
    source_sha256: str
    head_sha: str
    previous_tag: str | None


def run_git(args: list[str], *, root: Path = ROOT) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=root,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit(result.stdout)
    return result.stdout.strip()


def package_source_files(root: Path = ROOT) -> list[Path]:
    output = run_git(["ls-files", "--", *PACKAGE_SOURCE_PATHS], root=root)
    paths = [root / line for line in output.splitlines() if line.strip()]
    return sorted(path for path in paths if path.is_file())


def source_files_sha256(root: Path, paths: list[Path]) -> str:
    import hashlib

    digest = hashlib.sha256()
    for path in sorted(paths, key=lambda item: item.relative_to(root).as_posix()):
        relative_path = path.relative_to(root).as_posix()
        digest.update(relative_path.encode("utf-8"))
        digest.update(b"\0")
        digest.update(path.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


def source_archive_sha256(source_archive: Path) -> str:
    import hashlib

    return hashlib.sha256(source_archive.read_bytes()).hexdigest()


def head_sha(root: Path = ROOT) -> str:
    return run_git(["rev-parse", "HEAD"], root=root)


def previous_alpha_release_tag(
    root: Path = ROOT, *, exclude_tag: str | None = None
) -> str | None:
    output = run_git(
        [
            "tag",
            "--merged",
            "HEAD",
            "--sort=-creatordate",
            "--list",
            ALPHA_RELEASE_TAG_PATTERN,
        ],
        root=root,
    )
    for line in output.splitlines():
        tag = line.strip()
        if tag and tag != exclude_tag:
            return tag
    return None


def commits_since(previous_tag: str | None, *, root: Path = ROOT) -> list[Commit]:
    revision = f"{previous_tag}..HEAD" if previous_tag else "HEAD"
    output = run_git(
        ["log", "--reverse", "--pretty=format:%H%x00%s%x00%an", revision], root=root
    )
    commits: list[Commit] = []
    for line in output.splitlines():
        if not line.strip():
            continue
        sha, subject, author = line.split("\x00", 2)
        commits.append(Commit(sha=sha, subject=subject, author=author))
    return commits


def format_release_notes(
    *,
    release_version: str,
    source_sha256: str,
    head_sha: str,
    previous_tag: str | None,
    commits: list[Commit],
) -> str:
    lines = [
        f"# {release_version}",
        "",
        f"Source files SHA-256: `{source_sha256}`",
        f"Built commit: `{head_sha}`",
    ]
    if previous_tag:
        lines.append(f"Previous release tag: `{previous_tag}`")
        lines.append(f"Commit range: `{previous_tag}..{head_sha}`")
    else:
        lines.append("Previous release tag: none")
        lines.append("Commit range: full history")
    lines.extend(["", "## Commits"])
    if commits:
        for commit in commits:
            lines.append(f"- `{commit.sha[:7]}` {commit.subject} ({commit.author})")
    else:
        lines.append("- No commits since the previous release tag.")
    lines.append("")
    return "\n".join(lines)


def build_release_metadata(
    *,
    release_version: str,
    source_sha256: str,
    head_sha: str,
    previous_tag: str | None,
    commits: list[Commit],
    build_number: int,
) -> ReleaseMetadata:
    version = f"{release_version}.{build_number}"
    return ReleaseMetadata(
        tag=f"v{version}",
        name=version,
        notes=format_release_notes(
            release_version=version,
            source_sha256=source_sha256,
            head_sha=head_sha,
            previous_tag=previous_tag,
            commits=commits,
        ),
        source_sha256=source_sha256,
        head_sha=head_sha,
        previous_tag=previous_tag,
    )


def prepare_release(
    *,
    release_version: str,
    notes_path: Path,
    build_number: int,
    previous_tag: str | None,
    root: Path = ROOT,
    source_archive: Path | None = None,
) -> ReleaseMetadata:
    source_sha256 = (
        source_archive_sha256(source_archive)
        if source_archive is not None
        else source_files_sha256(root, package_source_files(root))
    )
    current_head = head_sha(root)
    metadata = build_release_metadata(
        release_version=release_version,
        source_sha256=source_sha256,
        head_sha=current_head,
        previous_tag=previous_tag,
        commits=commits_since(previous_tag, root=root),
        build_number=build_number,
    )
    notes_path.write_text(metadata.notes, encoding="utf-8")
    return metadata


def write_github_outputs(metadata: ReleaseMetadata) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        return
    with Path(output_path).open("a", encoding="utf-8") as handle:
        handle.write(f"release_tag={metadata.tag}\n")
        handle.write(f"release_name={metadata.name}\n")
        handle.write(f"source_sha256={metadata.source_sha256}\n")
        handle.write(f"head_sha={metadata.head_sha}\n")
        handle.write(f"previous_tag={metadata.previous_tag or ''}\n")


def command_prepare(args: argparse.Namespace) -> int:
    metadata = prepare_release(
        release_version=args.release_version,
        notes_path=Path(args.notes_path),
        source_archive=Path(args.source_archive) if args.source_archive else None,
        build_number=args.build_number,
        previous_tag=args.previous_tag,
        root=Path(args.root).resolve(),
    )
    write_github_outputs(metadata)
    print(
        json.dumps(
            {
                "release_tag": metadata.tag,
                "release_name": metadata.name,
                "source_sha256": metadata.source_sha256,
                "head_sha": metadata.head_sha,
                "previous_tag": metadata.previous_tag,
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--release-version", default="0.2.0-alpha")
    prepare.add_argument("--build-number", type=int, required=True)
    prepare.add_argument("--previous-tag")
    prepare.add_argument("--notes-path", default="release-notes.md")
    prepare.add_argument("--source-archive")
    prepare.add_argument("--root", default=str(ROOT))
    prepare.set_defaults(func=command_prepare)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
