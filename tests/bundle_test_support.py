"""Owned disposable Git fixtures only; no downloaded object ingestion."""

from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path


def fixture_git(repository: Path, *args: str, data: bytes = b"") -> bytes:
    git = shutil.which("git", path=os.defpath)
    if git is None:
        raise RuntimeError("Git is required for bundle fixture tests")
    env = {
        "PATH": os.defpath,
        "GIT_CONFIG_NOSYSTEM": "1",
        "GIT_CONFIG_GLOBAL": os.devnull,
        "GIT_CONFIG_SYSTEM": os.devnull,
        "GIT_AUTHOR_NAME": "Owned Fixture",
        "GIT_AUTHOR_EMAIL": "fixture@example.invalid",
        "GIT_COMMITTER_NAME": "Owned Fixture",
        "GIT_COMMITTER_EMAIL": "fixture@example.invalid",
        "GIT_TERMINAL_PROMPT": "0",
    }
    return subprocess.run(
        [git, f"--git-dir={repository}", "-c", f"core.hooksPath={os.devnull}", *args],
        input=data,
        env=env,
        capture_output=True,
        check=True,
        timeout=10,
    ).stdout.strip()


def create_repository(repository: Path) -> tuple[str, str]:
    repository.mkdir(parents=True)
    (repository / "objects").mkdir()
    (repository / "refs").mkdir()
    (repository / "HEAD").write_text("ref: refs/heads/main\n", encoding="ascii")
    (repository / "config").write_text(
        "[core]\nrepositoryformatversion=0\nbare=true\n", encoding="ascii"
    )
    blob = fixture_git(
        repository, "hash-object", "-w", "--stdin", data=b"owned fixture bytes\n"
    ).decode()
    attributes = fixture_git(
        repository, "hash-object", "-w", "--stdin", data=b"* filter=trap\n"
    ).decode()
    tree = fixture_git(
        repository,
        "mktree",
        data=f"100644 blob {attributes}\t.gitattributes\n100644 blob {blob}\tfile.txt\n".encode(),
    ).decode()
    commit = fixture_git(
        repository, "commit-tree", tree, data=b"owned fixture\n"
    ).decode()
    ref = "refs/heads/main"
    fixture_git(repository, "update-ref", ref, commit)
    return ref, commit
