"""Private bounded SQLite files shared by trusted local service utilities.

Never use these helpers to open peer-supplied databases. Parent directories must
be trusted and stable; permissions are not a sandbox against hostile local users.
"""

from __future__ import annotations

import os
import sqlite3
import stat
from itertools import islice
from pathlib import Path


def _private(path: Path, *, directory: bool) -> None:
    info = path.lstat()
    kind = stat.S_ISDIR(info.st_mode) if directory else stat.S_ISREG(info.st_mode)
    if (
        not kind
        or info.st_uid != os.getuid()
        or info.st_mode & 0o077
        or (not directory and (info.st_nlink != 1 or info.st_size > 4194304))
    ):
        raise ValueError("local storage must be private, owned, and physical")


def _physical(directory: Path) -> None:
    if directory != directory.resolve():
        raise ValueError("storage directory must be an absolute physical path")


def _configure(db: sqlite3.Connection) -> None:
    db.execute("PRAGMA trusted_schema=OFF")
    db.execute("PRAGMA journal_mode=DELETE")
    db.execute("PRAGMA synchronous=EXTRA")
    if db.execute("PRAGMA page_size").fetchone()[0] != 4096:
        raise ValueError("unsupported local storage page size")
    db.execute("PRAGMA max_page_count=1024")


def open_private_database(directory: Path, filename: str) -> sqlite3.Connection:
    """Open an existing operator-owned database; return a thread-confined handle."""
    _physical(directory)
    _private(directory, directory=True)
    if any(
        path.name not in {filename, filename + "-journal"}
        for path in islice(directory.iterdir(), 3)
    ):
        raise ValueError("unexpected local storage entries")
    database = directory / filename
    _private(database, directory=False)
    journal = directory / (filename + "-journal")
    try:
        _private(journal, directory=False)
    except FileNotFoundError:
        # DELETE-mode commits may remove the optional journal during inspection.
        pass
    db = sqlite3.connect(database.as_uri() + "?mode=rw", uri=True, timeout=5)
    try:
        _configure(db)
        return db
    except BaseException:
        db.close()
        raise


def create_private_database(directory: Path, filename: str) -> sqlite3.Connection:
    """Exclusive creation; failures may leave an incomplete private directory."""
    _physical(directory)
    directory.mkdir(mode=0o700)
    descriptor = os.open(
        directory / filename, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600
    )
    os.close(descriptor)
    db = sqlite3.connect(directory / filename)
    try:
        db.execute("PRAGMA page_size=4096")
        _configure(db)
        return db
    except BaseException:
        db.close()
        raise


def sync_directory_creation(directory: Path) -> None:
    for path in (directory, directory.parent):
        descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
