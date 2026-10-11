"""Real competing-process fencing and SQLite lock recovery with isolated files."""

from __future__ import annotations

import asyncio
import json
import sqlite3
import sys
import tempfile
import unittest
from contextlib import closing
from pathlib import Path

from aethermesh_core.learning_state import LearningBinding, LearningJournal

BINDING = LearningBinding("race-test", "1" * 64, "2" * 64, "3" * 64, "4" * 64)
CHILD = """
import json,sys,time,sqlite3
from pathlib import Path
from aethermesh_core.learning_state import LearningBinding,LearningJournal,LearningConflict
root=Path(sys.argv[1]); name=sys.argv[2]; action=sys.argv[3]
binding=LearningBinding("race-test","1"*64,"2"*64,"3"*64,"4"*64)
store=LearningJournal(root/"job",binding)
if action=="lock": store._db.execute("BEGIN IMMEDIATE")
(root/(name+".ready")).touch()
deadline=time.monotonic()+10
while not (root/"go").exists():
    if time.monotonic()>deadline: raise TimeoutError("test barrier")
    time.sleep(0.01)
try:
    if action=="claim": result=store.claim(expected_revision=0)
    elif action=="commit":
        result=store.commit(term=1,expected_revision=1,state=name.encode(),checkpoint=name.encode(),
                            evidence=(("a" if name=="one" else "b")*64,),validate=lambda *args:None)
    else:
        store._db.rollback()
        result=store.snapshot()
    print(json.dumps({"status":"ok","revision":result.revision,"term":result.term}))
except LearningConflict:
    print(json.dumps({"status":"conflict"}))
finally: store.close()
"""


class LearningConcurrencyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.store = LearningJournal.create(
            self.root / "job", BINDING, state=b"initial"
        )
        self.addCleanup(self.store.close)
        self.children: list[asyncio.subprocess.Process] = []
        self.addAsyncCleanup(self.cleanup_children)

    async def cleanup_children(self) -> None:
        for child in self.children:
            if child.returncode is None:
                child.kill()
            await child.wait()

    async def start(self, name: str, action: str) -> asyncio.subprocess.Process:
        child = await asyncio.create_subprocess_exec(
            sys.executable,
            "-c",
            CHILD,
            str(self.root),
            name,
            action,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        self.children.append(child)
        return child

    async def ready(self, *names: str) -> None:
        async with asyncio.timeout(5):
            while not all((self.root / (name + ".ready")).exists() for name in names):
                await asyncio.sleep(0.01)

    async def result(self, child: asyncio.subprocess.Process) -> dict[str, object]:
        async with asyncio.timeout(5):
            out, err = await child.communicate()
        self.assertEqual(child.returncode, 0, err.decode())
        return json.loads(out)

    async def test_simultaneous_claims_have_one_winner(self) -> None:
        children = [await self.start(name, "claim") for name in ("one", "two")]
        await self.ready("one", "two")
        (self.root / "go").touch()
        results = await asyncio.gather(*(self.result(child) for child in children))
        self.assertCountEqual(
            [result["status"] for result in results], ["ok", "conflict"]
        )
        self.assertEqual(
            (self.store.snapshot().revision, self.store.snapshot().term), (1, 1)
        )

    async def test_simultaneous_commits_never_mix_state_checkpoint_or_tombstone(
        self,
    ) -> None:
        self.store.claim(expected_revision=0)
        children = [await self.start(name, "commit") for name in ("one", "two")]
        await self.ready("one", "two")
        (self.root / "go").touch()
        results = await asyncio.gather(*(self.result(child) for child in children))
        self.assertCountEqual(
            [result["status"] for result in results], ["ok", "conflict"]
        )
        snapshot = self.store.snapshot()
        self.assertEqual(snapshot.revision, 2)
        self.assertEqual(snapshot.state, snapshot.checkpoint)
        winner = "a" if snapshot.state == b"one" else "b"
        with closing(sqlite3.connect(self.root / "job" / "state.sqlite3")) as db, db:
            self.assertEqual(
                db.execute("SELECT digest FROM evidence").fetchall(), [(winner * 64,)]
            )

    async def test_real_lock_timeout_then_successful_recovery(self) -> None:
        self.assertEqual(
            self.store._db.execute("PRAGMA busy_timeout").fetchone()[0], 5000
        )
        # Exercise the real SQLite timeout with a short test-only wait; the public
        # store's production default remains the verified five seconds above.
        self.store._db.execute("PRAGMA busy_timeout=50")
        holder = await self.start("holder", "lock")
        await self.ready("holder")
        before = self.store.snapshot()
        with self.assertRaises(sqlite3.OperationalError):
            self.store.claim(expected_revision=0)
        self.assertEqual(self.store.snapshot(), before)
        (self.root / "go").touch()
        self.assertEqual((await self.result(holder))["status"], "ok")
        claimed = self.store.claim(expected_revision=0)
        self.assertEqual((claimed.revision, claimed.term), (1, 1))


if __name__ == "__main__":
    unittest.main()
