from __future__ import annotations

import contextlib
import importlib.util
import io
import os
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import ModuleType
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
FULL_TEST_PATH = ROOT / "scripts" / "full_test.py"


def load_full_test_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "aethermesh_full_test", FULL_TEST_PATH
    )
    if spec is None or spec.loader is None:
        raise RuntimeError("could not load scripts/full_test.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class FullTestRunnerTests(unittest.TestCase):
    def test_gate_inventory_and_commands_match_serial_baseline(self) -> None:
        module = load_full_test_module()
        python = sys.executable
        expected_fast = [
            ("pytest", (python, "-m", "pytest", "-q", "tests")),
            (
                "branch coverage",
                (
                    python,
                    "-m",
                    "pytest",
                    "-q",
                    "--cov=aethermesh_core",
                    "--cov-branch",
                    "--cov-report=term-missing",
                    "--cov-fail-under=100",
                    "tests",
                ),
            ),
            ("ruff check", ("ruff", "check", "src", "tests", "scripts")),
            (
                "ruff format",
                ("ruff", "format", "--check", "src", "tests", "scripts"),
            ),
            ("mypy", ("mypy", "--strict", "src", "scripts")),
            (
                "test integrity",
                (python, "scripts/ci_quality_gates.py", "test-integrity"),
            ),
            (
                "dependency review",
                (
                    python,
                    "scripts/ci_quality_gates.py",
                    "dependency-review",
                    "--base",
                    "chosen/base",
                ),
            ),
            (
                "workflow security",
                (python, "scripts/ci_quality_gates.py", "workflow-security"),
            ),
            (
                "PR size",
                (
                    python,
                    "scripts/ci_quality_gates.py",
                    "pr-size",
                    "--base",
                    "chosen/base",
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
        expected_full_extra = [
            (
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
            ),
            ("complexity", ("radon", "cc", "src", "tests", "scripts", "-s", "-n", "E")),
            (
                "dead code",
                ("vulture", "src", "tests", "scripts", "--min-confidence", "100"),
            ),
            ("bandit", ("bandit", "-q", "-r", "src")),
            (
                "pip audit",
                (python, "scripts/ci_quality_gates.py", "dependency-audit"),
            ),
            ("build", (python, "-m", "build")),
            (
                "install smoke",
                (
                    python,
                    "scripts/ci_quality_gates.py",
                    "install-smoke",
                    "--dist",
                    "dist",
                ),
            ),
            (
                "artifact provenance",
                (
                    python,
                    "scripts/ci_quality_gates.py",
                    "artifact-provenance",
                    "--dist",
                    "dist",
                ),
            ),
            (
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
            (
                "flaky tests",
                (python, "scripts/ci_quality_gates.py", "flaky-tests"),
            ),
        ]
        expected_mutation = [
            (
                "mutation",
                (python, "scripts/mutmut_early_fail.py", "--max-children", "4"),
            ),
            (
                "mutation score",
                (
                    python,
                    "scripts/ci_quality_gates.py",
                    "mutation-score",
                    "--minimum",
                    "95",
                ),
            ),
        ]

        fast = module._checks_for_mode("fast", "chosen/base", include_mutation=False)
        full = module._checks_for_mode("full", "chosen/base", include_mutation=False)
        full_with_mutation = module._checks_for_mode(
            "full", "chosen/base", include_mutation=True
        )

        self.assertEqual([(check.name, check.command) for check in fast], expected_fast)
        self.assertEqual(
            [(check.name, check.command) for check in full],
            expected_fast + expected_full_extra,
        )
        self.assertEqual(
            [(check.name, check.command) for check in full_with_mutation],
            expected_fast + expected_full_extra + expected_mutation,
        )
        self.assertNotIn("mutation", {check.name for check in full})
        self.assertNotIn("mutation score", {check.name for check in full})

    def test_dag_declares_required_dependencies_and_resource_conflicts(self) -> None:
        module = load_full_test_module()
        checks = {
            check.name: check
            for check in module._checks_for_mode("full", "base", include_mutation=True)
        }

        self.assertEqual(checks["mutation"].dependencies, ("pytest", "branch coverage"))
        self.assertEqual(checks["mutation score"].dependencies, ("mutation",))
        self.assertFalse(
            checks["pytest"].resources & checks["branch coverage"].resources
        )
        self.assertIsNotNone(checks["pytest"].env)
        self.assertIsNotNone(checks["branch coverage"].env)
        assert checks["pytest"].env is not None
        assert checks["branch coverage"].env is not None
        self.assertNotEqual(
            checks["pytest"].env["PYTEST_ADDOPTS"],
            checks["branch coverage"].env["PYTEST_ADDOPTS"],
        )
        self.assertIn("COVERAGE_FILE", checks["branch coverage"].env)
        self.assertNotIn("desktop tests", checks)
        self.assertTrue(
            checks["ruff check"].resources & checks["ruff format"].resources
        )
        self.assertIn("build-dist", checks["build"].resources)
        self.assertEqual(checks["install smoke"].dependencies, ("build",))
        self.assertEqual(checks["artifact provenance"].dependencies, ("build",))
        self.assertTrue(checks["build"].resources & checks["install smoke"].resources)

    def test_missing_required_tools_are_reported_before_running(self) -> None:
        module = load_full_test_module()
        checks = [
            module.Check("missing", ("definitely-not-a-real-aethermesh-tool", "--x"))
        ]

        with mock.patch.object(module.shutil, "which", return_value=None):
            exit_code = module.run_checks(checks, keep_going=True, jobs=2)

        self.assertEqual(exit_code, 127)

    def test_scheduler_bounds_concurrency_and_respects_dependencies(self) -> None:
        module = load_full_test_module()
        lock = threading.Lock()
        active = 0
        maximum = 0
        finished: set[str] = set()
        dependency_seen = False
        checks = [
            module.Check("first", (sys.executable,), resources=frozenset({"a"})),
            module.Check("second", (sys.executable,), resources=frozenset({"b"})),
            module.Check("third", (sys.executable,), dependencies=("first",)),
        ]

        def runner(check: object, _: threading.Event) -> object:
            nonlocal active, maximum, dependency_seen
            name = check.name
            with lock:
                if name == "third":
                    dependency_seen = "first" in finished
                active += 1
                maximum = max(maximum, active)
            time.sleep(0.03)
            with lock:
                active -= 1
                finished.add(name)
            return module.CheckResult(check, 0, f"output {name}\n")

        exit_code = module.run_checks(checks, keep_going=True, jobs=2, runner=runner)

        self.assertEqual(exit_code, 0)
        self.assertEqual(maximum, 2)
        self.assertTrue(dependency_seen)

    def test_conflicting_resources_never_overlap(self) -> None:
        module = load_full_test_module()
        lock = threading.Lock()
        active_resources: set[str] = set()
        overlap = False
        checks = [
            module.Check("one", (sys.executable,), resources=frozenset({"shared"})),
            module.Check("two", (sys.executable,), resources=frozenset({"shared"})),
            module.Check("free", (sys.executable,), resources=frozenset({"other"})),
        ]

        def runner(check: object, _: threading.Event) -> object:
            nonlocal overlap
            with lock:
                overlap = overlap or bool(active_resources & check.resources)
                active_resources.update(check.resources)
            time.sleep(0.03)
            with lock:
                active_resources.difference_update(check.resources)
            return module.CheckResult(check, 0, "")

        self.assertEqual(
            module.run_checks(checks, keep_going=True, jobs=3, runner=runner), 0
        )
        self.assertFalse(overlap)

    def test_failure_cancels_pending_work_without_keep_going(self) -> None:
        module = load_full_test_module()
        started: list[str] = []
        checks = [
            module.Check("fail", (sys.executable,), resources=frozenset({"serial"})),
            module.Check("pending", (sys.executable,), resources=frozenset({"serial"})),
        ]

        def runner(check: object, _: threading.Event) -> object:
            started.append(check.name)
            return module.CheckResult(check, 7 if check.name == "fail" else 0, "")

        self.assertEqual(
            module.run_checks(checks, keep_going=False, jobs=2, runner=runner), 1
        )
        self.assertEqual(started, ["fail"])

    def test_runner_exception_cancels_concurrent_subprocess_and_is_reported(
        self,
    ) -> None:
        module = load_full_test_module()
        with tempfile.TemporaryDirectory(
            prefix="full-test-runner-exception-"
        ) as directory:
            marker = Path(directory) / "sleeper-started"
            checks = [
                module.Check("explode", (sys.executable,)),
                module.Check(
                    "sleeper",
                    (
                        sys.executable,
                        "-c",
                        f"from pathlib import Path; import time; Path({str(marker)!r}).touch(); time.sleep(30)",
                    ),
                ),
            ]

            def runner(check: object, cancel_event: threading.Event) -> object:
                if getattr(check, "name", None) == "sleeper":
                    return module._run_check(check, cancel_event)
                deadline = time.monotonic() + 5
                while not marker.exists() and time.monotonic() < deadline:
                    time.sleep(0.01)
                raise RuntimeError("deterministic runner boom")

            stdout = io.StringIO()
            started = time.monotonic()
            with contextlib.redirect_stdout(stdout):
                exit_code = module.run_checks(
                    checks, keep_going=False, jobs=2, runner=runner
                )

        self.assertEqual(exit_code, 1)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn(
            "runner raised RuntimeError: deterministic runner boom", stdout.getvalue()
        )
        self.assertIn("CANCELLED: sleeper", stdout.getvalue())

    def test_spawn_registration_is_atomic_with_cancellation(self) -> None:
        module = load_full_test_module()
        registry = module._ProcessRegistry()
        cancel_event = threading.Event()
        popen_entered = threading.Event()
        release_popen = threading.Event()
        terminated = threading.Event()

        class FakeProcess:
            pid = 12345

            def poll(self) -> None:
                return None

        process = FakeProcess()

        def delayed_popen(*args: object, **kwargs: object) -> object:
            del args, kwargs
            popen_entered.set()
            self.assertTrue(release_popen.wait(timeout=5))
            return process

        spawned: list[object] = []
        with (
            mock.patch.object(module.subprocess, "Popen", side_effect=delayed_popen),
            mock.patch.object(
                module,
                "_terminate_process_group",
                side_effect=lambda child: terminated.set(),
            ),
        ):
            spawn_thread = threading.Thread(
                target=lambda: spawned.append(
                    registry.spawn(cancel_event, (sys.executable,))
                )
            )
            spawn_thread.start()
            self.assertTrue(popen_entered.wait(timeout=5))
            cancel_event.set()
            terminate_thread = threading.Thread(target=registry.terminate_all)
            terminate_thread.start()
            time.sleep(0.03)
            self.assertFalse(terminated.is_set())
            release_popen.set()
            spawn_thread.join(timeout=5)
            terminate_thread.join(timeout=5)

        self.assertEqual(spawned, [process])
        self.assertTrue(terminated.is_set())

    def test_keep_going_runs_all_checks_and_collects_failures(self) -> None:
        module = load_full_test_module()
        started: list[str] = []
        checks = [
            module.Check("fail", (sys.executable,)),
            module.Check("pass", (sys.executable,)),
        ]

        def runner(check: object, _: threading.Event) -> object:
            started.append(check.name)
            return module.CheckResult(check, 5 if check.name == "fail" else 0, "")

        self.assertEqual(
            module.run_checks(checks, keep_going=True, jobs=2, runner=runner), 1
        )
        self.assertCountEqual(started, ["fail", "pass"])

    def test_failed_prerequisite_skips_dependents_in_stable_order(self) -> None:
        module = load_full_test_module()
        started: list[str] = []
        checks = [
            module.Check("prerequisite", (sys.executable,)),
            module.Check(
                "dependent", (sys.executable,), dependencies=("prerequisite",)
            ),
            module.Check("transitive", (sys.executable,), dependencies=("dependent",)),
            module.Check("independent", (sys.executable,)),
        ]

        def runner(check: object, _: threading.Event) -> object:
            started.append(check.name)
            return module.CheckResult(
                check, 7 if check.name == "prerequisite" else 0, ""
            )

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = module.run_checks(
                checks, keep_going=True, jobs=2, runner=runner
            )

        self.assertEqual(exit_code, 1)
        self.assertCountEqual(started, ["prerequisite", "independent"])
        output = stdout.getvalue()
        self.assertLess(output.index("==> dependent"), output.index("==> transitive"))
        self.assertIn(
            "SKIPPED: dependent (unsuccessful prerequisites: prerequisite)", output
        )
        self.assertIn(
            "SKIPPED: transitive (unsuccessful prerequisites: dependent)", output
        )

    def test_fail_fast_terminates_active_subprocess_groups(self) -> None:
        module = load_full_test_module()
        checks = [
            module.Check(
                "fail",
                (
                    sys.executable,
                    "-c",
                    "import time; time.sleep(0.1); raise SystemExit(9)",
                ),
            ),
            module.Check(
                "sleeper",
                (sys.executable, "-c", "import time; time.sleep(30)"),
            ),
        ]
        stdout = io.StringIO()
        started = time.monotonic()

        with contextlib.redirect_stdout(stdout):
            exit_code = module.run_checks(checks, keep_going=False, jobs=2)

        self.assertEqual(exit_code, 1)
        self.assertLess(time.monotonic() - started, 5)
        self.assertIn("CANCELLED: sleeper", stdout.getvalue())

    @unittest.skipUnless(  # justification: os.killpg is POSIX-only.
        os.name == "posix", "POSIX process-group regression"
    )
    def test_posix_termination_kills_owned_group_once_and_reaps(self) -> None:
        module = load_full_test_module()
        process = mock.Mock(pid=43210)
        process.poll.return_value = None

        with (
            mock.patch.object(module.os, "killpg") as killpg,
            mock.patch.object(module.subprocess, "run") as run,
        ):
            module._terminate_process_group(process)

        killpg.assert_called_once_with(43210, signal.SIGKILL)
        run.assert_not_called()
        process.wait.assert_called_once_with()

    @unittest.skipUnless(  # justification: os.killpg is POSIX-only.
        os.name == "posix", "POSIX process-group regression"
    )
    def test_posix_termination_does_nothing_after_child_was_reaped(self) -> None:
        module = load_full_test_module()
        process = mock.Mock(pid=43210)
        process.poll.return_value = 0

        with mock.patch.object(module.os, "killpg") as killpg:
            module._terminate_process_group(process)

        killpg.assert_not_called()
        process.wait.assert_not_called()

    def test_windows_termination_uses_taskkill_for_entire_tree_and_waits(self) -> None:
        module = load_full_test_module()
        process = mock.Mock(pid=43210)
        process.poll.return_value = None
        process.wait.return_value = 0
        with (
            mock.patch.object(module.os, "name", "nt"),
            mock.patch.object(module.subprocess, "run") as run,
        ):
            module._terminate_process_group(process)

        run.assert_called_once_with(
            ["taskkill", "/PID", "43210", "/T", "/F"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        process.wait.assert_called_once_with()

    def test_reporting_is_buffered_in_configured_order(self) -> None:
        module = load_full_test_module()
        checks = [
            module.Check("slow", (sys.executable,)),
            module.Check("fast", (sys.executable,)),
        ]

        def runner(check: object, _: threading.Event) -> object:
            if check.name == "slow":
                time.sleep(0.03)
            return module.CheckResult(check, 0, f"log-{check.name}\n")

        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            exit_code = module.run_checks(
                checks, keep_going=True, jobs=2, runner=runner
            )

        output = stdout.getvalue()
        self.assertEqual(exit_code, 0)
        self.assertLess(output.index("==> slow"), output.index("==> fast"))
        self.assertLess(output.index("log-slow"), output.index("log-fast"))

    def test_jobs_one_is_exact_serial_fallback(self) -> None:
        module = load_full_test_module()
        checks = [
            module.Check("first", (sys.executable,)),
            module.Check("second", (sys.executable,)),
            module.Check("third", (sys.executable,)),
        ]
        order: list[str] = []
        active = 0
        maximum = 0

        def runner(check: object, _: threading.Event) -> object:
            nonlocal active, maximum
            order.append(check.name)
            active += 1
            maximum = max(maximum, active)
            time.sleep(0.01)
            active -= 1
            return module.CheckResult(check, 0, "")

        self.assertEqual(
            module.run_checks(checks, keep_going=True, jobs=1, runner=runner), 0
        )
        self.assertEqual(order, ["first", "second", "third"])
        self.assertEqual(maximum, 1)

    def test_cli_has_safe_jobs_default_and_rejects_zero(self) -> None:
        module = load_full_test_module()
        parser = module.build_parser()

        self.assertGreaterEqual(parser.parse_args([]).jobs, 1)
        with self.assertRaises(SystemExit):
            parser.parse_args(["--jobs", "0"])

    def test_cli_requires_full_mode_for_explicit_mutation_opt_in(self) -> None:
        module = load_full_test_module()

        with self.assertRaises(SystemExit):
            module.main(["--mode", "fast", "--include-mutation", "--list"])

        args = module.build_parser().parse_args(
            ["--mode", "full", "--include-mutation"]
        )
        self.assertTrue(args.include_mutation)

    def test_cli_help_marks_mutation_early_phase_optional_and_nonblocking(self) -> None:
        help_text = load_full_test_module().build_parser().format_help().lower()

        self.assertIn("explicit opt-in", help_text)
        self.assertIn("early-phase", help_text)
        self.assertIn("nonblocking", help_text)

    def test_list_mode_prints_commands_without_running(self) -> None:
        module = load_full_test_module()

        with mock.patch.object(module, "run_checks") as run_checks:
            exit_code = module.main(
                ["--mode", "fast", "--base", "origin/main", "--list"]
            )

        self.assertEqual(exit_code, 0)
        run_checks.assert_not_called()


if __name__ == "__main__":
    unittest.main()
