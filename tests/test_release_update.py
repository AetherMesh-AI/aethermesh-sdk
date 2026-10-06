import hashlib
import unittest
from pathlib import Path
from typing import Self
from unittest import mock

from aethermesh_core import release_update


class ReleaseUpdateTests(unittest.TestCase):
    def test_build_update_plan_selects_wheel_and_checksum(self) -> None:
        wheel_bytes = b"wheel bytes"
        checksum = hashlib.sha256(wheel_bytes).hexdigest()
        release = {
            "tag_name": "v0.2.0-alpha-abc123",
            "name": "0.2.0-alpha - (...bc123)",
            "html_url": "https://github.example/releases/tag/v0.2.0-alpha-abc123",
            "assets": [
                {
                    "name": "aethermesh-0.2.0a0.tar.gz",
                    "browser_download_url": "https://github.example/aethermesh.tar.gz",
                },
                {
                    "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "browser_download_url": "https://github.example/aethermesh.whl",
                },
                {
                    "name": "SHA256SUMS.txt",
                    "browser_download_url": "https://github.example/SHA256SUMS.txt",
                },
            ],
        }

        plan = release_update.build_update_plan(
            release, f"{checksum}  aethermesh-0.2.0a0-py3-none-any.whl\n"
        )

        self.assertEqual(plan.release_tag, "v0.2.0-alpha-abc123")
        self.assertEqual(plan.release_name, "0.2.0-alpha - (...bc123)")
        self.assertEqual(plan.wheel_name, "aethermesh-0.2.0a0-py3-none-any.whl")
        self.assertEqual(plan.wheel_url, "https://github.example/aethermesh.whl")
        self.assertEqual(plan.expected_sha256, checksum)

    def test_build_update_plan_rejects_release_without_wheel(self) -> None:
        with self.assertRaisesRegex(release_update.ReleaseUpdateError, "wheel asset"):
            release_update.build_update_plan(
                {"tag_name": "v0.1.1-alpha", "assets": []}, checksum_text=""
            )

    def test_update_from_latest_release_downloads_verifies_and_installs_wheel(
        self,
    ) -> None:
        wheel_bytes = b"fake wheel"
        checksum = hashlib.sha256(wheel_bytes).hexdigest()
        calls: list[tuple[str, object]] = []
        release_json = {
            "tag_name": "v0.2.0-alpha-abc123",
            "name": "0.2.0-alpha - (...bc123)",
            "html_url": "https://github.example/releases/tag/v0.2.0-alpha-abc123",
            "assets": [
                {
                    "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "browser_download_url": "https://github.example/aethermesh.whl",
                },
                {
                    "name": "SHA256SUMS.txt",
                    "browser_download_url": "https://github.example/SHA256SUMS.txt",
                },
            ],
        }

        def fetch_json(url: str) -> dict[str, object]:
            calls.append(("json", url))
            return release_json

        def fetch_bytes(url: str) -> bytes:
            calls.append(("bytes", url))
            if url.endswith("SHA256SUMS.txt"):
                return f"{checksum}  aethermesh-0.2.0a0-py3-none-any.whl\n".encode()
            return wheel_bytes

        def install_wheel(path: Path) -> None:
            calls.append(("install", path.name))
            self.assertEqual(path.read_bytes(), wheel_bytes)

        result = release_update.update_from_latest_release(
            fetch_json=fetch_json,
            fetch_bytes=fetch_bytes,
            install_wheel=install_wheel,
        )

        self.assertEqual(result.release_tag, "v0.2.0-alpha-abc123")
        self.assertEqual(result.wheel_name, "aethermesh-0.2.0a0-py3-none-any.whl")
        self.assertEqual(result.installed, True)
        self.assertEqual(result.sha256, checksum)
        self.assertEqual(
            calls,
            [
                ("json", release_update.DEFAULT_LATEST_RELEASE_URL),
                ("bytes", "https://github.example/SHA256SUMS.txt"),
                ("bytes", "https://github.example/aethermesh.whl"),
                ("install", "aethermesh-0.2.0a0-py3-none-any.whl"),
            ],
        )

    def test_update_from_latest_release_rejects_checksum_mismatch(self) -> None:
        release_json = {
            "tag_name": "v0.2.0-alpha-abc123",
            "assets": [
                {
                    "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "browser_download_url": "https://github.example/aethermesh.whl",
                },
                {
                    "name": "SHA256SUMS.txt",
                    "browser_download_url": "https://github.example/SHA256SUMS.txt",
                },
            ],
        }

        def fetch_bytes(url: str) -> bytes:
            if url.endswith("SHA256SUMS.txt"):
                return f"{'0' * 64}  aethermesh-0.2.0a0-py3-none-any.whl\n".encode()
            return b"different wheel"

        with self.assertRaisesRegex(release_update.ReleaseUpdateError, "checksum"):
            release_update.update_from_latest_release(
                fetch_json=lambda url: release_json,
                fetch_bytes=fetch_bytes,
                install_wheel=lambda path: None,
            )

    def test_update_from_latest_release_dry_run_does_not_install(self) -> None:
        wheel_bytes = b"fake wheel"
        checksum = hashlib.sha256(wheel_bytes).hexdigest()
        installed: list[str] = []
        release_json = {
            "tag_name": "v0.2.0-alpha-abc123",
            "assets": [
                {
                    "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "browser_download_url": "https://github.example/aethermesh.whl",
                },
                {
                    "name": "SHA256SUMS.txt",
                    "browser_download_url": "https://github.example/SHA256SUMS.txt",
                },
            ],
        }

        def fetch_bytes(url: str) -> bytes:
            if url.endswith("SHA256SUMS.txt"):
                return f"{checksum}  aethermesh-0.2.0a0-py3-none-any.whl\n".encode()
            return wheel_bytes

        result = release_update.update_from_latest_release(
            dry_run=True,
            fetch_json=lambda url: release_json,
            fetch_bytes=fetch_bytes,
            install_wheel=lambda path: installed.append(path.name),
        )

        self.assertEqual(result.installed, False)
        self.assertEqual(result.sha256, checksum)
        self.assertEqual(installed, [])

    def test_update_result_to_dict_returns_json_ready_payload(self) -> None:
        result = release_update.ReleaseUpdateResult(
            release_tag="v0.2.0-alpha-abc123",
            release_name="0.2.0-alpha - (...bc123)",
            release_url=None,
            wheel_name="aethermesh.whl",
            wheel_url="https://github.example/aethermesh.whl",
            sha256="abc123",
            expected_sha256=None,
            installed=True,
        )

        self.assertEqual(
            result.to_dict(),
            {
                "release_tag": "v0.2.0-alpha-abc123",
                "release_name": "0.2.0-alpha - (...bc123)",
                "release_url": None,
                "wheel_name": "aethermesh.whl",
                "wheel_url": "https://github.example/aethermesh.whl",
                "sha256": "abc123",
                "expected_sha256": None,
                "installed": True,
            },
        )

    def test_update_plan_tolerates_missing_checksum_and_optional_release_fields(
        self,
    ) -> None:
        plan = release_update.build_update_plan(
            {
                "assets": [
                    "ignored",
                    {"name": "ignored.txt"},
                    {
                        "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                        "browser_download_url": "https://github.example/aethermesh.whl",
                    },
                ]
            }
        )

        self.assertEqual(plan.release_tag, "unknown-release")
        self.assertEqual(plan.release_name, "unknown release")
        self.assertEqual(plan.release_url, None)
        self.assertEqual(plan.checksum_url, None)
        self.assertEqual(plan.expected_sha256, None)

    def test_release_assets_require_assets_list(self) -> None:
        with self.assertRaisesRegex(release_update.ReleaseUpdateError, "assets list"):
            release_update.build_update_plan({"assets": "not a list"})

    def test_update_from_latest_release_uses_default_helpers_when_not_overridden(
        self,
    ) -> None:
        wheel_bytes = b"fake wheel"
        release_json = {
            "tag_name": "v0.2.0-alpha-abc123",
            "assets": [
                {
                    "name": "aethermesh-0.2.0a0-py3-none-any.whl",
                    "browser_download_url": "https://github.example/aethermesh.whl",
                }
            ],
        }
        installed: list[str] = []
        with (
            mock.patch.object(
                release_update, "_fetch_json", return_value=release_json
            ) as fetch_json,
            mock.patch.object(
                release_update, "_fetch_bytes", return_value=wheel_bytes
            ) as fetch_bytes,
            mock.patch.object(
                release_update,
                "_install_wheel",
                side_effect=lambda path: installed.append(path.name),
            ),
        ):
            result = release_update.update_from_latest_release()

        fetch_json.assert_called_once_with(release_update.DEFAULT_LATEST_RELEASE_URL)
        fetch_bytes.assert_called_once_with("https://github.example/aethermesh.whl")
        self.assertEqual(installed, ["aethermesh-0.2.0a0-py3-none-any.whl"])
        self.assertEqual(result.installed, True)

    def test_fetch_json_rejects_invalid_and_non_object_json(self) -> None:
        with mock.patch.object(
            release_update, "_fetch_bytes", return_value=b'{"tag_name":"v1"}'
        ):
            self.assertEqual(
                release_update._fetch_json("https://github.example/release"),
                {"tag_name": "v1"},
            )

        with (
            mock.patch.object(release_update, "_fetch_bytes", return_value=b"not json"),
            self.assertRaisesRegex(release_update.ReleaseUpdateError, "valid JSON"),
        ):
            release_update._fetch_json("https://github.example/release")

        with (
            mock.patch.object(release_update, "_fetch_bytes", return_value=b"[]"),
            self.assertRaisesRegex(release_update.ReleaseUpdateError, "JSON object"),
        ):
            release_update._fetch_json("https://github.example/release")

    def test_fetch_bytes_wraps_download_errors(self) -> None:
        class FakeResponse:
            def __enter__(self) -> Self:
                return self

            def __exit__(self, *_args: object) -> None:
                return None

            def read(self) -> bytes:
                return b"payload"

        self.assertEqual(
            release_update._checksum_for_asset("not-enough-parts", "aethermesh.whl"),
            None,
        )
        with mock.patch.object(release_update, "urlopen", return_value=FakeResponse()):
            self.assertEqual(
                release_update._fetch_bytes("https://github.example/release"),
                b"payload",
            )

        with (
            mock.patch.object(
                release_update, "urlopen", side_effect=OSError("offline")
            ),
            self.assertRaisesRegex(
                release_update.ReleaseUpdateError, "failed to download"
            ),
        ):
            release_update._fetch_bytes("https://github.example/release")

    def test_install_wheel_wraps_pip_failures(self) -> None:
        with mock.patch.object(release_update.subprocess, "check_call") as check_call:
            release_update._install_wheel(Path("aethermesh.whl"))
        check_call.assert_called_once()

        with (
            mock.patch.object(
                release_update.subprocess,
                "check_call",
                side_effect=release_update.subprocess.CalledProcessError(1, ["pip"]),
            ),
            self.assertRaisesRegex(
                release_update.ReleaseUpdateError, "failed to install"
            ),
        ):
            release_update._install_wheel(Path("aethermesh.whl"))


if __name__ == "__main__":
    unittest.main()
