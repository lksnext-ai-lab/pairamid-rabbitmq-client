import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ContractSyncTests(unittest.TestCase):
    def test_manifest_and_source_checks_survive_checkout_line_ending_conversion(self):
        for source_newline in (b"\n", b"\r\n"):
            with (
                self.subTest(source_newline=source_newline),
                tempfile.TemporaryDirectory() as directory,
            ):
                root = Path(directory)
                script = root / "scripts/sync_contracts.py"
                script.parent.mkdir()
                shutil.copyfile(ROOT / "scripts/sync_contracts.py", script)
                source = root / "authoritative"
                for folder, original in (
                    ("contracts", ROOT / "src/pairamid_rabbitmq_client/contracts"),
                    ("examples", ROOT / "tests/examples"),
                ):
                    (source / folder).mkdir(parents=True)
                    for file in original.glob("*.json"):
                        if file.name != "manifest.json":
                            content = file.read_bytes().replace(b"\r\n", b"\n")
                            (source / folder / file.name).write_bytes(
                                content.replace(b"\n", source_newline)
                            )
                result = subprocess.run(
                    [sys.executable, str(script), "--source", str(source)],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                for checkout_newline in (b"\n", b"\r\n"):
                    for folder in (
                        root / "src/pairamid_rabbitmq_client/contracts",
                        root / "tests/examples",
                    ):
                        for file in folder.glob("*.json"):
                            content = file.read_bytes().replace(b"\r\n", b"\n")
                            file.write_bytes(content.replace(b"\n", checkout_newline))
                    for source_args in ([], ["--source", str(source)]):
                        result = subprocess.run(
                            [sys.executable, str(script), "--check", *source_args],
                            capture_output=True,
                            text=True,
                            check=False,
                        )
                        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                schema = root / (
                    "src/pairamid_rabbitmq_client/contracts/"
                    "simulation-execution-message.schema.json"
                )
                payload = json.loads(schema.read_text())
                payload["required"].append("new_required_field")
                schema.write_text(json.dumps(payload))
                result = subprocess.run(
                    [sys.executable, str(script), "--check"],
                    capture_output=True,
                    text=True,
                    check=False,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Contract manifest differs", result.stderr)

    def test_manifest_matches_bundled_schemas_and_examples(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts/sync_contracts.py"), "--check"],
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)

    def test_changed_authoritative_schema_is_detected(self):
        with tempfile.TemporaryDirectory() as directory:
            source = Path(directory)
            shutil.copytree(ROOT / "src/pairamid_rabbitmq_client/contracts", source / "contracts")
            shutil.copytree(ROOT / "tests/examples", source / "examples")
            schema = source / "contracts/simulation-execution-message.schema.json"
            payload = json.loads(schema.read_text())
            payload["required"].append("new_required_field")
            schema.write_text(json.dumps(payload))
            result = subprocess.run(
                [
                    sys.executable,
                    str(ROOT / "scripts/sync_contracts.py"),
                    "--source",
                    str(source),
                    "--check",
                ],
                capture_output=True,
                text=True,
                check=False,
            )
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("Contract drift", result.stderr)


if __name__ == "__main__":
    unittest.main()
