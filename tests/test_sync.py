import json
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class ContractSyncTests(unittest.TestCase):
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
