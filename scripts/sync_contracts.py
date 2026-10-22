"""Copy or check official v2 messaging schemas and their example fixtures."""

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMAS = (
    "simulation-execution-message",
    "simulation-execution-input-downloaded-event",
    "simulation-execution-result-event",
    "simulation-execution-result-payload",
    "simulation-execution-error-payload",
)
EXAMPLES = (
    "simulation-execution-message",
    "simulation-execution-input-downloaded-event",
    "simulation-execution-result-succeeded",
    "simulation-execution-result-failed",
    "simulation-execution-result-payload",
    "simulation-execution-error-payload",
)
TARGET = ROOT / "src/pairamid_rabbitmq_client/contracts"


def normalized_bytes(path: Path) -> bytes:
    """Ignore only Git's CRLF/LF conversion, not changes to JSON content."""
    return path.read_bytes().replace(b"\r\n", b"\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, help="iTool docs/specs directory")
    parser.add_argument("--check", action="store_true", help="Compare without modifying files")
    args = parser.parse_args()
    if args.source is None and not args.check:
        parser.error("--source is required to sync contracts")
    manifest = {}
    for folder, names, suffix, target in (
        ("contracts/v2", SCHEMAS, ".schema.json", TARGET),
        ("examples/v2", EXAMPLES, ".json", ROOT / "tests/examples"),
    ):
        for name in names:
            destination = target / (name + suffix)
            if args.source is not None:
                source = args.source / folder / destination.name
                if args.check:
                    if not destination.exists() or normalized_bytes(source) != normalized_bytes(
                        destination
                    ):
                        raise SystemExit(f"Contract drift: {folder}/{destination.name}")
                else:
                    target.mkdir(parents=True, exist_ok=True)
                    destination.write_bytes(normalized_bytes(source))
            manifest[f"{folder}/{destination.name}"] = hashlib.sha256(
                normalized_bytes(destination)
            ).hexdigest()
    manifest_file = TARGET / "manifest.json"
    if args.check:
        if json.loads(manifest_file.read_text(encoding="utf-8")) != manifest:
            raise SystemExit("Contract manifest differs; sync from the authoritative iTool specs")
        print("Contracts and examples are synchronized")
    else:
        manifest_file.write_bytes((json.dumps(manifest, indent=2) + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
