import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from pairamid_rabbitmq_client.partner.storage import (
    InputConflictError,
    InputStateStore,
    archive_rejected_message,
    materialize_input,
)

EXAMPLES = Path(__file__).parent / "examples"


def execution_message() -> dict[str, object]:
    return json.loads((EXAMPLES / "simulation-execution-message.json").read_text())


class PartnerStorageTests(unittest.TestCase):
    def test_materialize_writes_only_input_and_is_idempotent(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            input_dir = Path(directory) / "input"

            input_path, created = materialize_input(message, input_dir, "irtjv")
            duplicate_path, duplicate_created = materialize_input(message, input_dir, "irtjv")

            self.assertTrue(created)
            self.assertFalse(duplicate_created)
            self.assertEqual(input_path, duplicate_path)
            self.assertEqual(json.loads(input_path.read_text()), message["input"])
            self.assertEqual(list(input_dir.glob("input-*.json")), [input_path])
            self.assertEqual(list((input_dir / ".tmp").iterdir()), [])

    def test_materialize_rejects_different_input_for_same_execution(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            input_dir = Path(directory) / "input"
            materialize_input(message, input_dir, "irtjv")
            message["input"] = {"conflicting": True}

            with self.assertRaisesRegex(InputConflictError, "Conflicting input"):
                materialize_input(message, input_dir, "irtjv")

    def test_input_state_keeps_context_and_event_id_across_restart(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            root = Path(directory)
            input_path, _created = materialize_input(
                message, root / "input", "irtjv"
            )
            first_store = InputStateStore(root / "state", "irtjv")
            first_state = first_store.load_or_create(message, input_path)
            first_event_id = first_state["input_downloaded_event"]["event_id"]
            first_store.mark_published(message, first_state)

            restarted_store = InputStateStore(root / "state", "irtjv")
            restarted_state = restarted_store.load_or_create(message, input_path)

            self.assertTrue(restarted_state["input_downloaded_published"])
            self.assertEqual(
                restarted_state["input_downloaded_event"]["event_id"],
                first_event_id,
            )
            self.assertEqual(restarted_state["execution_message"], message)

    def test_rejected_archive_preserves_original_body_and_reason(self) -> None:
        body = b"not valid json"
        with TemporaryDirectory() as directory:
            body_path = archive_rejected_message(
                body,
                Path(directory),
                "invalid JSON",
                message_id="message-123",
            )

            metadata_path = Path(directory) / f"{body_path.stem}.json"
            metadata = json.loads(metadata_path.read_text())

            self.assertEqual(body_path.read_bytes(), body)
            self.assertEqual(metadata["reason"], "invalid JSON")
            self.assertEqual(metadata["message_id"], "message-123")


if __name__ == "__main__":
    unittest.main()
