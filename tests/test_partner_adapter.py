import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pika

from pairamid_rabbitmq_client import Delivery
from pairamid_rabbitmq_client.exceptions import PublishConnectionError
from pairamid_rabbitmq_client.partner.adapter import PartnerAdapter
from pairamid_rabbitmq_client.partner.config import PartnerSettings

EXAMPLES = Path(__file__).parent / "examples"


def make_settings(simulation_dir: str) -> PartnerSettings:
    return PartnerSettings.from_env(
        {
            "RABBITMQ_CLIENT_ID": "irtjv-rabbitmq-client",
            "RABBITMQ_CLIENT_SECRET": "test-secret",
            "OAUTH2_TOKEN_URL": "https://keycloak.example.test/token",
            "PARTNER_ID": "irtjv",
            "SIMULATION_DIR": simulation_dir,
        }
    )


def make_delivery(body: bytes, routing_key: str = "irtjv.lri") -> tuple[Delivery, MagicMock]:
    channel = MagicMock()
    delivery = Delivery(
        channel=channel,
        delivery_tag=1,
        body=body,
        properties=pika.BasicProperties(),
        headers=None,
        redelivered=False,
        routing_key=routing_key,
    )
    return delivery, channel


def execution_message() -> dict[str, object]:
    return json.loads((EXAMPLES / "simulation-execution-message.json").read_text())


class PartnerAdapterTests(unittest.TestCase):
    def test_invalid_json_is_rejected_without_requeue(self) -> None:
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            delivery, channel = make_delivery(b"not-json")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)
            archived_body = next(settings.rejected_input_dir.glob("*.body"))
            self.assertEqual(archived_body.read_bytes(), b"not-json")

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_not_called()

    def test_contract_violation_is_rejected_without_requeue(self) -> None:
        message = execution_message()
        message["schema_version"] = "invalid"
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode())

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)

    def test_wrong_simulation_or_routing_key_is_rejected(self) -> None:
        message = execution_message()
        message["simulation_key"] = "another"
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode(), "irtjv.another")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)

    def test_valid_message_is_materialized_published_then_acknowledged(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode())
            operations: list[str] = []

            def publish_event(event_type: str, event: dict[str, object], message_id: str) -> None:
                operations.append("publish")
                self.assertEqual(event_type, "simulation_execution.input_downloaded")
                self.assertEqual(message_id, event["event_id"])
                input_path = (
                    settings.input_dir
                    / f"input-irtjv-lri-{message['simulation_execution_id']}.json"
                )
                self.assertEqual(json.loads(input_path.read_text()), message["input"])

            adapter.client.publish_event = MagicMock(side_effect=publish_event)
            channel.basic_ack.side_effect = lambda **_kwargs: operations.append("ack")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="INFO"):
                adapter.handle_execution(delivery)

            input_path = settings.input_dir / "input-irtjv-lri-1.json"
            state_path = settings.state_dir / "input-irtjv-lri-1.json"
            self.assertEqual(operations, ["publish", "ack"])
            self.assertEqual(json.loads(input_path.read_text()), message["input"])
            self.assertEqual(
                json.loads(state_path.read_text())["execution_message"],
                message,
            )

        adapter.client.publish_event.assert_called_once()
        channel.basic_ack.assert_called_once_with(delivery_tag=1)
        channel.basic_nack.assert_not_called()
        channel.basic_reject.assert_not_called()

    def test_duplicate_delivery_reuses_input_and_skips_confirmed_event(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            adapter.client.publish_event = MagicMock()

            for delivery_tag in (1, 2):
                delivery, _channel = make_delivery(json.dumps(message).encode())
                delivery.delivery_tag = delivery_tag
                adapter.handle_execution(delivery)

            input_files = list(settings.input_dir.glob("input-*.json"))
            self.assertEqual(len(input_files), 1)
            self.assertEqual(len(list(settings.state_dir.glob("input-*.json"))), 1)

        adapter.client.publish_event.assert_called_once()

    def test_publish_failure_requeues_without_ack_and_retry_reuses_event_id(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            adapter = PartnerAdapter(make_settings(directory))
            adapter.client.publish_event = MagicMock(
                side_effect=[PublishConnectionError("broker unavailable"), None]
            )
            first_delivery, first_channel = make_delivery(json.dumps(message).encode())

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="ERROR"):
                adapter.handle_execution(first_delivery)

            first_channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)
            first_channel.basic_ack.assert_not_called()
            first_event = adapter.client.publish_event.call_args_list[0].args[1]

            retry_delivery, retry_channel = make_delivery(json.dumps(message).encode())
            retry_delivery.delivery_tag = 2
            adapter.handle_execution(retry_delivery)
            second_event = adapter.client.publish_event.call_args_list[1].args[1]

        self.assertEqual(first_event["event_id"], second_event["event_id"])
        retry_channel.basic_ack.assert_called_once_with(delivery_tag=2)

    def test_ack_failure_redelivery_does_not_create_another_input_or_event(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            adapter.client.publish_event = MagicMock()
            first_delivery, first_channel = make_delivery(json.dumps(message).encode())
            first_channel.basic_ack.side_effect = RuntimeError("connection lost during ack")

            with self.assertRaisesRegex(RuntimeError, "connection lost"):
                adapter.handle_execution(first_delivery)

            self.assertTrue(first_delivery.responded)
            retry_delivery, retry_channel = make_delivery(json.dumps(message).encode())
            retry_delivery.delivery_tag = 2
            adapter.handle_execution(retry_delivery)

            self.assertEqual(len(list(settings.input_dir.glob("input-*.json"))), 1)
            self.assertEqual(len(list(settings.state_dir.glob("input-*.json"))), 1)

        adapter.client.publish_event.assert_called_once()
        retry_channel.basic_ack.assert_called_once_with(delivery_tag=2)

    def test_conflicting_input_is_archived_and_rejected(self) -> None:
        message = execution_message()
        conflicting_message = dict(message)
        conflicting_message["input"] = {"different": True}
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = PartnerAdapter(settings)
            adapter.client.publish_event = MagicMock()
            original_delivery, _ = make_delivery(json.dumps(message).encode())
            adapter.handle_execution(original_delivery)

            conflict_delivery, channel = make_delivery(
                json.dumps(conflicting_message).encode()
            )
            conflict_delivery.delivery_tag = 2
            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(conflict_delivery)

            archived_body = next(settings.rejected_input_dir.glob("*.body"))
            self.assertEqual(archived_body.read_bytes(), conflict_delivery.body)

        channel.basic_reject.assert_called_once_with(delivery_tag=2, requeue=False)
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_not_called()
        adapter.client.publish_event.assert_called_once()

    def test_disk_error_requeues_delivery(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            adapter = PartnerAdapter(make_settings(directory))
            delivery, channel = make_delivery(json.dumps(message).encode())

            with (
                patch(
                    "pairamid_rabbitmq_client.partner.adapter.materialize_input",
                    side_effect=OSError("disk full"),
                ),
                self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="ERROR"),
            ):
                adapter.handle_execution(delivery)

        channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)
        channel.basic_ack.assert_not_called()
        channel.basic_reject.assert_not_called()

    def test_rejection_archive_error_requeues_without_rejecting(self) -> None:
        with TemporaryDirectory() as directory:
            adapter = PartnerAdapter(make_settings(directory))
            delivery, channel = make_delivery(b"not-json")

            with (
                patch(
                    "pairamid_rabbitmq_client.partner.adapter.archive_rejected_message",
                    side_effect=OSError("disk full"),
                ),
                self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="ERROR"),
            ):
                adapter.handle_execution(delivery)

        channel.basic_nack.assert_called_once_with(delivery_tag=1, requeue=True)
        channel.basic_reject.assert_not_called()
        channel.basic_ack.assert_not_called()


if __name__ == "__main__":
    unittest.main()
