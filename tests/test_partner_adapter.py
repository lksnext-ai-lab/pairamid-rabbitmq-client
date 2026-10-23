import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import MagicMock, patch

import pika

from pairamid_rabbitmq_client import ContractValidationError, Delivery
from pairamid_rabbitmq_client.exceptions import PublishConnectionError
from pairamid_rabbitmq_client.partner.adapter import PartnerAdapter
from pairamid_rabbitmq_client.partner.config import PartnerSettings
from pairamid_rabbitmq_client.partner.storage import (
    InputStateStore,
    materialize_input,
)

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
        source_queue="pairamid.input.irtjv.lri",
    )
    return delivery, channel


def make_adapter(settings: PartnerSettings) -> PartnerAdapter:
    adapter = PartnerAdapter(settings)
    adapter._current_assignments = {
        "pairamid.input.irtjv.lri": ("irtjv", "lri"),
    }
    return adapter


def execution_message() -> dict[str, object]:
    return json.loads((EXAMPLES / "simulation-execution-message.json").read_text())


def prepare_downloaded_input(settings: PartnerSettings, message: dict[str, object]) -> None:
    input_path, _created = materialize_input(
        message,
        settings.input_dir_for("lri"),
        "irtjv",
    )
    store = InputStateStore(settings.state_dir_for("lri"), "irtjv")
    state = store.load_or_create(message, input_path)
    store.mark_published(message, state)


class PartnerAdapterTests(unittest.TestCase):
    def test_invalid_json_is_rejected_without_requeue(self) -> None:
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            delivery, channel = make_delivery(b"not-json")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)
            archived_body = next(settings.rejected_input_dir_for("lri").glob("*.body"))
            self.assertEqual(archived_body.read_bytes(), b"not-json")

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_not_called()

    def test_contract_violation_is_rejected_without_requeue(self) -> None:
        message = execution_message()
        message["schema_version"] = "invalid"
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode())

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)

    def test_wrong_simulation_or_routing_key_is_rejected(self) -> None:
        message = execution_message()
        message["simulation_key"] = "another"
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode(), "irtjv.another")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)

    def test_valid_message_is_materialized_published_then_acknowledged(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            delivery, channel = make_delivery(json.dumps(message).encode())
            operations: list[str] = []

            def publish_event(
                event_type: str,
                event: dict[str, object],
                message_id: str,
                *,
                partner_id: str,
            ) -> None:
                operations.append("publish")
                self.assertEqual(partner_id, "irtjv")
                self.assertEqual(event_type, "simulation_execution.input_downloaded")
                self.assertEqual(message_id, event["event_id"])
                input_path = (
                    settings.input_dir_for("lri")
                    / f"input-irtjv-lri-{message['simulation_execution_id']}.json"
                )
                self.assertEqual(json.loads(input_path.read_text()), message["input"])

            adapter.client.publish_event = MagicMock(side_effect=publish_event)
            channel.basic_ack.side_effect = lambda **_kwargs: operations.append("ack")

            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="INFO"):
                adapter.handle_execution(delivery)

            input_path = settings.input_dir_for("lri") / "input-irtjv-lri-1.json"
            state_path = settings.state_dir_for("lri") / "input-irtjv-lri-1.json"
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

    def test_adapter_materializes_non_lri_simulation_from_assigned_queue(self) -> None:
        message = execution_message()
        message["simulation_key"] = "xray"
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            queue = "pairamid.input.irtjv.xray"
            adapter._current_assignments[queue] = ("irtjv", "xray")
            delivery, channel = make_delivery(
                json.dumps(message).encode(),
                routing_key="irtjv.xray",
            )
            delivery.source_queue = queue
            adapter.client.publish_event = MagicMock()

            adapter.handle_execution(delivery)

            input_path = settings.input_dir_for("xray") / "input-irtjv-xray-1.json"
            state_path = settings.state_dir_for("xray") / "input-irtjv-xray-1.json"
            self.assertEqual(json.loads(input_path.read_text()), message["input"])
            self.assertEqual(
                json.loads(state_path.read_text())["execution_message"],
                message,
            )

        channel.basic_ack.assert_called_once_with(delivery_tag=1)
        adapter.client.publish_event.assert_called_once()
        self.assertEqual(
            adapter.client.publish_event.call_args.kwargs["partner_id"],
            "irtjv",
        )

    def test_duplicate_delivery_reuses_input_and_skips_confirmed_event(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            adapter.client.publish_event = MagicMock()

            for delivery_tag in (1, 2):
                delivery, _channel = make_delivery(json.dumps(message).encode())
                delivery.delivery_tag = delivery_tag
                adapter.handle_execution(delivery)

            input_files = list(settings.input_dir_for("lri").glob("input-*.json"))
            self.assertEqual(len(input_files), 1)
            self.assertEqual(
                len(list(settings.state_dir_for("lri").glob("input-*.json"))),
                1,
            )

        adapter.client.publish_event.assert_called_once()

    def test_publish_failure_requeues_without_ack_and_retry_reuses_event_id(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            adapter = make_adapter(make_settings(directory))
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
            adapter = make_adapter(settings)
            adapter.client.publish_event = MagicMock()
            first_delivery, first_channel = make_delivery(json.dumps(message).encode())
            first_channel.basic_ack.side_effect = RuntimeError("connection lost during ack")

            with self.assertRaisesRegex(RuntimeError, "connection lost"):
                adapter.handle_execution(first_delivery)

            self.assertTrue(first_delivery.responded)
            retry_delivery, retry_channel = make_delivery(json.dumps(message).encode())
            retry_delivery.delivery_tag = 2
            adapter.handle_execution(retry_delivery)

            self.assertEqual(
                len(list(settings.input_dir_for("lri").glob("input-*.json"))),
                1,
            )
            self.assertEqual(
                len(list(settings.state_dir_for("lri").glob("input-*.json"))),
                1,
            )

        adapter.client.publish_event.assert_called_once()
        retry_channel.basic_ack.assert_called_once_with(delivery_tag=2)

    def test_conflicting_input_is_archived_and_rejected(self) -> None:
        message = execution_message()
        conflicting_message = dict(message)
        conflicting_message["input"] = {"different": True}
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            adapter = make_adapter(settings)
            adapter.client.publish_event = MagicMock()
            original_delivery, _ = make_delivery(json.dumps(message).encode())
            adapter.handle_execution(original_delivery)

            conflict_delivery, channel = make_delivery(
                json.dumps(conflicting_message).encode()
            )
            conflict_delivery.delivery_tag = 2
            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(conflict_delivery)

            archived_body = next(settings.rejected_input_dir_for("lri").glob("*.body"))
            self.assertEqual(archived_body.read_bytes(), conflict_delivery.body)

        channel.basic_reject.assert_called_once_with(delivery_tag=2, requeue=False)
        channel.basic_ack.assert_not_called()
        channel.basic_nack.assert_not_called()
        adapter.client.publish_event.assert_called_once()

    def test_disk_error_requeues_delivery(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            adapter = make_adapter(make_settings(directory))
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
            adapter = make_adapter(make_settings(directory))
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

    def test_queue_assignment_is_loaded_from_token_for_each_cycle(self) -> None:
        with TemporaryDirectory() as directory:
            adapter = PartnerAdapter(make_settings(directory))
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": [
                        "pairamid.input.irtjv.lri",
                        "pairamid.input.irtjv.xray",
                    ],
                }
            )

            queues = adapter._queues_for_current_token()

        self.assertEqual(
            queues,
            ("pairamid.input.irtjv.lri", "pairamid.input.irtjv.xray"),
        )
        self.assertEqual(
            adapter._current_assignments["pairamid.input.irtjv.xray"],
            ("irtjv", "xray"),
        )
        adapter.client.get_token_claims.assert_called_once()

    def test_message_must_match_source_queue_key_and_routing_key(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            adapter = make_adapter(make_settings(directory))
            delivery, channel = make_delivery(json.dumps(message).encode(), "irtjv.wrong")
            with self.assertLogs("pairamid_rabbitmq_client.partner.adapter", level="WARNING"):
                adapter.handle_execution(delivery)

        channel.basic_reject.assert_called_once_with(delivery_tag=1, requeue=False)
        adapter.client.publish_event = MagicMock()
        adapter.client.publish_event.assert_not_called()

    def test_manual_result_publication_uses_saved_context_and_correct_filename(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            prepare_downloaded_input(settings, message)
            results_dir = settings.results_dir_for("lri")
            results_dir.mkdir(parents=True)
            result_path = results_dir / "results-irtjv-lri-1.json"
            result_path.write_text('{"status":"completed","value":42}', encoding="utf-8")

            adapter = PartnerAdapter(settings)
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                }
            )
            adapter.client.publish_result = MagicMock()

            self.assertTrue(adapter.publish_result_file(result_path))
            adapter.client.publish_result.assert_called_once()
            published = adapter.client.publish_result.call_args
            event = published.kwargs["event"]
            self.assertEqual(event["event_type"], "simulation_execution.succeeded")
            self.assertEqual(
                event["result"]["payload"]["output"],
                {"status": "completed", "value": 42},
            )
            self.assertEqual(
                event["simulation_execution_id"],
                message["simulation_execution_id"],
            )

            state = json.loads(
                (settings.state_dir_for("lri") / "input-irtjv-lri-1.json").read_text()
            )
            self.assertTrue(state["result_published"])
            self.assertEqual(state["result_event"], event)
            self.assertFalse(adapter.publish_result_file(result_path))

        adapter.client.publish_result.assert_called_once()

    def test_manual_result_retry_reuses_event_id_after_ambiguous_failure(self) -> None:
        message = execution_message()
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            prepare_downloaded_input(settings, message)
            settings.results_dir_for("lri").mkdir(parents=True)
            result_path = settings.results_dir_for("lri") / "results-irtjv-lri-1.json"
            result_path.write_text('{"value":42}', encoding="utf-8")

            adapter = PartnerAdapter(settings)
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                }
            )
            adapter.client.publish_result = MagicMock(
                side_effect=[PublishConnectionError("connection lost"), None]
            )

            with self.assertRaises(PublishConnectionError):
                adapter.publish_result_file(result_path)
            self.assertTrue(adapter.publish_result_file(result_path))

            first_event = adapter.client.publish_result.call_args_list[0].kwargs["event"]
            second_event = adapter.client.publish_result.call_args_list[1].kwargs["event"]
            self.assertEqual(first_event["event_id"], second_event["event_id"])
            state = json.loads(
                (settings.state_dir_for("lri") / "input-irtjv-lri-1.json").read_text()
            )
            self.assertTrue(state["result_published"])

    def test_manual_error_result_publishes_failed_event(self) -> None:
        message = execution_message()
        error = json.loads(
            (EXAMPLES / "simulation-execution-error-payload.json").read_text()
        )
        for key in ("simulation_execution_id", "run_id", "attempt_id", "simulation_key"):
            error[key] = message[key]
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            prepare_downloaded_input(settings, message)
            settings.results_dir_for("lri").mkdir(parents=True)
            result_path = settings.results_dir_for("lri") / "results-irtjv-lri-1.json"
            result_path.write_text(json.dumps(error), encoding="utf-8")

            adapter = PartnerAdapter(settings)
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                }
            )
            adapter.client.publish_result = MagicMock()

            self.assertTrue(adapter.publish_result_file(result_path))
            event = adapter.client.publish_result.call_args.kwargs["event"]
            self.assertEqual(event["event_type"], "simulation_execution.failed")
            self.assertEqual(event["error"], error["error"])

    def test_malformed_error_result_is_not_published_as_success(self) -> None:
        message = execution_message()
        error = json.loads(
            (EXAMPLES / "simulation-execution-error-payload.json").read_text()
        )
        for key in ("simulation_execution_id", "run_id", "attempt_id", "simulation_key"):
            error[key] = message[key]
        del error["error"]["retryable"]
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            prepare_downloaded_input(settings, message)
            settings.results_dir_for("lri").mkdir(parents=True)
            result_path = settings.results_dir_for("lri") / "results-irtjv-lri-1.json"
            result_path.write_text(json.dumps(error), encoding="utf-8")

            adapter = PartnerAdapter(settings)
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                }
            )
            adapter.client.publish_result = MagicMock()

            with self.assertRaises(ContractValidationError):
                adapter.publish_result_file(result_path)
            adapter.client.publish_result.assert_not_called()
            self.assertFalse(result_path.exists())
            archived_body = next(
                settings.rejected_results_dir_for("lri").glob("*.body")
            )
            archived_result = json.loads(archived_body.read_bytes())
            self.assertEqual(
                archived_result["error"]["code"],
                "PARTNER_EXECUTION_FAILED",
            )

    def test_manual_result_rejects_wrong_name_and_unknown_context(self) -> None:
        with TemporaryDirectory() as directory:
            settings = make_settings(directory)
            results_dir = settings.results_dir_for("lri")
            results_dir.mkdir(parents=True)
            wrong_name = results_dir / "result-irtjv-lri-1.json"
            wrong_name.write_text('{"value":42}', encoding="utf-8")
            orphan = results_dir / "results-irtjv-lri-999.json"
            orphan.write_text('{"value":42}', encoding="utf-8")

            adapter = PartnerAdapter(settings)
            adapter.client.get_token_claims = MagicMock(
                return_value={
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                }
            )
            with self.assertRaisesRegex(ValueError, "Result filename"):
                adapter.publish_result_file(wrong_name)
            with self.assertRaises(FileNotFoundError):
                adapter.publish_result_file(orphan)


if __name__ == "__main__":
    unittest.main()
