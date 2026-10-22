"""IRTJV adapter process and execution-message validation."""

from __future__ import annotations

import json
import logging
import signal
from types import FrameType
from typing import Any

from pairamid_rabbitmq_client import ContractValidationError, Delivery, PartnerClient
from pairamid_rabbitmq_client.contracts import validate_execution_message
from pairamid_rabbitmq_client.exceptions import PublishError

from .config import PartnerSettings
from .storage import (
    InputConflictError,
    InputStateStore,
    archive_rejected_message,
    materialize_input,
)

logger = logging.getLogger(__name__)


class _UnexpectedDestinationError(ValueError):
    pass


class PartnerAdapter:
    def __init__(self, settings: PartnerSettings) -> None:
        self.settings = settings
        self.client = PartnerClient(
            partner_id=settings.partner_id,
            host=settings.host,
            port=settings.port,
            vhost=settings.vhost,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            token_url=settings.token_url,
            input_queue=settings.queue,
            simulation_input_dir=str(settings.input_dir),
            poll_interval_seconds=settings.poll_interval_seconds,
        )
        self._state_store = InputStateStore(settings.state_dir, settings.partner_id)

    def handle_execution(self, delivery: Delivery) -> None:
        """Validate, materialize, publish the download event, and then acknowledge."""
        try:
            message = json.loads(delivery.body.decode("utf-8"))
            validate_execution_message(message)
            if (
                message["simulation_key"] != self.settings.simulation_key
                or delivery.routing_key != self.settings.routing_key
            ):
                raise _UnexpectedDestinationError("unexpected simulation or routing key")
        except (UnicodeDecodeError, json.JSONDecodeError, ContractValidationError) as exc:
            self._reject_invalid(delivery, str(exc))
            return
        except _UnexpectedDestinationError as exc:
            self._reject_invalid(delivery, str(exc))
            return

        try:
            input_path, created = materialize_input(
                message,
                self.settings.input_dir,
                self.settings.partner_id,
            )
            state = self._state_store.load_or_create(message, input_path)
        except InputConflictError as exc:
            self._reject_invalid(delivery, str(exc))
            return
        except OSError:
            logger.exception(
                "event=input_materialization_failed partner_id=%s simulation_execution_id=%s",
                self.settings.partner_id,
                message["simulation_execution_id"],
            )
            delivery.nack(requeue=True)
            return

        if not state["input_downloaded_published"]:
            event = state["input_downloaded_event"]
            try:
                self.client.publish_event(
                    "simulation_execution.input_downloaded",
                    event,
                    event["event_id"],
                )
            except PublishError:
                logger.exception(
                    "event=input_downloaded_publish_failed partner_id=%s "
                    "simulation_execution_id=%s",
                    self.settings.partner_id,
                    message["simulation_execution_id"],
                )
                delivery.nack(requeue=True)
                return

            try:
                self._state_store.mark_published(message, state)
            except OSError:
                logger.exception(
                    "event=input_downloaded_state_update_failed partner_id=%s "
                    "simulation_execution_id=%s",
                    self.settings.partner_id,
                    message["simulation_execution_id"],
                )
                delivery.nack(requeue=True)
                return

        delivery.ack()
        logger.info(
            "event=execution_input_downloaded partner_id=%s simulation_key=%s "
            "simulation_execution_id=%s created=%s redelivered=%s",
            self.settings.partner_id,
            self.settings.simulation_key,
            message["simulation_execution_id"],
            created,
            delivery.redelivered,
        )

    def _reject_invalid(self, delivery: Delivery, reason: str) -> None:
        properties = delivery.properties
        message_id: Any = properties.message_id if properties is not None else None
        try:
            archive_path = archive_rejected_message(
                delivery.body,
                self.settings.rejected_input_dir,
                reason,
                message_id if isinstance(message_id, str) else None,
            )
        except OSError:
            logger.exception(
                "event=execution_rejection_archive_failed partner_id=%s routing_key=%s",
                self.settings.partner_id,
                delivery.routing_key,
            )
            delivery.nack(requeue=True)
            return
        delivery.reject(requeue=False)
        logger.warning(
            "event=execution_rejected partner_id=%s routing_key=%s archive=%s reason=%s",
            self.settings.partner_id,
            delivery.routing_key,
            archive_path.name,
            reason,
        )

    def run(self) -> None:
        self.settings.prepare_directories()
        logger.info(
            "event=adapter_started partner_id=%s simulation_key=%s queue=%s simulation_dir=%s",
            self.settings.partner_id,
            self.settings.simulation_key,
            self.settings.queue,
            self.settings.simulation_dir,
        )
        self.client.poll_forever(self.settings.queue, self.handle_execution)

    def stop(self) -> None:
        self.client.stop()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    try:
        settings = PartnerSettings.from_env()
    except ValueError as exc:
        logger.error("event=configuration_error error=%s", exc)
        raise SystemExit(2) from exc

    adapter = PartnerAdapter(settings)

    def request_stop(_signum: int, _frame: FrameType | None) -> None:
        adapter.stop()

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    adapter.run()
