"""Token-assigned partner adapter process and execution-message validation."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import signal
from pathlib import Path
from types import FrameType
from typing import Any

from pairamid_rabbitmq_client import ContractValidationError, Delivery, PartnerClient
from pairamid_rabbitmq_client.contracts import validate_execution_message
from pairamid_rabbitmq_client.exceptions import PublishError

from .config import PartnerSettings, parse_queue_assignments
from .storage import (
    InputConflictError,
    InputStateStore,
    archive_rejected_message,
    archive_rejected_result,
    materialize_input,
)

logger = logging.getLogger(__name__)


class _UnexpectedDestinationError(ValueError):
    pass


class PartnerAdapter:
    """Poll assigned partner queues and durably confirm valid executions."""

    def __init__(self, settings: PartnerSettings) -> None:
        self.settings = settings
        self.client = PartnerClient(
            partner_id=None,
            host=settings.host,
            port=settings.port,
            vhost=settings.vhost,
            client_id=settings.client_id,
            client_secret=settings.client_secret,
            token_url=settings.token_url,
            input_queue="",
            poll_interval_seconds=settings.poll_interval_seconds,
        )
        self._current_assignments: dict[str, tuple[str, str]] = {}

    def _queues_for_current_token(self) -> tuple[str, ...]:
        assignments = parse_queue_assignments(
            self.client.get_token_claims(),
            self.settings.expected_partner_id,
        )
        self.settings.prepare_directories(
            simulation_key for _, simulation_key in assignments.values()
        )
        self._current_assignments = assignments
        logger.info(
            "event=partner_queues_loaded partner_id=%s queue_count=%d",
            next(iter(assignments.values()))[0],
            len(assignments),
        )
        return tuple(assignments)

    def handle_execution(self, delivery: Delivery) -> None:
        """Validate, materialize, publish the download event, and then acknowledge."""
        try:
            message = json.loads(delivery.body.decode("utf-8"))
            validate_execution_message(message)
            if delivery.source_queue is None:
                raise _UnexpectedDestinationError("delivery does not identify its source queue")
            assignment = self._current_assignments.get(delivery.source_queue)
            if assignment is None:
                raise _UnexpectedDestinationError("delivery came from an unassigned queue")
            partner_id, simulation_key = assignment
            if (
                message["simulation_key"] != simulation_key
                or delivery.routing_key != f"{partner_id}.{simulation_key}"
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
                self.settings.input_dir_for(simulation_key),
                partner_id,
            )
            state_store = InputStateStore(
                self.settings.state_dir_for(simulation_key),
                partner_id,
            )
            state = state_store.load_or_create(message, input_path)
        except InputConflictError as exc:
            self._reject_invalid(delivery, str(exc))
            return
        except OSError:
            logger.exception(
                "event=input_materialization_failed partner_id=%s simulation_execution_id=%s",
                partner_id,
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
                    partner_id=partner_id,
                )
            except PublishError:
                logger.exception(
                    "event=input_downloaded_publish_failed partner_id=%s "
                    "simulation_execution_id=%s",
                    partner_id,
                    message["simulation_execution_id"],
                )
                delivery.nack(requeue=True)
                return

            try:
                state_store.mark_published(message, state)
            except OSError:
                logger.exception(
                    "event=input_downloaded_state_update_failed partner_id=%s "
                    "simulation_execution_id=%s",
                    partner_id,
                    message["simulation_execution_id"],
                )
                delivery.nack(requeue=True)
                return

        delivery.ack()
        logger.info(
            "event=execution_input_downloaded partner_id=%s simulation_key=%s "
            "simulation_execution_id=%s created=%s redelivered=%s",
            partner_id,
            simulation_key,
            message["simulation_execution_id"],
            created,
            delivery.redelivered,
        )

    def _reject_invalid(self, delivery: Delivery, reason: str) -> None:
        properties = delivery.properties
        message_id: Any = properties.message_id if properties is not None else None
        assignment = self._current_assignments.get(delivery.source_queue or "")
        simulation_key = assignment[1] if assignment is not None else "_unassigned"
        try:
            archive_path = archive_rejected_message(
                delivery.body,
                self.settings.rejected_input_dir_for(simulation_key),
                reason,
                message_id if isinstance(message_id, str) else None,
            )
        except OSError:
            logger.exception(
                "event=execution_rejection_archive_failed partner_id=%s routing_key=%s",
                self.settings.expected_partner_id or "unknown",
                delivery.routing_key,
            )
            delivery.nack(requeue=True)
            return
        delivery.reject(requeue=False)
        logger.warning(
            "event=execution_rejected partner_id=%s routing_key=%s archive=%s reason=%s",
            self.settings.expected_partner_id or "token-assigned",
            delivery.routing_key,
            archive_path.name,
            reason,
        )

    def publish_result_file(self, file_path: str | Path) -> bool:
        """Publish one explicitly selected result file for its saved execution context."""
        result_path = Path(file_path).expanduser()
        assignments = parse_queue_assignments(
            self.client.get_token_claims(),
            self.settings.expected_partner_id,
        )
        self.settings.prepare_directories(
            simulation_key for _, simulation_key in assignments.values()
        )

        matches: list[tuple[str, str, int]] = []
        for partner_id, simulation_key in assignments.values():
            match = re.fullmatch(
                rf"results-{re.escape(partner_id)}-{re.escape(simulation_key)}-"
                r"([1-9][0-9]*)\.json",
                result_path.name,
            )
            if match is not None:
                matches.append((partner_id, simulation_key, int(match.group(1))))
        if len(matches) != 1:
            raise ValueError(
                "Result filename must be "
                "results-{partner_id}-{assigned_simulation_key}-{simulation_execution_id}.json"
            )
        partner_id, simulation_key, execution_id = matches[0]

        expected_directory = self.settings.results_dir_for(simulation_key).resolve()
        if result_path.is_symlink() or result_path.parent.resolve() != expected_directory:
            raise ValueError(
                f"Result file must be placed directly in {expected_directory}"
            )
        if not result_path.is_file():
            raise FileNotFoundError(f"Result file does not exist: {result_path}")
        result_sha256 = hashlib.sha256(result_path.read_bytes()).hexdigest()

        state_store = InputStateStore(
            self.settings.state_dir_for(simulation_key),
            partner_id,
        )
        state = state_store.load_for_result(simulation_key, execution_id)
        if state.get("result_published") is True:
            if state.get("result_sha256") != result_sha256:
                raise InputConflictError(
                    f"Published result file changed: {result_path.name}"
                )
            logger.info(
                "event=result_already_published partner_id=%s simulation_key=%s "
                "simulation_execution_id=%s",
                partner_id,
                simulation_key,
                execution_id,
            )
            return False

        execution_message = state["execution_message"]
        try:
            event = self.client.build_result_event(result_path, execution_message)
        except (ValueError, ContractValidationError) as exc:
            archive_path = archive_rejected_result(
                result_path,
                self.settings.rejected_results_dir_for(simulation_key),
                str(exc),
            )
            logger.warning(
                "event=result_rejected partner_id=%s simulation_key=%s "
                "simulation_execution_id=%s archive=%s reason=%s",
                partner_id,
                simulation_key,
                execution_id,
                archive_path.name,
                exc,
            )
            raise
        event = state_store.save_result_event(state, result_sha256, event)
        self.client.publish_result(
            result_path,
            execution_message,
            partner_id=partner_id,
            event=event,
        )
        state_store.mark_result_published(state)
        logger.info(
            "event=result_published partner_id=%s simulation_key=%s "
            "simulation_execution_id=%s event_type=%s event_id=%s",
            partner_id,
            simulation_key,
            execution_id,
            event["event_type"],
            event["event_id"],
        )
        return True

    def run(self) -> None:
        self.settings.prepare_directories()
        logger.info(
            "event=adapter_started expected_partner_id=%s simulation_dir=%s",
            self.settings.expected_partner_id or "from-token",
            self.settings.simulation_dir,
        )
        self.client.poll_forever_queues(self._queues_for_current_token, self.handle_execution)

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
