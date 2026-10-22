"""Role-specific RabbitMQ clients for partners and the Pairamid platform."""

from __future__ import annotations

import json
import os
import re
import time
import warnings
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .consumer import Consumer
from .contracts import (
    validate_execution_message,
    validate_input_downloaded_event,
    validate_result_event,
)
from .publisher import Publisher

_PARTNER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")
_SIMULATION_NAME_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,99}$")


def _validate_partner(partner_id: object) -> None:
    if not isinstance(partner_id, str) or not _PARTNER_ID_RE.fullmatch(partner_id):
        raise ValueError("partner_id must match ^[a-z][a-z0-9_-]{0,99}$ (no dots)")


def _validate_simulation(name: object) -> None:
    if not isinstance(name, str) or not _SIMULATION_NAME_RE.fullmatch(name):
        raise ValueError("simulation key/name must match ^[a-z][a-z0-9_.-]{0,99}$")


class PartnerClient(Consumer):
    """Partner-side consumer and publisher for executions and simulation events."""

    def __init__(
        self,
        partner_id: str,
        host: str | None = None,
        port: int | None = None,
        vhost: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
        *,
        events_exchange: str | None = None,
        input_queue: str | None = None,
        results_queue: str | None = None,
        simulation_input_dir: str | None = None,
        poll_interval_seconds: float | None = None,
        user_id: str | None = None,
        set_user_id: bool = True,
    ) -> None:
        _validate_partner(partner_id)
        self.partner_id = partner_id
        self.events_exchange = (
            events_exchange
            if events_exchange is not None
            else os.environ.get("RABBITMQ_EVENTS_EXCHANGE", "simulation_events_exchange")
        )
        self.input_queue = (
            input_queue if input_queue is not None else f"pairamid.input.{partner_id}"
        )
        self.results_queue = results_queue if results_queue is not None else "pairamid.results"
        super().__init__(
            host,
            port,
            vhost,
            client_id,
            client_secret,
            token_url,
            simulation_input_dir=simulation_input_dir,
            poll_interval_seconds=poll_interval_seconds,
            user_id=user_id,
            set_user_id=set_user_id,
        )

    def execution_queue(self, simulation_key: str) -> str:
        """Return the provisioned queue name; does not start the new adapter mode."""
        _validate_simulation(simulation_key)
        return f"pairamid.input.{self.partner_id}.{simulation_key}"

    def publish_event(
        self,
        event_type: str,
        body: Mapping[str, Any],
        message_id: str,
    ) -> None:
        """Publish a validated event on an already open connection."""
        if event_type == "simulation_execution.input_downloaded":
            validate_input_downloaded_event(body)
            suffix = "input_downloaded"
        elif event_type in ("simulation_execution.succeeded", "simulation_execution.failed"):
            validate_result_event(body)
            suffix = "result"
        else:
            raise ValueError("Unsupported event_type")
        if body["event_type"] != event_type:
            raise ValueError("event_type must match the body")
        if body["event_id"] != message_id:
            raise ValueError("message_id must match event_id")
        self.publish(
            self.events_exchange,
            f"{self.partner_id}.{suffix}",
            json.dumps(dict(body), ensure_ascii=False, allow_nan=False),
            message_id=message_id,
            correlation_id=str(body["simulation_execution_id"]),
            timestamp=int(time.time()),
        )

    def publish_result(self, file_path: str | Path) -> None:
        """Publish the contents of a result file to the shared results queue."""
        warnings.warn(
            "publish_result is deprecated; use publish_event with a result contract",
            DeprecationWarning,
            stacklevel=2,
        )
        message = Path(file_path).read_bytes()
        with self:
            self.publish(queue=self.results_queue, message=message)


class PlatformClient(Publisher):
    """Platform client for publishing inputs to partner queues."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        vhost: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
        *,
        execution_exchange: str | None = None,
        user_id: str | None = None,
        set_user_id: bool = True,
    ) -> None:
        self.execution_exchange = (
            execution_exchange
            if execution_exchange is not None
            else os.environ.get("RABBITMQ_EXECUTION_EXCHANGE", "simulation_execution_exchange")
        )
        super().__init__(
            host,
            port,
            vhost,
            client_id,
            client_secret,
            token_url,
            user_id=user_id,
            set_user_id=set_user_id,
        )

    def publish_execution(
        self,
        partner_id: str,
        simulation_key: str,
        message: Mapping[str, Any],
        message_id: str,
    ) -> None:
        """Publish a validated execution on an already open connection."""
        _validate_partner(partner_id)
        _validate_simulation(simulation_key)
        validate_execution_message(message)
        if message["simulation_key"] != simulation_key:
            raise ValueError("simulation_key must match the message")
        if message["message_id"] != message_id:
            raise ValueError("message_id must match the message body")
        self.publish(
            self.execution_exchange,
            f"{partner_id}.{simulation_key}",
            json.dumps(dict(message), ensure_ascii=False, allow_nan=False),
            message_id=message_id,
            correlation_id=str(message["simulation_execution_id"]),
            timestamp=int(time.time()),
        )

    def publish_input(self, partner_id: str, message: str, simulation_name: str) -> None:
        """Publish an input body with its simulation name as AMQP metadata."""
        warnings.warn(
            "publish_input is deprecated; use publish_execution", DeprecationWarning, stacklevel=2
        )
        _validate_partner(partner_id)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", simulation_name):
            raise ValueError(
                "Legacy simulation_name must start with a letter/digit and contain "
                "letters, digits, '_' or '-' (no dots)"
            )
        self.publish(
            queue=f"pairamid.input.{partner_id}",
            message=message,
            headers={"simulation_name": simulation_name},
        )
