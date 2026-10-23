"""Polling consumer for partner input queues."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from threading import Event
from typing import Any

import pika

from .publisher import Publisher

logger = logging.getLogger(__name__)
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")
MessageHandler = Callable[["Delivery"], None]
QueueProvider = Callable[[], Sequence[str]]


class _InvalidMessageError(ValueError):
    pass


def _validate_queues(queues: Sequence[str]) -> None:
    if isinstance(queues, (str, bytes)) or not isinstance(queues, Sequence) or not queues:
        raise ValueError("queues must be a non-empty sequence of queue names")
    if any(not isinstance(queue, str) or not queue for queue in queues):
        raise ValueError("queue names must be non-empty strings")
    if len(set(queues)) != len(queues):
        raise ValueError("queue names must be unique")


class Delivery:
    """A single RabbitMQ delivery that can be acknowledged exactly once."""

    def __init__(
        self,
        channel: pika.adapters.blocking_connection.BlockingChannel,
        delivery_tag: int,
        body: bytes,
        properties: pika.BasicProperties | None,
        headers: Mapping[str, Any] | None,
        redelivered: bool,
        routing_key: str,
        source_queue: str | None = None,
    ) -> None:
        self.body = body
        self.properties = properties
        self.headers = dict(headers or {})
        self.delivery_tag = delivery_tag
        self.redelivered = redelivered
        self.routing_key = routing_key
        self.source_queue = source_queue
        self._channel = channel
        self._responded = False

    @property
    def responded(self) -> bool:
        """Whether an ack, reject, or nack has already been attempted."""
        return self._responded

    def ack(self) -> None:
        """Acknowledge this delivery."""
        self._ensure_unanswered()
        self._responded = True
        self._channel.basic_ack(delivery_tag=self.delivery_tag)

    def reject(self, requeue: bool = False) -> None:
        """Reject this delivery, optionally returning it to the queue."""
        self._ensure_unanswered()
        if not isinstance(requeue, bool):
            raise TypeError("requeue must be a bool")
        self._responded = True
        self._channel.basic_reject(delivery_tag=self.delivery_tag, requeue=requeue)

    def nack(self, *, requeue: bool) -> None:
        """Negatively acknowledge this delivery."""
        self._ensure_unanswered()
        if not isinstance(requeue, bool):
            raise TypeError("requeue must be a bool")
        self._responded = True
        self._channel.basic_nack(delivery_tag=self.delivery_tag, requeue=requeue)

    def _ensure_unanswered(self) -> None:
        if self._responded:
            raise RuntimeError("Delivery has already been acknowledged, rejected, or nacked")


class Consumer(Publisher):
    """Poll RabbitMQ queues and provide explicit delivery acknowledgements."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        vhost: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
        *,
        simulation_input_dir: str | None = None,
        poll_interval_seconds: float | None = None,
        user_id: str | None = None,
        set_user_id: bool = True,
    ) -> None:
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
        if simulation_input_dir:
            self.simulation_input_dir = Path(simulation_input_dir)
        else:
            simulation_dir = os.environ.get("SIMULATION_DIR", "./simulations").strip()
            if not simulation_dir:
                raise ValueError("SIMULATION_DIR must not be empty")
            self.simulation_input_dir = Path(simulation_dir).expanduser() / "input"
        interval = (
            float(os.environ.get("CONSUMER_POLL_INTERVAL_SECONDS", "30"))
            if poll_interval_seconds is None
            else poll_interval_seconds
        )
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("CONSUMER_POLL_INTERVAL_SECONDS must be a positive number")
        self.poll_interval_seconds = interval
        self._stop_event = Event()

    def poll(
        self,
        queue: str,
        handler: MessageHandler,
        *,
        max_messages: int | None = None,
    ) -> int:
        """Process available messages from one queue and close the connection.

        When ``max_messages`` is omitted, the poll drains the queue until it is
        empty. A positive limit bounds the number of deliveries in this cycle.
        """
        if not isinstance(queue, str) or not queue:
            raise ValueError("queue must be a non-empty string")
        return self.poll_queues(
            (queue,),
            handler,
            max_messages_per_queue=max_messages,
        )

    def poll_queues(
        self,
        queues: Sequence[str] | QueueProvider,
        handler: MessageHandler,
        *,
        max_messages_per_queue: int | None = 1,
    ) -> int:
        """Poll assigned queues in order, bounding work per queue for fairness.

        A queue provider is resolved after connecting, so its values can be
        derived from the exact cached access token used by the connection.
        """
        if not callable(handler):
            raise TypeError("handler must be callable")
        if max_messages_per_queue is not None and (
            isinstance(max_messages_per_queue, bool)
            or not isinstance(max_messages_per_queue, int)
            or max_messages_per_queue <= 0
        ):
            raise ValueError("max_messages_per_queue must be a positive integer or None")
        if not callable(queues):
            _validate_queues(queues)

        processed = 0
        try:
            self.connect()
            channel = self._channel
            if channel is None:
                raise RuntimeError("RabbitMQ connection did not provide a channel")

            selected_queues = queues() if callable(queues) else queues
            _validate_queues(selected_queues)
            for queue in selected_queues:
                handled_from_queue = 0
                while not self._stop_event.is_set() and (
                    max_messages_per_queue is None
                    or handled_from_queue < max_messages_per_queue
                ):
                    method_frame, properties, body = channel.basic_get(
                        queue=queue, auto_ack=False
                    )
                    if method_frame is None:
                        break
                    if (
                        method_frame.delivery_tag is None
                        or not isinstance(body, bytes)
                        or not isinstance(method_frame.redelivered, bool)
                        or not isinstance(method_frame.routing_key, str)
                    ):
                        raise RuntimeError("RabbitMQ returned an invalid delivery frame")

                    headers = properties.headers if properties is not None else None
                    delivery = Delivery(
                        channel=channel,
                        delivery_tag=method_frame.delivery_tag,
                        body=body,
                        properties=properties,
                        headers=headers,
                        redelivered=method_frame.redelivered,
                        routing_key=method_frame.routing_key,
                        source_queue=queue,
                    )
                    processed += 1
                    handled_from_queue += 1
                    try:
                        handler(delivery)
                        if not delivery.responded:
                            logger.error(
                                "Handler returned without responding to delivery %s from %s",
                                delivery.delivery_tag,
                                queue,
                            )
                            delivery.nack(requeue=True)
                            raise RuntimeError(
                                "Message handler returned without ack, reject, or nack"
                            )
                    except Exception:
                        logger.exception(
                            "Message handler failed for delivery %s from %s",
                            delivery.delivery_tag,
                            queue,
                        )
                        if not delivery.responded:
                            delivery.nack(requeue=True)
                        raise
            return processed
        finally:
            self.close()

    def poll_forever_queues(
        self,
        queues: Sequence[str] | QueueProvider,
        handler: MessageHandler,
        *,
        max_messages_per_queue: int = 1,
    ) -> None:
        """Poll all assigned queues fairly until stop() is called."""
        if not callable(queues):
            _validate_queues(queues)
        if not callable(handler):
            raise TypeError("handler must be callable")
        if (
            isinstance(max_messages_per_queue, bool)
            or not isinstance(max_messages_per_queue, int)
            or max_messages_per_queue <= 0
        ):
            raise ValueError("max_messages_per_queue must be a positive integer")

        while not self._stop_event.is_set():
            try:
                self.poll_queues(
                    queues,
                    handler,
                    max_messages_per_queue=max_messages_per_queue,
                )
            except Exception:
                logger.exception("RabbitMQ multi-queue polling cycle failed")
            self._stop_event.wait(self.poll_interval_seconds)

    def poll_once(self, queue: str) -> Path | None:
        """Fetch at most one legacy message, persist it, and acknowledge it."""
        target_file: Path | None = None

        def store(delivery: Delivery) -> None:
            nonlocal target_file
            simulation_name = delivery.headers.get("simulation_name")
            try:
                target_file = self._store_simulation_input(delivery.body, simulation_name)
            except _InvalidMessageError as exc:
                delivery.nack(requeue=False)
                logger.warning("Rejected invalid input message from %s: %s", queue, exc)
                return
            delivery.ack()
            logger.info("Stored input message from %s as %s", queue, target_file.name)

        self.poll(queue, store, max_messages=1)
        return target_file

    def poll_forever(
        self,
        queue: str,
        handler: MessageHandler | None = None,
        *,
        max_messages: int | None = None,
    ) -> None:
        """Poll periodically until stop() is called.

        Omitting ``handler`` retains the legacy file-writing behavior through
        ``poll_once`` for compatibility with existing callers.
        """
        if not isinstance(queue, str) or not queue:
            raise ValueError("queue must be a non-empty string")
        if handler is not None and not callable(handler):
            raise TypeError("handler must be callable")
        if max_messages is not None and (
            isinstance(max_messages, bool)
            or not isinstance(max_messages, int)
            or max_messages <= 0
        ):
            raise ValueError("max_messages must be a positive integer or None")

        while not self._stop_event.is_set():
            try:
                if handler is None:
                    self.poll_once(queue)
                else:
                    self.poll(queue, handler, max_messages=max_messages)
            except Exception:
                logger.exception("RabbitMQ polling failed for queue %s", queue)
            self._stop_event.wait(self.poll_interval_seconds)

    def stop(self) -> None:
        """Stop after the current handler finishes and wake the polling loop."""
        self._stop_event.set()

    def _store_simulation_input(self, body: bytes, simulation_name: str | None) -> Path:
        """Store the original JSON body under a unique, validated simulation filename."""
        if not isinstance(simulation_name, str) or not _SAFE_NAME_RE.fullmatch(simulation_name):
            raise _InvalidMessageError("missing or invalid simulation_name header")

        try:
            message = body.decode("utf-8")
            payload = json.loads(message)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise _InvalidMessageError("body is not valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise _InvalidMessageError("JSON body must be an object")

        self.simulation_input_dir.mkdir(parents=True, exist_ok=True)
        descriptor, temporary_name = tempfile.mkstemp(dir=self.simulation_input_dir)
        try:
            with os.fdopen(descriptor, "wb") as temporary_file:
                temporary_file.write(body)
                temporary_file.flush()
                os.fsync(temporary_file.fileno())

            suffix = 1
            while True:
                suffix_part = "" if suffix == 1 else f"-{suffix}"
                target_file = self.simulation_input_dir / (f"{simulation_name}{suffix_part}.json")
                try:
                    os.link(temporary_name, target_file)
                    return target_file
                except FileExistsError:
                    suffix += 1
        finally:
            os.unlink(temporary_name)
