"""Polling consumer for partner input queues."""

from __future__ import annotations

import json
import logging
import math
import os
import re
import tempfile
import time
from pathlib import Path

from .publisher import Publisher

logger = logging.getLogger(__name__)
_SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


class _InvalidMessageError(ValueError):
    pass


class Consumer(Publisher):
    """Poll a queue and persist valid input messages before acknowledging them."""

    def __init__(
        self,
        *args,
        simulation_input_dir: str | None = None,
        poll_interval_seconds: float | None = None,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.simulation_input_dir = Path(
            simulation_input_dir or os.environ.get("SIMULATION_INPUT_DIR", "./data")
        )
        interval = (
            float(os.environ.get("CONSUMER_POLL_INTERVAL_SECONDS", "30"))
            if poll_interval_seconds is None
            else poll_interval_seconds
        )
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("CONSUMER_POLL_INTERVAL_SECONDS must be a positive number")
        self.poll_interval_seconds = interval

    def poll_once(self, queue: str) -> Path | None:
        """Fetch at most one message, store it, acknowledge it, and close the connection."""
        try:
            self.connect()
            if self._channel is None:
                raise RuntimeError("RabbitMQ connection did not provide a channel")

            method_frame, properties, body = self._channel.basic_get(
                queue=queue, auto_ack=False
            )
            if method_frame is None:
                logger.debug("No input messages available in queue %s", queue)
                return None

            headers = properties.headers if properties is not None else None
            simulation_name = headers.get("simulation_name") if headers else None
            try:
                target_file = self._store_simulation_input(body, simulation_name)
            except _InvalidMessageError as exc:
                self._channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag, requeue=False
                )
                logger.warning("Rejected invalid input message from %s: %s", queue, exc)
                return None
            except OSError:
                self._channel.basic_nack(
                    delivery_tag=method_frame.delivery_tag, requeue=True
                )
                raise

            self._channel.basic_ack(delivery_tag=method_frame.delivery_tag)
            logger.info("Stored input message from %s as %s", queue, target_file.name)
            return target_file
        finally:
            self.close()

    def poll_forever(self, queue: str) -> None:
        """Poll periodically, logging transient failures and continuing the loop."""
        while True:
            try:
                self.poll_once(queue)
            except Exception:
                logger.exception("RabbitMQ polling failed for queue %s", queue)
            time.sleep(self.poll_interval_seconds)

    def _store_simulation_input(
        self, body: bytes, simulation_name: str | None
    ) -> Path:
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
                target_file = self.simulation_input_dir / (
                    f"{simulation_name}{suffix_part}.json"
                )
                try:
                    os.link(temporary_name, target_file)
                    return target_file
                except FileExistsError:
                    suffix += 1
        finally:
            os.unlink(temporary_name)
