"""Role-specific RabbitMQ clients for partners and the Pairamid platform."""

from __future__ import annotations

import re
from pathlib import Path

from .consumer import Consumer
from .publisher import Publisher

_PARTNER_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_SIMULATION_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]*$")


class PartnerClient(Consumer):
    """Client restricted to one partner input queue and shared results queue."""

    def __init__(self, partner_id: str, *args, **kwargs) -> None:
        if not _PARTNER_ID_RE.fullmatch(partner_id):
            raise ValueError("partner_id must contain lowercase letters, digits, '_' or '-'")
        self.partner_id = partner_id
        self.input_queue = f"pairamid.input.{partner_id}"
        self.results_queue = "pairamid.results"
        super().__init__(*args, **kwargs)

    def publish_result(self, file_path: str | Path) -> None:
        """Publish the contents of a result file to the shared results queue."""
        message = Path(file_path).read_bytes()
        with self:
            self.publish(queue=self.results_queue, message=message)


class PlatformClient(Publisher):
    """Platform client for publishing inputs to partner queues."""

    def publish_input(self, partner_id: str, message: str, simulation_name: str) -> None:
        """Publish an input body with its simulation name as AMQP metadata."""
        if not _PARTNER_ID_RE.fullmatch(partner_id):
            raise ValueError("partner_id must contain lowercase letters, digits, '_' or '-'")
        if not _SIMULATION_NAME_RE.fullmatch(simulation_name):
            raise ValueError(
                "simulation_name must contain lowercase letters, digits, '_' or '-'")
        self.publish(
            queue=f"pairamid.input.{partner_id}",
            message=message,
            headers={"simulation_name": simulation_name},
        )
