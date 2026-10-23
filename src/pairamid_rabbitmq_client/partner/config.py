"""Environment configuration for the IRTJV partner adapter."""

from __future__ import annotations

import math
import os
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

_PARTNER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")
_SIMULATION_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,99}$")


@dataclass(frozen=True)
class PartnerSettings:
    host: str
    port: int
    vhost: str
    client_id: str
    client_secret: str
    token_url: str
    expected_partner_id: str | None
    simulation_dir: Path
    poll_interval_seconds: float

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> PartnerSettings:
        """Build and validate settings from the process environment."""
        values = os.environ if environ is None else environ
        expected_partner_id = values.get("PARTNER_ID", "").strip() or None
        if expected_partner_id is not None and not _PARTNER_ID_RE.fullmatch(
            expected_partner_id
        ):
            raise ValueError("PARTNER_ID must be a valid lowercase partner identifier")

        client_id = _required(values, "RABBITMQ_CLIENT_ID")
        client_secret = _required(values, "RABBITMQ_CLIENT_SECRET")
        token_url = _required(values, "OAUTH2_TOKEN_URL")
        host = values.get("RABBITMQ_HOST", "rabbitmq").strip()
        if not host:
            raise ValueError("RABBITMQ_HOST must not be empty")
        vhost = values.get("RABBITMQ_DEFAULT_VHOST", "/")
        if not vhost:
            raise ValueError("RABBITMQ_DEFAULT_VHOST must not be empty")

        port_value = values.get("RABBITMQ_PORT", "5672")
        try:
            port = int(port_value)
        except ValueError as exc:
            raise ValueError("RABBITMQ_PORT must be an integer from 1 to 65535") from exc
        if not 1 <= port <= 65535:
            raise ValueError("RABBITMQ_PORT must be an integer from 1 to 65535")

        interval_value = values.get("CONSUMER_POLL_INTERVAL_SECONDS", "30")
        try:
            interval = float(interval_value)
        except ValueError as exc:
            raise ValueError(
                "CONSUMER_POLL_INTERVAL_SECONDS must be a finite positive number"
            ) from exc
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("CONSUMER_POLL_INTERVAL_SECONDS must be a finite positive number")

        simulation_dir_value = values.get("SIMULATION_DIR", "./simulations").strip()
        if not simulation_dir_value:
            raise ValueError("SIMULATION_DIR must not be empty")

        return cls(
            host=host,
            port=port,
            vhost=vhost,
            client_id=client_id,
            client_secret=client_secret,
            token_url=token_url,
            expected_partner_id=expected_partner_id,
            simulation_dir=Path(simulation_dir_value).expanduser(),
            poll_interval_seconds=interval,
        )

    def simulation_path(self, simulation_key: str) -> Path:
        if not _SIMULATION_KEY_RE.fullmatch(simulation_key):
            raise ValueError(f"Invalid simulation key: {simulation_key!r}")
        return self.simulation_dir / simulation_key

    def input_dir_for(self, simulation_key: str) -> Path:
        return self.simulation_path(simulation_key) / "input"

    def results_dir_for(self, simulation_key: str) -> Path:
        return self.simulation_path(simulation_key) / "results"

    def rejected_input_dir_for(self, simulation_key: str) -> Path:
        return self.simulation_path(simulation_key) / "rejected" / "input"

    def rejected_results_dir_for(self, simulation_key: str) -> Path:
        return self.simulation_path(simulation_key) / "rejected" / "results"

    def state_dir_for(self, simulation_key: str) -> Path:
        return self.simulation_path(simulation_key) / "state"

    def prepare_directories(self, simulation_keys: Iterable[str] = ()) -> None:
        """Create directories for assigned simulations and adapter state."""
        directories = [self.simulation_dir]
        for simulation_key in dict.fromkeys(simulation_keys):
            input_dir = self.input_dir_for(simulation_key)
            directories.extend(
                (
                    input_dir,
                    input_dir / ".tmp",
                    self.results_dir_for(simulation_key),
                    self.rejected_input_dir_for(simulation_key),
                    self.rejected_results_dir_for(simulation_key),
                    self.state_dir_for(simulation_key),
                )
            )
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise OSError(f"Cannot create simulation directory {directory}: {exc}") from exc
            if not directory.is_dir():
                raise NotADirectoryError(f"Simulation path is not a directory: {directory}")


def parse_queue_assignments(
    claims: Mapping[str, object],
    expected_partner_id: str | None = None,
) -> dict[str, tuple[str, str]]:
    """Validate token identity and concrete queue assignments.

    Returns queue name -> (partner_id, simulation_key). RabbitMQ still enforces
    authorization independently through the token's extra_scope claim.
    """
    partner_id = claims.get("partner_id")
    if not isinstance(partner_id, str) or not _PARTNER_ID_RE.fullmatch(partner_id):
        raise ValueError("Access token claim partner_id must be a valid partner identifier")
    if expected_partner_id is not None and partner_id != expected_partner_id:
        raise ValueError("PARTNER_ID does not match the access token partner_id claim")

    assigned_queues = claims.get("simulation_queues")
    if (
        not isinstance(assigned_queues, list)
        or not assigned_queues
        or any(not isinstance(queue, str) or not queue for queue in assigned_queues)
    ):
        raise ValueError(
            "Access token claim simulation_queues must be a non-empty JSON array of queue names"
        )
    if len(assigned_queues) != len(set(assigned_queues)):
        raise ValueError("Access token claim simulation_queues must not contain duplicates")
    if len(assigned_queues) > 100:
        raise ValueError("Access token claim simulation_queues cannot contain more than 100 queues")

    assignments: dict[str, tuple[str, str]] = {}
    for queue in assigned_queues:
        prefix = f"pairamid.input.{partner_id}."
        if not queue.startswith(prefix):
            raise ValueError(
                f"Assigned queue {queue!r} does not belong to partner {partner_id!r}"
            )
        simulation_key = queue[len(prefix) :]
        if not _SIMULATION_KEY_RE.fullmatch(simulation_key):
            raise ValueError(f"Assigned queue {queue!r} has an invalid simulation key")
        assignments[queue] = (partner_id, simulation_key)
    return assignments


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} must be set and not empty")
    return value.strip()
