"""Environment configuration for the IRTJV partner adapter."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class PartnerSettings:
    host: str
    port: int
    vhost: str
    client_id: str
    client_secret: str
    token_url: str
    partner_id: str
    simulation_dir: Path
    poll_interval_seconds: float

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> PartnerSettings:
        """Build and validate settings from the process environment."""
        values = os.environ if environ is None else environ
        partner_id = values.get("PARTNER_ID", "").strip()
        if not partner_id:
            raise ValueError("PARTNER_ID must be set to 'irtjv'")
        if partner_id != "irtjv":
            raise ValueError("This adapter release supports only PARTNER_ID=irtjv")

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
            partner_id=partner_id,
            simulation_dir=Path(simulation_dir_value).expanduser(),
            poll_interval_seconds=interval,
        )

    @property
    def simulation_key(self) -> str:
        return "lri"

    @property
    def queue(self) -> str:
        return f"pairamid.input.{self.partner_id}.{self.simulation_key}"

    @property
    def routing_key(self) -> str:
        return f"{self.partner_id}.{self.simulation_key}"

    @property
    def input_dir(self) -> Path:
        return self.simulation_dir / "input"

    @property
    def results_dir(self) -> Path:
        return self.simulation_dir / "results"

    @property
    def rejected_input_dir(self) -> Path:
        return self.simulation_dir / "rejected" / "input"

    @property
    def state_dir(self) -> Path:
        return self.simulation_dir / "state"

    def prepare_directories(self) -> None:
        """Create the adapter's exchange and internal state directories."""
        directories = (
            self.simulation_dir,
            self.input_dir,
            self.input_dir / ".tmp",
            self.results_dir,
            self.rejected_input_dir,
            self.simulation_dir / "rejected" / "results",
            self.state_dir,
        )
        for directory in directories:
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as exc:
                raise OSError(f"Cannot create simulation directory {directory}: {exc}") from exc
            if not directory.is_dir():
                raise NotADirectoryError(f"Simulation path is not a directory: {directory}")


def _required(values: Mapping[str, str], name: str) -> str:
    value = values.get(name)
    if value is None or not value.strip():
        raise ValueError(f"{name} must be set and not empty")
    return value.strip()
