"""Internal RabbitMQ publishing and OAuth2 connection primitives."""

from __future__ import annotations

import os
import logging
from collections.abc import Mapping
from typing import Any

import pika

from .oauth2 import ClientCredentialsTokenProvider

logger = logging.getLogger(__name__)


class Publisher:
    """Open an OAuth2-authenticated RabbitMQ connection and publish messages."""

    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        vhost: str | None = None,
        client_id: str | None = None,
        client_secret: str | None = None,
        token_url: str | None = None,
    ) -> None:
        self.host = host or os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self.port = port or int(os.environ.get("RABBITMQ_PORT", "5672"))
        self.vhost = vhost or os.environ.get("RABBITMQ_DEFAULT_VHOST", "/")
        self.client_id = client_id or os.environ.get("RABBITMQ_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("RABBITMQ_CLIENT_SECRET")
        self.token_url = token_url or os.environ.get("KEYCLOAK_TOKEN_URL")
        self._token_provider = self._build_token_provider()

        self._connection: pika.BlockingConnection | None = None
        self._channel = None

    def connect(self) -> None:
        """Open a connection and channel to the broker."""
        credentials = pika.PlainCredentials("oauth2", self._token_provider.get_token())
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
        )
        self._connection = pika.BlockingConnection(parameters)
        self._channel = self._connection.channel()

    def _build_token_provider(self) -> ClientCredentialsTokenProvider:
        """Build the required Keycloak client-credentials provider."""
        if not all((self.client_id, self.client_secret, self.token_url)):
            raise ValueError(
                "RABBITMQ_CLIENT_ID, RABBITMQ_CLIENT_SECRET and KEYCLOAK_TOKEN_URL "
                "must be configured together"
            )
        return ClientCredentialsTokenProvider(
            token_url=self.token_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def publish(
        self,
        queue: str,
        message: str | bytes,
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        """Publish to a pre-provisioned queue and require broker confirmation."""
        if self._channel is None:
            raise RuntimeError("No open connection: call connect() first")

        self._channel.confirm_delivery()
        self._channel.basic_publish(
            exchange="",
            routing_key=queue,
            body=message.encode("utf-8") if isinstance(message, str) else message,
            properties=pika.BasicProperties(delivery_mode=2, headers=headers),
            mandatory=True,
        )
        logger.info("Published message to queue %s", queue)

    def close(self) -> None:
        """Close the broker connection."""
        if self._connection is not None and self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None

    def __enter__(self) -> "Publisher":
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
