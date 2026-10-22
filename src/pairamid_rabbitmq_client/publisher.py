"""Internal RabbitMQ publishing and OAuth2 connection primitives."""

from __future__ import annotations

import logging
import os
import warnings
from collections.abc import Mapping
from typing import Any, TypeVar

import pika
import pika.exceptions

from .exceptions import PublishConnectionError, PublishNackedError, UnroutableMessageError
from .oauth2 import ClientCredentialsTokenProvider, token_username

logger = logging.getLogger(__name__)
_PublisherT = TypeVar("_PublisherT", bound="Publisher")


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
        *,
        user_id: str | None = None,
        set_user_id: bool = True,
    ) -> None:
        self.host = host or os.environ.get("RABBITMQ_HOST", "rabbitmq")
        self.port = port or int(os.environ.get("RABBITMQ_PORT", "5672"))
        self.vhost = vhost or os.environ.get("RABBITMQ_DEFAULT_VHOST", "/")
        self.client_id = client_id or os.environ.get("RABBITMQ_CLIENT_ID")
        self.client_secret = client_secret or os.environ.get("RABBITMQ_CLIENT_SECRET")
        self.token_url = token_url or os.environ.get("OAUTH2_TOKEN_URL")
        self._token_provider = self._build_token_provider()
        if user_id is not None and not user_id:
            raise ValueError("user_id must not be empty")
        self.user_id = user_id
        self.set_user_id = set_user_id
        self._connection_user_id: str | None = None

        self._connection: pika.BlockingConnection | None = None
        self._channel: pika.adapters.blocking_connection.BlockingChannel | None = None

    def connect(self) -> None:
        """Open a connection and channel to the broker."""
        if self._connection is not None and self._connection.is_open:
            raise RuntimeError("Connection already open: call close() before connect()")
        token = self._token_provider.get_token()
        self._connection_user_id = (
            self.user_id or token_username(token) if self.set_user_id else None
        )
        credentials = pika.PlainCredentials("oauth2", token)
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            virtual_host=self.vhost,
            credentials=credentials,
        )
        try:
            self._connection = pika.BlockingConnection(parameters)
            self._channel = self._connection.channel()
            self._channel.confirm_delivery()
        except pika.exceptions.AMQPConnectionError as exc:
            self.close()
            raise PublishConnectionError("Could not open an AMQP connection") from exc
        except pika.exceptions.AMQPError:
            self.close()
            raise

    def _build_token_provider(self) -> ClientCredentialsTokenProvider:
        """Build the required Keycloak client-credentials provider."""
        if not self.client_id or not self.client_secret or not self.token_url:
            raise ValueError(
                "RABBITMQ_CLIENT_ID, RABBITMQ_CLIENT_SECRET and OAUTH2_TOKEN_URL "
                "must be configured together"
            )
        return ClientCredentialsTokenProvider(
            token_url=self.token_url,
            client_id=self.client_id,
            client_secret=self.client_secret,
        )

    def publish(
        self,
        exchange: str | None = None,
        routing_key: str | bytes | None = None,
        body: str | bytes | Mapping[str, Any] | None = None,
        *,
        queue: str | None = None,
        message: str | bytes | None = None,
        headers: Mapping[str, Any] | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        content_type: str = "application/json",
        user_id: str | None = None,
        timestamp: int | None = None,
    ) -> None:
        """Publish without declaring resources; retain the deprecated queue API."""
        if queue is not None or message is not None:
            if queue is None and exchange is not None and routing_key is None and body is None:
                queue, exchange = exchange, None
            if exchange is not None or routing_key is not None or body is not None:
                raise TypeError("Do not combine queue/message with exchange/routing_key/body")
            if queue is None or message is None:
                raise TypeError("Both queue and message are required")
            exchange, routing_key, body = "", queue, message
            warnings.warn(
                "queue/message are deprecated; use exchange/routing_key/body",
                DeprecationWarning,
                stacklevel=2,
            )
        elif body is None or isinstance(body, Mapping):
            if exchange is None or routing_key is None:
                raise TypeError("exchange, routing_key and body are required")
            if isinstance(body, Mapping):
                if headers is not None:
                    raise TypeError("headers provided twice")
                headers = body
            exchange, routing_key, body = "", exchange, routing_key
            warnings.warn(
                "Positional queue publication is deprecated", DeprecationWarning, stacklevel=2
            )
        if not isinstance(exchange, str) or not isinstance(routing_key, str):
            raise TypeError("exchange and routing_key must be strings")
        if not isinstance(body, (str, bytes)):
            raise TypeError("body must be a string or bytes")
        if self._channel is None:
            raise RuntimeError("No open connection: call connect() first")

        try:
            self._channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body.encode("utf-8") if isinstance(body, str) else body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    headers=dict(headers) if headers is not None else None,
                    message_id=message_id,
                    correlation_id=correlation_id,
                    content_type=content_type,
                    user_id=user_id if user_id is not None else self._connection_user_id,
                    timestamp=timestamp,
                ),
                mandatory=True,
            )
        except pika.exceptions.UnroutableError as exc:
            raise UnroutableMessageError("Mandatory publication has no destination") from exc
        except pika.exceptions.NackError as exc:
            raise PublishNackedError("Broker negatively acknowledged publication") from exc
        except (
            pika.exceptions.AMQPConnectionError,
            pika.exceptions.ConnectionWrongStateError,
            pika.exceptions.ChannelWrongStateError,
        ) as exc:
            raise PublishConnectionError(
                "Connection lost; publication outcome may be unknown"
            ) from exc
        logger.info("Published message to exchange %s with routing key %s", exchange, routing_key)

    def close(self) -> None:
        """Close the broker connection."""
        if self._connection is not None and self._connection.is_open:
            self._connection.close()
        self._connection = None
        self._channel = None
        self._connection_user_id = None

    def __enter__(self: _PublisherT) -> _PublisherT:
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
