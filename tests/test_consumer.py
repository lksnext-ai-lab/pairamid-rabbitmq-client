import os
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pika

from pairamid_rabbitmq_client import Consumer, Delivery

SETTINGS = dict(client_id="test", client_secret="not-a-secret", token_url="https://local/token")


def make_delivery_frame(
    delivery_tag: int,
    *,
    redelivered: bool = False,
    routing_key: str = "pairamid.input.irtjv.lri",
) -> SimpleNamespace:
    return SimpleNamespace(
        delivery_tag=delivery_tag,
        redelivered=redelivered,
        routing_key=routing_key,
    )


class DeliveryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.channel = MagicMock()
        self.delivery = Delivery(
            channel=self.channel,
            delivery_tag=4,
            body=b'{"input":{"value":1}}',
            properties=pika.BasicProperties(headers={"source": "test"}),
            headers={"source": "test"},
            redelivered=True,
            routing_key="pairamid.input.irtjv.lri",
        )

    def test_ack_is_sent_once(self) -> None:
        self.delivery.ack()

        self.channel.basic_ack.assert_called_once_with(delivery_tag=4)
        self.assertTrue(self.delivery.responded)
        with self.assertRaisesRegex(RuntimeError, "already been"):
            self.delivery.ack()

    def test_reject_and_nack_use_explicit_requeue_policy(self) -> None:
        self.delivery.reject()
        self.channel.basic_reject.assert_called_once_with(delivery_tag=4, requeue=False)

        another_delivery = Delivery(
            channel=self.channel,
            delivery_tag=5,
            body=b"{}",
            properties=None,
            headers=None,
            redelivered=False,
            routing_key="pairamid.input.irtjv.lri",
        )
        another_delivery.nack(requeue=True)
        self.channel.basic_nack.assert_called_once_with(delivery_tag=5, requeue=True)

    def test_delivery_exposes_message_metadata(self) -> None:
        self.assertEqual(self.delivery.body, b'{"input":{"value":1}}')
        self.assertEqual(self.delivery.headers, {"source": "test"})
        self.assertTrue(self.delivery.redelivered)
        self.assertEqual(self.delivery.routing_key, "pairamid.input.irtjv.lri")
        self.assertEqual(self.delivery.delivery_tag, 4)


class ConsumerPollingTests(unittest.TestCase):
    def make_consumer(self, poll_interval_seconds: float = 0.01) -> Consumer:
        return Consumer(
            **SETTINGS,
            simulation_input_dir="unused",
            poll_interval_seconds=poll_interval_seconds,
        )

    def test_poll_drains_queue_and_closes_connection(self) -> None:
        consumer = self.make_consumer()
        channel = MagicMock()
        channel.basic_get.side_effect = [
            (
                make_delivery_frame(1, routing_key="pairamid.input.irtjv.lri"),
                pika.BasicProperties(headers={"index": 1}),
                b'{"input":1}',
            ),
            (
                make_delivery_frame(2, redelivered=True, routing_key="pairamid.input.irtjv.lri"),
                pika.BasicProperties(headers={"index": 2}),
                b'{"input":2}',
            ),
            (None, None, None),
        ]
        consumer._channel = channel
        seen: list[Delivery] = []

        def acknowledge(item: Delivery) -> None:
            seen.append(item)
            item.ack()

        with patch.object(consumer, "connect"), patch.object(consumer, "close") as close:
            processed = consumer.poll("pairamid.input.irtjv.lri", acknowledge)

        self.assertEqual(processed, 2)
        self.assertEqual([item.body for item in seen], [b'{"input":1}', b'{"input":2}'])
        self.assertEqual(seen[0].headers, {"index": 1})
        self.assertTrue(seen[1].redelivered)
        self.assertEqual(channel.basic_get.call_count, 3)
        self.assertEqual(channel.basic_ack.call_count, 2)
        channel.basic_nack.assert_not_called()
        close.assert_called_once()

    def test_poll_respects_max_messages(self) -> None:
        consumer = self.make_consumer()
        channel = MagicMock()
        channel.basic_get.return_value = (
            make_delivery_frame(1),
            pika.BasicProperties(),
            b"{}",
        )
        consumer._channel = channel

        with patch.object(consumer, "connect"), patch.object(consumer, "close"):
            processed = consumer.poll("pairamid.input.irtjv.lri", lambda item: item.ack(), max_messages=2)

        self.assertEqual(processed, 2)
        self.assertEqual(channel.basic_get.call_count, 2)

    def test_poll_rejects_invalid_max_messages_before_connecting(self) -> None:
        consumer = self.make_consumer()
        with patch.object(consumer, "connect") as connect:
            with self.assertRaisesRegex(ValueError, "max_messages"):
                consumer.poll("pairamid.input.irtjv.lri", lambda item: item.ack(), max_messages=0)
        connect.assert_not_called()

    def test_consumer_uses_input_subdirectory_under_simulation_dir(self) -> None:
        with patch.dict(os.environ, {"SIMULATION_DIR": "/tmp/irtjv-data"}):
            consumer = Consumer(**SETTINGS)

        self.assertEqual(consumer.simulation_input_dir, Path("/tmp/irtjv-data/input"))

    def test_poll_forever_rejects_invalid_configuration_before_looping(self) -> None:
        consumer = self.make_consumer()
        with patch.object(consumer, "poll") as poll:
            with self.assertRaisesRegex(ValueError, "max_messages"):
                consumer.poll_forever(
                    "pairamid.input.irtjv.lri",
                    lambda item: item.ack(),
                    max_messages=0,
                )
        poll.assert_not_called()

    def test_handler_exception_nacks_and_propagates(self) -> None:
        consumer = self.make_consumer()
        channel = MagicMock()
        channel.basic_get.return_value = (
            make_delivery_frame(8),
            pika.BasicProperties(),
            b"{}",
        )
        consumer._channel = channel

        def fail(_delivery: Delivery) -> None:
            raise ValueError("handler failed")

        with (
            patch.object(consumer, "connect"),
            patch.object(consumer, "close"),
            self.assertLogs("pairamid_rabbitmq_client.consumer", level="ERROR"),
        ):
            with self.assertRaisesRegex(ValueError, "handler failed"):
                consumer.poll("pairamid.input.irtjv.lri", fail)

        channel.basic_nack.assert_called_once_with(delivery_tag=8, requeue=True)
        channel.basic_ack.assert_not_called()

    def test_handler_must_respond_to_each_delivery(self) -> None:
        consumer = self.make_consumer()
        channel = MagicMock()
        channel.basic_get.return_value = (
            make_delivery_frame(9),
            pika.BasicProperties(),
            b"{}",
        )
        consumer._channel = channel

        with (
            patch.object(consumer, "connect"),
            patch.object(consumer, "close"),
            self.assertLogs("pairamid_rabbitmq_client.consumer", level="ERROR"),
        ):
            with self.assertRaisesRegex(RuntimeError, "without ack"):
                consumer.poll("pairamid.input.irtjv.lri", lambda _delivery: None)

        channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=True)

    def test_stop_interrupts_poll_interval(self) -> None:
        consumer = self.make_consumer(poll_interval_seconds=30)
        poll_started = threading.Event()
        with patch.object(consumer, "poll", side_effect=lambda *_args, **_kwargs: poll_started.set()):
            thread = threading.Thread(
                target=consumer.poll_forever,
                args=("pairamid.input.irtjv.lri", lambda delivery: delivery.ack()),
            )
            thread.start()
            self.assertTrue(poll_started.wait(timeout=2))
            consumer.stop()
            thread.join(timeout=2)

        self.assertFalse(thread.is_alive())

    def test_poll_forever_logs_cycle_error_and_continues(self) -> None:
        consumer = self.make_consumer()
        poll_count = 0

        def fail_once_then_stop(*_args, **_kwargs) -> None:
            nonlocal poll_count
            poll_count += 1
            if poll_count == 1:
                raise RuntimeError("temporary failure")
            consumer.stop()

        with (
            patch.object(consumer, "poll", side_effect=fail_once_then_stop),
            self.assertLogs("pairamid_rabbitmq_client.consumer", level="ERROR"),
        ):
            consumer.poll_forever("pairamid.input.irtjv.lri", lambda delivery: delivery.ack())

        self.assertEqual(poll_count, 2)

    def test_legacy_poll_forever_remains_available(self) -> None:
        consumer = self.make_consumer()
        with patch.object(consumer, "poll_once", side_effect=lambda _queue: consumer.stop()) as poll:
            consumer.poll_forever("pairamid.input.irtjv.lri")
        poll.assert_called_once_with("pairamid.input.irtjv.lri")


if __name__ == "__main__":
    unittest.main()
