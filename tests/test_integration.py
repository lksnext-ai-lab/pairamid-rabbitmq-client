"""Opt-in AMQP tests: provision topology first and ensure queues are idle."""

import json
import os
import unittest
from uuid import uuid4

from test_contracts import example

from pairamid_rabbitmq_client import PartnerClient, PlatformClient, UnroutableMessageError
from pairamid_rabbitmq_client.contracts import (
    build_failed_event,
    build_input_downloaded_event,
    build_result_payload,
    build_succeeded_event,
)


@unittest.skipUnless(
    os.environ.get("RABBITMQ_INTEGRATION") == "1",
    "Set RABBITMQ_INTEGRATION=1 with service credentials and idle provisioned queues",
)
class RabbitMQIntegrationTests(unittest.TestCase):
    def test_execution_events_properties_and_unroutable_message(self):
        common = dict(
            host=os.environ.get("RABBITMQ_HOST", "localhost"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            vhost="/",
            token_url=os.environ["OAUTH2_TOKEN_URL"],
        )
        execution = example("simulation-execution-message")
        execution["message_id"] = str(uuid4())
        execution["simulation_execution_id"] = str(uuid4())
        execution["simulation_key"] = "lri"
        with (
            PlatformClient(
                **common,
                client_id="pairamid-api-rabbitmq",
                client_secret=os.environ["API_RABBITMQ_CLIENT_SECRET"],
            ) as platform,
            PartnerClient(
                "irtjv",
                **common,
                client_id="irtjv-rabbitmq-client",
                client_secret=os.environ["PARTNER_RABBITMQ_CLIENT_SECRET"],
            ) as partner,
        ):
            platform.publish_execution("irtjv", "lri", execution, execution["message_id"])
            self.receive(
                partner,
                "pairamid.input.irtjv.lri",
                execution,
                "service-account-pairamid-api-rabbitmq",
                execution["message_id"],
            )
            output = {"status": "completed", "value": 42, "nested": [None, {"kept": True}]}
            succeeded = build_succeeded_event(build_result_payload(execution, output))
            failed_payload = example("simulation-execution-error-payload")
            for key in (
                "simulation_execution_id",
                "run_id",
                "attempt_id",
                "workflow_instance_name",
                "simulation_key",
            ):
                failed_payload[key] = execution[key]
            for event, queue in (
                (build_input_downloaded_event(execution), "pairamid.events.input_downloaded"),
                (succeeded, "pairamid.events.result"),
                (build_failed_event(failed_payload), "pairamid.events.result"),
            ):
                partner.publish_event(event["event_type"], event, event["event_id"])
                self.receive(
                    platform,
                    queue,
                    event,
                    "service-account-irtjv-rabbitmq-client",
                    event["event_id"],
                )
            self.assertEqual(succeeded["result"]["payload"]["output"], output)
            with self.assertRaises(UnroutableMessageError):
                platform.publish(
                    "simulation_execution_exchange",
                    "no-binding-" + str(uuid4()),
                    "{}",
                    message_id=str(uuid4()),
                )

    def receive(self, client, queue, expected, username, message_id):
        channel = client._channel
        method, props, body = channel.basic_get(queue=queue, auto_ack=False)
        self.assertIsNotNone(method, "Expected confirmed message is missing")
        if props.message_id != message_id:
            channel.basic_nack(method.delivery_tag, requeue=True)
            self.fail("Unexpected message requeued; stop tests and inspect queue without purging")
        try:
            self.assertEqual(json.loads(body), expected)
            self.assertEqual(props.correlation_id, expected["simulation_execution_id"])
            self.assertEqual(props.user_id, username)
            self.assertEqual(props.delivery_mode, 2)
            self.assertEqual(props.content_type, "application/json")
            self.assertIsInstance(props.timestamp, int)
        finally:
            channel.basic_ack(method.delivery_tag)


if __name__ == "__main__":
    unittest.main()
