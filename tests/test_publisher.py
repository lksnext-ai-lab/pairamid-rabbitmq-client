import base64
import json
import unittest
from unittest.mock import MagicMock, patch

import pika
from test_contracts import example

from pairamid_rabbitmq_client import (
    PartnerClient,
    PlatformClient,
    PublishConnectionError,
    Publisher,
    PublishNackedError,
    UnroutableMessageError,
)
from pairamid_rabbitmq_client.contracts import (
    build_failed_event,
    build_input_downloaded_event,
    build_succeeded_event,
)
from pairamid_rabbitmq_client.oauth2 import token_username

SETTINGS = dict(client_id="test", client_secret="not-a-secret", token_url="https://local/token")


def jwt(claims):
    part = base64.urlsafe_b64encode(json.dumps(claims).encode()).decode().rstrip("=")
    return f"header.{part}.signature"


class PublisherTests(unittest.TestCase):
    def setUp(self):
        self.connection = MagicMock()
        self.channel = self.connection.channel.return_value
        self.connect_patch = patch(
            "pairamid_rabbitmq_client.publisher.pika.BlockingConnection",
            return_value=self.connection,
        )
        self.connect_patch.start()
        self.addCleanup(self.connect_patch.stop)
        self.token_patch = patch(
            "pairamid_rabbitmq_client.oauth2.ClientCredentialsTokenProvider.get_token",
            return_value=jwt({"preferred_username": "service-account-test", "client_id": "test"}),
        )
        self.token_patch.start()
        self.addCleanup(self.token_patch.stop)

    def test_confirm_once_per_channel_and_all_properties(self):
        with Publisher(**SETTINGS) as client:
            client.publish(
                "exchange",
                "route",
                '{"value":42}',
                headers={"trace": "one"},
                message_id="id",
                correlation_id="execution",
                timestamp=123,
            )
            client.publish("exchange", "route", b"{}")
            self.channel.confirm_delivery.assert_called_once_with()
            first = self.channel.basic_publish.call_args_list[0].kwargs
            self.assertEqual((first["exchange"], first["routing_key"]), ("exchange", "route"))
            self.assertEqual(first["body"], b'{"value":42}')
            self.assertTrue(first["mandatory"])
            props = first["properties"]
            self.assertEqual(props.delivery_mode, 2)
            self.assertEqual(props.user_id, "service-account-test")
            self.assertEqual(props.message_id, "id")
            self.assertEqual(props.correlation_id, "execution")
            self.assertEqual(props.timestamp, 123)
            self.assertEqual(props.content_type, "application/json")
            self.assertEqual(props.headers, {"trace": "one"})
            with self.assertRaises(RuntimeError):
                client.connect()
        self.connection.close.assert_called_once()

    def test_legacy_keyword_and_positional_calls(self):
        with Publisher(**SETTINGS) as client:
            for operation in (
                lambda: client.publish(queue="old", message="{}"),
                lambda: client.publish("old", message="{}"),
                lambda: client.publish("old", "{}"),
                lambda: client.publish("old", "{}", {"simulation_name": "lri"}),
            ):
                with self.assertWarns(DeprecationWarning):
                    operation()
                self.assertEqual(self.channel.basic_publish.call_args.kwargs["exchange"], "")
                self.assertEqual(self.channel.basic_publish.call_args.kwargs["routing_key"], "old")
            with self.assertRaises(TypeError):
                client.publish("x", "r", "{}", queue="old", message="{}")

    def test_publication_errors_and_access_errors_stay_distinct(self):
        with Publisher(**SETTINGS) as client:
            for cause, expected in (
                (pika.exceptions.UnroutableError([]), UnroutableMessageError),
                (pika.exceptions.NackError([]), PublishNackedError),
                (pika.exceptions.StreamLostError("closed"), PublishConnectionError),
                (pika.exceptions.ConnectionWrongStateError(), PublishConnectionError),
            ):
                self.channel.basic_publish.side_effect = cause
                with self.subTest(error=expected), self.assertRaises(expected) as raised:
                    client.publish("x", "r", b"{}")
                self.assertIs(raised.exception.__cause__, cause)
            self.channel.basic_publish.side_effect = pika.exceptions.ChannelClosedByBroker(
                403, "ACCESS_REFUSED"
            )
            with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
                client.publish("x", "r", b"{}")

    def test_identity_override_opt_out_and_invalid_tokens(self):
        self.assertEqual(token_username(jwt({"client_id": "client"})), "client")
        for token in ("opaque", "header.%%%.sig", jwt({}), jwt([])):
            with self.subTest(token=token), self.assertRaises(ValueError):
                token_username(token)
        with patch(
            "pairamid_rabbitmq_client.oauth2.ClientCredentialsTokenProvider.get_token",
            return_value="opaque",
        ):
            for settings, expected in (
                ({"user_id": "configured"}, "configured"),
                ({"set_user_id": False}, None),
            ):
                with Publisher(**SETTINGS, **settings) as client:
                    client.publish("x", "r", "{}")
                    props = self.channel.basic_publish.call_args.kwargs["properties"]
                    self.assertEqual(props.user_id, expected)

    def test_token_claims_are_available_from_the_active_client_credentials_token(self):
        claims = {
            "preferred_username": "service-account-irtjv-rabbitmq-client",
            "partner_id": "irtjv",
            "simulation_queues": ["pairamid.input.irtjv.lri"],
        }
        with patch(
            "pairamid_rabbitmq_client.oauth2.ClientCredentialsTokenProvider.get_token",
            return_value=jwt(claims),
        ):
            with Publisher(**SETTINGS) as client:
                self.assertEqual(client.get_token_claims()["simulation_queues"], [
                    "pairamid.input.irtjv.lri"
                ])

    def test_failed_channel_initialization_closes_connection(self):
        self.channel.confirm_delivery.side_effect = pika.exceptions.ChannelClosedByBroker(403, "no")
        client = Publisher(**SETTINGS)
        with self.assertRaises(pika.exceptions.ChannelClosedByBroker):
            client.connect()
        self.connection.close.assert_called_once()
        with self.assertRaises(RuntimeError):
            client.publish("x", "r", "{}")

    def test_execution_routing_validation_and_configurable_exchange(self):
        obj = example("simulation-execution-message")
        with PlatformClient(**SETTINGS, execution_exchange="custom") as client:
            client.publish_execution("irtjv", obj["simulation_key"], obj, obj["message_id"])
            call = self.channel.basic_publish.call_args.kwargs
            self.assertEqual(call["exchange"], "custom")
            self.assertEqual(call["routing_key"], "irtjv." + obj["simulation_key"])
            self.assertEqual(json.loads(call["body"]), obj)
            self.assertEqual(call["properties"].correlation_id, str(obj["simulation_execution_id"]))
            for partner, simulation, message_id in (
                ("irtjv.other", obj["simulation_key"], obj["message_id"]),
                ("irtjv", "other", obj["message_id"]),
                ("irtjv", obj["simulation_key"], "wrong"),
            ):
                with self.assertRaises(ValueError):
                    client.publish_execution(partner, simulation, obj, message_id)

    def test_partner_event_routing_all_types_and_no_legacy_results_dependency(self):
        events = (
            build_input_downloaded_event(example("simulation-execution-message")),
            build_succeeded_event(example("simulation-execution-result-payload")),
            build_failed_event(example("simulation-execution-error-payload")),
        )
        with PartnerClient("irtjv", **SETTINGS, events_exchange="events") as client:
            self.assertEqual(client.execution_queue("lri"), "pairamid.input.irtjv.lri")
            for event in events:
                client.publish_event(event["event_type"], event, event["event_id"])
                call = self.channel.basic_publish.call_args.kwargs
                suffix = "input_downloaded" if "downloaded" in event["event_type"] else "result"
                self.assertEqual(
                    (call["exchange"], call["routing_key"]), ("events", "irtjv." + suffix)
                )
                self.assertEqual(json.loads(call["body"]), event)
                self.assertEqual(
                    call["properties"].correlation_id, str(event["simulation_execution_id"])
                )
            with self.assertRaises(ValueError):
                client.publish_event("unknown", {}, "id")
            with self.assertRaises(ValueError):
                client.publish_event(events[0]["event_type"], events[0], "mismatch")
        for partner in ("", "upperCASE", "irtjv.other", "a*", "x/y"):
            with self.subTest(partner=partner), self.assertRaises(ValueError):
                PartnerClient(partner, **SETTINGS)

    def test_partner_event_can_route_from_token_assignment(self):
        event = build_input_downloaded_event(example("simulation-execution-message"))
        with PartnerClient(**SETTINGS, events_exchange="events") as client:
            client.publish_event(
                event["event_type"],
                event,
                event["event_id"],
                partner_id="irtjv",
            )
            self.assertEqual(
                self.channel.basic_publish.call_args.kwargs["routing_key"],
                "irtjv.input_downloaded",
            )
            with self.assertRaisesRegex(ValueError, "partner_id"):
                client.publish_event(
                    event["event_type"],
                    event,
                    event["event_id"],
                )


if __name__ == "__main__":
    unittest.main()
