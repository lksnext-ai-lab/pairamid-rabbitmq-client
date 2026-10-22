import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from pairamid_rabbitmq_client import PartnerClient, PlatformClient
from pairamid_rabbitmq_client.oauth2 import ClientCredentialsTokenProvider

SETTINGS = dict(client_id="test", client_secret="not-a-secret", token_url="https://local/token")


class CompatibilityTests(unittest.TestCase):
    def test_legacy_input_and_result_publication(self):
        with patch.object(PlatformClient, "publish") as publish:
            client = PlatformClient(**SETTINGS)
            with self.assertWarns(DeprecationWarning):
                client.publish_input("irtjv", "{}", "LRi_1")
            publish.assert_called_once_with(
                queue="pairamid.input.irtjv",
                message="{}",
                headers={"simulation_name": "LRi_1"},
            )
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / "result.json"
            file.write_text('{"value":42}')
            with (
                patch.object(PartnerClient, "connect"),
                patch.object(PartnerClient, "close"),
                patch.object(PartnerClient, "publish") as publish,
            ):
                client = PartnerClient("irtjv", **SETTINGS, results_queue="legacy-custom")
                with self.assertWarns(DeprecationWarning):
                    client.publish_result(file)
                publish.assert_called_once_with(queue="legacy-custom", message=b'{"value":42}')

    def test_legacy_polling_stores_original_bytes_before_ack_and_keeps_unique_suffix(self):
        with tempfile.TemporaryDirectory() as directory:
            client = PartnerClient("irtjv", **SETTINGS, simulation_input_dir=directory)
            channel = MagicMock()
            method = MagicMock(delivery_tag=7)
            body = b'{"arbitrary":"input"}'
            channel.basic_get.return_value = (
                method,
                MagicMock(headers={"simulation_name": "LRi_1"}),
                body,
            )
            client._channel = channel
            acked = []
            channel.basic_ack.side_effect = lambda **kwargs: acked.append(
                (kwargs["delivery_tag"], sorted(p.name for p in Path(directory).glob("*.json")))
            )
            with patch.object(client, "connect"), patch.object(client, "close"):
                first = client.poll_once(client.input_queue)
                second = client.poll_once(client.input_queue)
            self.assertEqual(first.read_bytes(), body)
            self.assertEqual(second.read_bytes(), body)
            self.assertEqual(first.name, "LRi_1.json")
            self.assertEqual(second.name, "LRi_1-2.json")
            self.assertEqual(acked[0], (7, ["LRi_1.json"]))
            channel.basic_nack.assert_not_called()

    def test_legacy_polling_empty_and_invalid_json_do_not_ack(self):
        client = PartnerClient("irtjv", **SETTINGS)
        channel = MagicMock()
        client._channel = channel
        with patch.object(client, "connect"), patch.object(client, "close") as close:
            channel.basic_get.return_value = (None, None, None)
            self.assertIsNone(client.poll_once(client.input_queue))
            channel.basic_get.return_value = (
                MagicMock(delivery_tag=9),
                MagicMock(headers={"simulation_name": "lri"}),
                b"bad",
            )
            with self.assertLogs("pairamid_rabbitmq_client.consumer", level="WARNING"):
                self.assertIsNone(client.poll_once(client.input_queue))
            channel.basic_nack.assert_called_once_with(delivery_tag=9, requeue=False)
            channel.basic_ack.assert_not_called()
            self.assertEqual(close.call_count, 2)

    def test_token_provider_cache_and_refresh_are_preserved(self):
        provider = ClientCredentialsTokenProvider(
            **{
                "token_url": "https://local/token",
                "client_id": "test",
                "client_secret": "test",
            }
        )
        responses = []
        for token in ("first", "second"):
            response = MagicMock()
            response.__enter__.return_value.read.return_value = json.dumps(
                {"access_token": token, "expires_in": 60}
            )
            responses.append(response)
        with patch("pairamid_rabbitmq_client.oauth2.urlopen", side_effect=responses) as request:
            with patch("pairamid_rabbitmq_client.oauth2.time.time", return_value=100):
                self.assertEqual(provider.get_token(), "first")
            with patch("pairamid_rabbitmq_client.oauth2.time.time", return_value=120):
                self.assertEqual(provider.get_token(), "first")
            with patch("pairamid_rabbitmq_client.oauth2.time.time", return_value=131):
                self.assertEqual(provider.get_token(), "second")
            self.assertEqual(request.call_count, 2)


if __name__ == "__main__":
    unittest.main()
