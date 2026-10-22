import tempfile
import unittest
from pathlib import Path

from pairamid_rabbitmq_client.partner.config import PartnerSettings


def make_environment(simulation_dir: str = "./simulations") -> dict[str, str]:
    return {
        "RABBITMQ_CLIENT_ID": "irtjv-rabbitmq-client",
        "RABBITMQ_CLIENT_SECRET": "test-secret",
        "OAUTH2_TOKEN_URL": "https://keycloak.example.test/token",
        "PARTNER_ID": "irtjv",
        "SIMULATION_DIR": simulation_dir,
    }


class PartnerSettingsTests(unittest.TestCase):
    def test_defaults_and_single_queue_configuration(self) -> None:
        settings = PartnerSettings.from_env(make_environment())

        self.assertEqual(settings.host, "rabbitmq")
        self.assertEqual(settings.port, 5672)
        self.assertEqual(settings.vhost, "/")
        self.assertEqual(settings.poll_interval_seconds, 30)
        self.assertEqual(settings.queue, "pairamid.input.irtjv.lri")
        self.assertEqual(settings.routing_key, "irtjv.lri")
        self.assertEqual(settings.input_dir, Path("./simulations/input"))
        self.assertEqual(settings.results_dir, Path("./simulations/results"))

    def test_custom_connection_and_polling_values(self) -> None:
        environment = make_environment("/tmp/irtjv-data")
        environment.update(
            {
                "RABBITMQ_HOST": "broker.internal",
                "RABBITMQ_PORT": "5673",
                "RABBITMQ_DEFAULT_VHOST": "/partner",
                "CONSUMER_POLL_INTERVAL_SECONDS": "2.5",
            }
        )

        settings = PartnerSettings.from_env(environment)

        self.assertEqual(settings.host, "broker.internal")
        self.assertEqual(settings.port, 5673)
        self.assertEqual(settings.vhost, "/partner")
        self.assertEqual(settings.poll_interval_seconds, 2.5)
        self.assertEqual(settings.simulation_dir, Path("/tmp/irtjv-data"))

    def test_required_credentials_are_validated(self) -> None:
        environment = make_environment()
        del environment["RABBITMQ_CLIENT_SECRET"]

        with self.assertRaisesRegex(ValueError, "RABBITMQ_CLIENT_SECRET"):
            PartnerSettings.from_env(environment)

    def test_only_irtjv_is_supported_for_this_adapter(self) -> None:
        environment = make_environment()
        environment["PARTNER_ID"] = "another-partner"

        with self.assertRaisesRegex(ValueError, "only PARTNER_ID=irtjv"):
            PartnerSettings.from_env(environment)

    def test_invalid_port_and_poll_interval_are_rejected(self) -> None:
        environment = make_environment()
        environment["RABBITMQ_PORT"] = "invalid"
        with self.assertRaisesRegex(ValueError, "RABBITMQ_PORT"):
            PartnerSettings.from_env(environment)

        environment["RABBITMQ_PORT"] = "5672"
        environment["CONSUMER_POLL_INTERVAL_SECONDS"] = "inf"
        with self.assertRaisesRegex(ValueError, "CONSUMER_POLL_INTERVAL_SECONDS"):
            PartnerSettings.from_env(environment)

    def test_simulation_directory_layout_is_created(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            settings = PartnerSettings.from_env(
                make_environment(str(Path(temporary_directory) / "simulations"))
            )

            settings.prepare_directories()

            for relative_path in (
                "input",
                "input/.tmp",
                "results",
                "rejected/input",
                "rejected/results",
                "state",
            ):
                self.assertTrue((settings.simulation_dir / relative_path).is_dir())


if __name__ == "__main__":
    unittest.main()
