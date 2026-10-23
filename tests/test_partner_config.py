import tempfile
import unittest
from pathlib import Path

from pairamid_rabbitmq_client.partner.config import (
    PartnerSettings,
    parse_queue_assignments,
)


def make_environment(simulation_dir: str = "./simulations") -> dict[str, str]:
    return {
        "RABBITMQ_CLIENT_ID": "irtjv-rabbitmq-client",
        "RABBITMQ_CLIENT_SECRET": "test-secret",
        "OAUTH2_TOKEN_URL": "https://keycloak.example.test/token",
        "PARTNER_ID": "irtjv",
        "SIMULATION_DIR": simulation_dir,
    }


class PartnerSettingsTests(unittest.TestCase):
    def test_defaults_and_simulation_directories(self) -> None:
        settings = PartnerSettings.from_env(make_environment())

        self.assertEqual(settings.host, "rabbitmq")
        self.assertEqual(settings.port, 5672)
        self.assertEqual(settings.vhost, "/")
        self.assertEqual(settings.poll_interval_seconds, 30)
        self.assertEqual(settings.expected_partner_id, "irtjv")
        self.assertEqual(settings.input_dir_for("lri"), Path("./simulations/lri/input"))
        self.assertEqual(settings.results_dir_for("lri"), Path("./simulations/lri/results"))

    def test_partner_id_is_an_optional_token_consistency_guard(self) -> None:
        environment = make_environment()
        del environment["PARTNER_ID"]

        settings = PartnerSettings.from_env(environment)

        self.assertIsNone(settings.expected_partner_id)

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

    def test_invalid_partner_id_guard_is_rejected(self) -> None:
        environment = make_environment()
        environment["PARTNER_ID"] = "invalid.partner"

        with self.assertRaisesRegex(ValueError, "valid lowercase partner"):
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

            settings.prepare_directories(("lri", "xray"))

            for relative_path in (
                "lri/input",
                "lri/input/.tmp",
                "lri/results",
                "lri/rejected/input",
                "lri/rejected/results",
                "lri/state",
                "xray/input",
                "xray/results",
            ):
                self.assertTrue((settings.simulation_dir / relative_path).is_dir())

    def test_queue_assignments_are_parsed_from_token_claims(self) -> None:
        assignments = parse_queue_assignments(
            {
                "partner_id": "irtjv",
                "simulation_queues": [
                    "pairamid.input.irtjv.lri",
                    "pairamid.input.irtjv.xray",
                ],
            }
        )

        self.assertEqual(
            assignments,
            {
                "pairamid.input.irtjv.lri": ("irtjv", "lri"),
                "pairamid.input.irtjv.xray": ("irtjv", "xray"),
            },
        )

    def test_queue_assignment_rejects_bad_claim_shapes_and_foreign_queues(self) -> None:
        invalid_claims = (
            {},
            {"partner_id": "irtjv", "simulation_queues": "pairamid.input.irtjv.lri"},
            {"partner_id": "irtjv", "simulation_queues": []},
            {"partner_id": "irtjv", "simulation_queues": ["pairamid.input.irtjv.*"]},
            {
                "partner_id": "irtjv",
                "simulation_queues": [
                    "pairamid.input.irtjv.lri",
                    "pairamid.input.irtjv.lri",
                ],
            },
            {"partner_id": "irtjv", "simulation_queues": ["pairamid.input.other.lri"]},
            {"partner_id": "irtjv", "simulation_queues": ["pairamid.input.irtjv./x"]},
        )
        for claims in invalid_claims:
            with self.subTest(claims=claims), self.assertRaises(ValueError):
                parse_queue_assignments(claims)

    def test_queue_assignment_checks_optional_local_partner_id(self) -> None:
        with self.assertRaisesRegex(ValueError, "does not match"):
            parse_queue_assignments(
                {
                    "partner_id": "irtjv",
                    "simulation_queues": ["pairamid.input.irtjv.lri"],
                },
                expected_partner_id="other",
            )


if __name__ == "__main__":
    unittest.main()
