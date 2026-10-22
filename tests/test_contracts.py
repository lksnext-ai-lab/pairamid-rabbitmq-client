import copy
import json
import unittest
from pathlib import Path
from unittest.mock import patch

from pairamid_rabbitmq_client import ContractValidationError
from pairamid_rabbitmq_client.contracts import (
    build_failed_event,
    build_input_downloaded_event,
    build_result_payload,
    build_succeeded_event,
    validate_error_payload,
    validate_execution_message,
    validate_input_downloaded_event,
    validate_result_event,
    validate_result_payload,
)

EXAMPLES = Path(__file__).parent / "examples"


def example(name):
    return json.loads((EXAMPLES / (name + ".json")).read_text())


class ContractTests(unittest.TestCase):
    def test_official_examples_validate_without_network(self):
        cases = (
            ("simulation-execution-message", validate_execution_message),
            ("simulation-execution-input-downloaded-event", validate_input_downloaded_event),
            ("simulation-execution-result-succeeded", validate_result_event),
            ("simulation-execution-result-failed", validate_result_event),
            ("simulation-execution-result-payload", validate_result_payload),
            ("simulation-execution-error-payload", validate_error_payload),
        )
        with patch("socket.socket", side_effect=AssertionError("Network access forbidden")):
            for name, validator in cases:
                with self.subTest(example=name):
                    validator(example(name))

    def test_missing_unknown_uuid_datetime_version_and_pattern_are_rejected(self):
        for key, value in (
            ("message_id", "not-a-uuid"),
            ("requested_at", "2026-02-30T00:00:00Z"),
            ("requested_at", "2026-01-01"),
            ("simulation_key", "UPPER"),
            ("schema_version", "2.0"),
            ("unexpected", True),
        ):
            obj = example("simulation-execution-message")
            obj[key] = value
            with self.subTest(field=key, value=value):
                with self.assertRaises(ContractValidationError) as raised:
                    validate_execution_message(obj)
                self.assertTrue(raised.exception.errors)
                if key != "unexpected":
                    self.assertIn(f"$/{key}", str(raised.exception))
        obj = example("simulation-execution-message")
        del obj["input"]
        with self.assertRaises(ContractValidationError):
            validate_execution_message(obj)

    def test_all_schema_versions_and_terminal_rules(self):
        for name, validator in (
            ("simulation-execution-input-downloaded-event", validate_input_downloaded_event),
            ("simulation-execution-result-succeeded", validate_result_event),
            ("simulation-execution-result-payload", validate_result_payload),
            ("simulation-execution-error-payload", validate_error_payload),
        ):
            obj = example(name)
            obj["schema_version"] = "99"
            with (
                self.subTest(name=name),
                self.assertRaisesRegex(ContractValidationError, "Unsupported schema_version"),
            ):
                validator(obj)
        obj = example("simulation-execution-result-failed")
        obj["error"]["retryable"] = True
        with self.assertRaises(ContractValidationError):
            validate_result_event(obj)
        obj = example("simulation-execution-error-payload")
        obj["error"]["retryable"] = True
        with self.assertRaises(ContractValidationError):
            validate_error_payload(obj)
        obj = example("simulation-execution-result-succeeded")
        obj["error"] = example("simulation-execution-result-failed")["error"]
        with self.assertRaises(ContractValidationError):
            validate_result_event(obj)

    def test_nested_reference_formats_and_context_consistency(self):
        obj = example("simulation-execution-result-succeeded")
        obj["result"]["payload"]["run_id"] = "bad"
        with self.assertRaises(ContractValidationError):
            validate_result_event(obj)
        obj = example("simulation-execution-result-succeeded")
        obj["result"]["payload"]["simulation_key"] = "another"
        with self.assertRaisesRegex(ContractValidationError, "Must match the event context"):
            validate_result_event(obj)

    def test_builders_validate_and_deep_copy_without_transforming_output(self):
        execution = example("simulation-execution-message")
        output = {"status": "completed", "value": 42, "nested": [None, {"x": "ñ"}]}
        original = copy.deepcopy(output)
        payload = build_result_payload(execution, output)
        output["nested"].append("later")
        self.assertEqual(payload["output"], original)
        event = build_succeeded_event(payload)
        self.assertEqual(event["result"]["payload"], payload)
        payload["output"]["value"] = -1
        self.assertEqual(event["result"]["payload"]["output"], original)
        downloaded = build_input_downloaded_event(execution)
        self.assertEqual(
            downloaded["simulation_execution_id"], execution["simulation_execution_id"]
        )
        self.assertTrue(downloaded["downloaded_at"].endswith("Z"))
        failed = build_failed_event(example("simulation-execution-error-payload"))
        validate_result_event(failed)
        with self.assertRaises(ContractValidationError):
            build_input_downloaded_event(execution, event_id="")
        with self.assertRaises(ContractValidationError):
            build_succeeded_event(event["result"]["payload"], finished_at="yesterday")

    def test_physical_inputs_are_not_interpreted_but_must_be_json(self):
        obj = example("simulation-execution-message")
        obj["input"] = {"arbitrary_partner_field": [{"value": 17}]}
        validate_execution_message(obj)
        for value in (float("nan"), float("inf"), b"bytes", {1: "integer key"}, ("tuple",)):
            obj["input"] = {"value": value}
            with self.subTest(value=repr(value)), self.assertRaises(ContractValidationError):
                validate_execution_message(obj)


if __name__ == "__main__":
    unittest.main()
