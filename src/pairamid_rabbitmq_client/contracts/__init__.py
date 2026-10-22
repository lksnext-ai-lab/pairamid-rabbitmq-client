"""Offline validation and construction of the version 1.0 messaging contracts."""

from __future__ import annotations

import copy
import json
import math
from collections.abc import Mapping
from datetime import datetime, timezone
from functools import lru_cache
from importlib.resources import files
from typing import Any
from uuid import uuid4

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

from ..exceptions import ContractValidationError, ValidationIssue

SCHEMA_NAMES = (
    "simulation-execution-message",
    "simulation-execution-input-downloaded-event",
    "simulation-execution-result-event",
    "simulation-execution-result-payload",
    "simulation-execution-error-payload",
)
CONTEXT_FIELDS = (
    "simulation_execution_id",
    "run_id",
    "attempt_id",
    "workflow_instance_name",
    "simulation_key",
)


@lru_cache(maxsize=1)
def _validators() -> dict[str, Draft202012Validator]:
    schemas = {
        name: json.loads(files(__package__).joinpath(name + ".schema.json").read_text("utf-8"))
        for name in SCHEMA_NAMES
    }
    registry: Registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas.values()
    )
    for schema in schemas.values():
        Draft202012Validator.check_schema(schema)
    return {
        name: Draft202012Validator(schema, registry=registry, format_checker=FormatChecker())
        for name, schema in schemas.items()
    }


def _json_issues(value: Any, path: str = "$") -> list[ValidationIssue]:
    if value is None or isinstance(value, (str, bool, int)):
        return []
    if isinstance(value, float):
        return [] if math.isfinite(value) else [ValidationIssue(path, "Must be finite JSON")]
    if isinstance(value, dict):
        errors = []
        for key, item in value.items():
            if not isinstance(key, str):
                errors.append(ValidationIssue(path, "JSON object keys must be strings"))
            errors.extend(_json_issues(item, f"{path}/{key}"))
        return errors
    if isinstance(value, list):
        return [
            error
            for index, item in enumerate(value)
            for error in _json_issues(item, f"{path}/{index}")
        ]
    return [ValidationIssue(path, "Must be a JSON value")]


def _validate(name: str, obj: Any) -> None:
    issues = _json_issues(obj)
    if issues:
        raise ContractValidationError(issues)
    errors = sorted(
        _validators()[name].iter_errors(obj),
        key=lambda error: tuple(str(part) for part in error.absolute_path),
    )
    issues = [
        ValidationIssue(
            "$"
            + "".join(
                "/" + str(part).replace("~", "~0").replace("/", "~1")
                for part in error.absolute_path
            ),
            "Unsupported schema_version; expected '1.0'"
            if list(error.absolute_path)[-1:] == ["schema_version"]
            else error.message,
        )
        for error in errors
    ]
    if issues:
        raise ContractValidationError(issues)


def validate_execution_message(obj: Any) -> None:
    _validate("simulation-execution-message", obj)


def validate_input_downloaded_event(obj: Any) -> None:
    _validate("simulation-execution-input-downloaded-event", obj)


def validate_result_payload(obj: Any) -> None:
    _validate("simulation-execution-result-payload", obj)


def validate_error_payload(obj: Any) -> None:
    _validate("simulation-execution-error-payload", obj)


def validate_result_event(obj: Any) -> None:
    _validate("simulation-execution-result-event", obj)
    if obj["event_type"] == "simulation_execution.succeeded":
        payload = obj["result"]["payload"]
        errors = [
            ValidationIssue(f"$/result/payload/{key}", "Must match the event context")
            for key in CONTEXT_FIELDS
            if payload[key] != obj[key]
        ]
        if errors:
            raise ContractValidationError(errors)


def _context(obj: Mapping[str, Any]) -> dict[str, Any]:
    return {key: obj[key] for key in CONTEXT_FIELDS}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def build_input_downloaded_event(
    execution_message: Mapping[str, Any],
    *,
    event_id: str | None = None,
    downloaded_at: str | None = None,
) -> dict[str, Any]:
    validate_execution_message(execution_message)
    event = {
        **_context(execution_message),
        "schema_version": "1.0",
        "event_id": str(uuid4()) if event_id is None else event_id,
        "event_type": "simulation_execution.input_downloaded",
        "downloaded_at": _now() if downloaded_at is None else downloaded_at,
    }
    validate_input_downloaded_event(event)
    return event


def build_result_payload(
    execution_message: Mapping[str, Any],
    output: Any,
) -> dict[str, Any]:
    """Preserve partner output intact and copy the original execution context."""
    validate_execution_message(execution_message)
    payload = {
        **_context(execution_message),
        "schema_version": "1.0",
        "input": copy.deepcopy(execution_message["input"]),
        "output": copy.deepcopy(output),
    }
    validate_result_payload(payload)
    return payload


def build_succeeded_event(
    result_payload: Mapping[str, Any],
    *,
    event_id: str | None = None,
    finished_at: str | None = None,
) -> dict[str, Any]:
    validate_result_payload(result_payload)
    event = {
        **_context(result_payload),
        "schema_version": "1.0",
        "event_id": str(uuid4()) if event_id is None else event_id,
        "event_type": "simulation_execution.succeeded",
        "finished_at": _now() if finished_at is None else finished_at,
        "result": {"payload": copy.deepcopy(dict(result_payload))},
    }
    validate_result_event(event)
    return event


def build_failed_event(
    error_payload: Mapping[str, Any],
    *,
    event_id: str | None = None,
) -> dict[str, Any]:
    validate_error_payload(error_payload)
    event = {
        **_context(error_payload),
        "schema_version": "1.0",
        "event_id": str(uuid4()) if event_id is None else event_id,
        "event_type": "simulation_execution.failed",
        "finished_at": error_payload["finished_at"],
        "error": copy.deepcopy(error_payload["error"]),
    }
    validate_result_event(event)
    return event
