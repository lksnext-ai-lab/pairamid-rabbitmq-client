"""Durable input materialization and rejected-message archival."""

from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

from pairamid_rabbitmq_client.contracts import (
    build_input_downloaded_event,
    validate_input_downloaded_event,
)
from pairamid_rabbitmq_client.exceptions import ContractValidationError

_PARTNER_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,99}$")
_SIMULATION_KEY_RE = re.compile(r"^[a-z][a-z0-9_.-]{0,99}$")


class InputConflictError(RuntimeError):
    """An execution ID already refers to different input or durable context."""


def materialize_input(
    execution_message: Mapping[str, Any],
    input_dir: Path,
    partner_id: str,
) -> tuple[Path, bool]:
    """Atomically persist only the execution input and deduplicate identical retries."""
    if not isinstance(partner_id, str) or not _PARTNER_ID_RE.fullmatch(partner_id):
        raise ValueError("partner_id is invalid")
    simulation_key = execution_message.get("simulation_key")
    if not isinstance(simulation_key, str) or not _SIMULATION_KEY_RE.fullmatch(simulation_key):
        raise ValueError("simulation_key is invalid")
    execution_id = execution_message.get("simulation_execution_id")
    if type(execution_id) is not int or execution_id <= 0:
        raise ValueError("simulation_execution_id must be a positive integer")

    try:
        input_bytes = json.dumps(
            execution_message["input"],
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("execution input must be a JSON value") from exc

    input_dir.mkdir(parents=True, exist_ok=True)
    temporary_dir = input_dir / ".tmp"
    temporary_dir.mkdir(parents=True, exist_ok=True)
    target = input_dir / f"input-{partner_id}-{simulation_key}-{execution_id}.json"
    temporary_path = _write_temporary(input_bytes, temporary_dir, target.name)

    try:
        try:
            os.link(temporary_path, target)
            created = True
        except FileExistsError:
            if target.is_symlink() or not target.is_file():
                raise InputConflictError(f"Input target is not a regular file: {target}")
            existing_digest = hashlib.sha256(target.read_bytes()).digest()
            if existing_digest != hashlib.sha256(input_bytes).digest():
                raise InputConflictError(f"Conflicting input already exists: {target.name}")
            created = False
        _fsync_directory(input_dir)
        return target, created
    finally:
        temporary_path.unlink(missing_ok=True)


class InputStateStore:
    """Persist execution context and the stable event used across redeliveries."""

    def __init__(self, state_dir: Path, partner_id: str) -> None:
        self.state_dir = state_dir
        self.partner_id = partner_id

    def load_or_create(self, execution_message: Mapping[str, Any], input_path: Path) -> dict[str, Any]:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        execution_id = execution_message["simulation_execution_id"]
        simulation_key = execution_message["simulation_key"]
        state_path = self.state_dir / (
            f"input-{self.partner_id}-{simulation_key}-{execution_id}.json"
        )
        input_digest = hashlib.sha256(input_path.read_bytes()).hexdigest()

        if state_path.exists():
            if state_path.is_symlink() or not state_path.is_file():
                raise InputConflictError(f"Input state is not a regular file: {state_path}")
            try:
                state = json.loads(state_path.read_text(encoding="utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise InputConflictError(f"Input state is corrupt: {state_path.name}") from exc
            if (
                not isinstance(state, dict)
                or state.get("execution_message") != dict(execution_message)
                or state.get("input_sha256") != input_digest
                or not isinstance(state.get("input_downloaded_published"), bool)
            ):
                raise InputConflictError(f"Input state conflicts with delivery: {state_path.name}")
            event = state.get("input_downloaded_event")
            if not isinstance(event, dict):
                raise InputConflictError(f"Input state has no event: {state_path.name}")
            try:
                validate_input_downloaded_event(event)
            except ContractValidationError as exc:
                raise InputConflictError(
                    f"Input state has an invalid event: {state_path.name}"
                ) from exc
            if any(
                event.get(key) != execution_message.get(key)
                for key in (
                    "simulation_execution_id",
                    "run_id",
                    "attempt_id",
                    "simulation_key",
                )
            ):
                raise InputConflictError(
                    f"Input state event does not match delivery: {state_path.name}"
                )
            return state

        state = {
            "execution_message": dict(execution_message),
            "input_sha256": input_digest,
            "input_downloaded_event": build_input_downloaded_event(execution_message),
            "input_downloaded_published": False,
        }
        self._write(state_path, state)
        return state

    def mark_published(self, execution_message: Mapping[str, Any], state: dict[str, Any]) -> None:
        execution_id = execution_message["simulation_execution_id"]
        simulation_key = execution_message["simulation_key"]
        state_path = self.state_dir / (
            f"input-{self.partner_id}-{simulation_key}-{execution_id}.json"
        )
        state["input_downloaded_published"] = True
        self._write(state_path, state)

    @staticmethod
    def _write(path: Path, state: Mapping[str, Any]) -> None:
        serialized = json.dumps(
            state, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True
        ).encode("utf-8")
        temporary_path = _write_temporary(serialized, path.parent, path.name)
        try:
            os.replace(temporary_path, path)
            _fsync_directory(path.parent)
        finally:
            temporary_path.unlink(missing_ok=True)


def archive_rejected_message(
    body: bytes,
    rejected_dir: Path,
    reason: str,
    message_id: str | None = None,
) -> Path:
    """Durably retain the original body and rejection reason before rejecting it."""
    rejected_dir.mkdir(parents=True, exist_ok=True)
    archive_id = uuid4().hex
    body_path = rejected_dir / f"rejected-{archive_id}.body"
    metadata_path = rejected_dir / f"rejected-{archive_id}.json"
    temporary_body = _write_temporary(body, rejected_dir, body_path.name)
    try:
        os.link(temporary_body, body_path)
        _fsync_directory(rejected_dir)
    finally:
        temporary_body.unlink(missing_ok=True)

    metadata = {
        "archive_id": archive_id,
        "message_id": message_id,
        "reason": reason,
        "rejected_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    temporary_metadata = _write_temporary(
        json.dumps(metadata, ensure_ascii=False, allow_nan=False).encode("utf-8"),
        rejected_dir,
        metadata_path.name,
    )
    try:
        os.link(temporary_metadata, metadata_path)
        _fsync_directory(rejected_dir)
    finally:
        temporary_metadata.unlink(missing_ok=True)
    return body_path


def _write_temporary(content: bytes, directory: Path, prefix: str) -> Path:
    descriptor, name = tempfile.mkstemp(dir=directory, prefix=f".{prefix}.")
    path = Path(name)
    completed = False
    try:
        with os.fdopen(descriptor, "wb") as temporary_file:
            temporary_file.write(content)
            temporary_file.flush()
            os.fsync(temporary_file.fileno())
        completed = True
    finally:
        if not completed:
            path.unlink(missing_ok=True)
    return path


def _fsync_directory(directory: Path) -> None:
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
