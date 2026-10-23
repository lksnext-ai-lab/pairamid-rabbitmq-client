#!/usr/bin/env python3
"""Consume and verify available partner executions using client 0.0.6."""

from __future__ import annotations

import argparse
import json
import logging
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from pairamid_rabbitmq_client import Delivery
from pairamid_rabbitmq_client.partner.adapter import PartnerAdapter
from pairamid_rabbitmq_client.partner.config import PartnerSettings

EXPECTED_VERSION = "0.0.6"


def _require_release_version() -> None:
    try:
        installed_version = version("pairamid-rabbitmq-client")
    except PackageNotFoundError as exc:
        raise SystemExit(
            "pairamid-rabbitmq-client is not installed in this Python environment"
        ) from exc
    if installed_version != EXPECTED_VERSION:
        raise SystemExit(
            f"Expected pairamid-rabbitmq-client {EXPECTED_VERSION}; "
            f"found {installed_version}"
        )


def _consume_available_executions(adapter: PartnerAdapter) -> None:
    matched_messages: list[tuple[str, dict[str, Any]]] = []

    def handle(delivery: Delivery) -> None:
        try:
            message = json.loads(delivery.body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            message = None
        if isinstance(message, dict) and delivery.source_queue is not None:
            matched_messages.append((delivery.source_queue, message))
        adapter.handle_execution(delivery)

    processed = adapter.client.poll_queues(
        adapter._queues_for_current_token,
        handle,
        max_messages_per_queue=1,
    )
    assignments = adapter._current_assignments
    print(f"Queues checked: {', '.join(assignments)}")
    if processed == 0:
        print("No pending executions")
        return

    for source_queue, message in matched_messages:
        execution_id = message.get("simulation_execution_id")
        assignment = assignments.get(source_queue)
        if (
            type(execution_id) is not int
            or execution_id <= 0
            or assignment is None
            or assignment[1] != message.get("simulation_key")
        ):
            raise RuntimeError("A consumed message has no valid execution ID or assigned queue")
        partner_id, simulation_key = assignment
        input_dir = adapter.settings.input_dir_for(simulation_key)
        state_dir = adapter.settings.state_dir_for(simulation_key)
        input_path = (
            input_dir
            / f"input-{partner_id}-{simulation_key}-{execution_id}.json"
        )
        state_path = (
            state_dir
            / f"input-{partner_id}-{simulation_key}-{execution_id}.json"
        )
        try:
            stored_input = json.loads(input_path.read_text(encoding="utf-8"))
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                f"Execution {execution_id} did not leave valid input and state files"
            ) from exc
        if stored_input != message.get("input"):
            raise RuntimeError("Materialized input does not match the API-published execution")
        if state.get("execution_message") != message:
            raise RuntimeError("Durable execution context does not match the API-published message")
        if state.get("input_downloaded_published") is not True:
            raise RuntimeError("input_downloaded was not confirmed by RabbitMQ")
        print(
            f"PASS: execution {execution_id} ({partner_id}/{simulation_key}) "
            "materialized, event confirmed and delivery acknowledged"
        )

    if not matched_messages:
        print(f"Processed {processed} invalid delivery/deliveries; inspect rejected/input/")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Consume available API-published partner executions with client 0.0.6."
    )
    parser.parse_args()

    _require_release_version()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    settings = PartnerSettings.from_env()
    adapter = PartnerAdapter(settings)
    _consume_available_executions(adapter)


if __name__ == "__main__":
    main()
