#!/usr/bin/env python3
"""Publish one partner result file using its durable input context."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from pairamid_rabbitmq_client.partner.adapter import PartnerAdapter
from pairamid_rabbitmq_client.partner.config import PartnerSettings


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Publish one results-{partner}-{simulation}-{execution_id}.json file."
    )
    parser.add_argument("result_file", type=Path, help="JSON file under the simulation results directory")
    arguments = parser.parse_args()
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    settings = PartnerSettings.from_env()
    adapter = PartnerAdapter(settings)
    published = adapter.publish_result_file(arguments.result_file)
    if published:
        print(f"Result published: {arguments.result_file.name}")
    else:
        print(f"Result was already published: {arguments.result_file.name}")


if __name__ == "__main__":
    main()
