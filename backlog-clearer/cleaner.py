#!/usr/bin/env python3
"""
backlog-clearer — drain pub-sub backlogs as fast as possible.

Supported systems: kafka, pulsar, streamnative, iggy, googlepubsub, eventhubs

Usage examples
--------------

Kafka::

    python cleaner.py kafka \\
        --bootstrap-servers broker1:9092,broker2:9092 \\
        --topic my-topic \\
        --group-id clearer-group \\
        --workers 20 \\
        --batch-size 2000

Google Pub/Sub::

    python cleaner.py googlepubsub \\
        --project-id my-gcp-project \\
        --subscription-id my-subscription \\
        --workers 10

Azure Event Hubs::

    python cleaner.py eventhubs \\
        --connection-string "Endpoint=sb://..." \\
        --eventhub-name my-hub \\
        --workers 16

Apache Pulsar / StreamNative::

    python cleaner.py pulsar \\
        --service-url pulsar+ssl://my-cluster:6651 \\
        --topic persistent://tenant/ns/topic \\
        --token "$(cat token.jwt)" \\
        --workers 10

Iggy::

    python cleaner.py iggy \\
        --host 127.0.0.1 --port 8090 \\
        --stream-id 1 --topic-id 1 \\
        --partition-ids 1,2,3,4 \\
        --workers 8
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import sys
from typing import Any, Dict, List, Optional

from clearers import SUPPORTED_SYSTEMS, get_clearer
from clearers.base import BaseClearer

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("cleaner")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="cleaner",
        description=(
            "Drain pub-sub backlogs at maximum speed. "
            "Consumes and acknowledges every message without inspecting payloads."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "system",
        choices=SUPPORTED_SYSTEMS,
        metavar="system",
        help=f"Pub-sub system to drain. One of: {', '.join(SUPPORTED_SYSTEMS)}",
    )

    # Common options
    common = parser.add_argument_group("common options")
    common.add_argument(
        "--workers",
        type=int,
        default=10,
        metavar="N",
        help="Number of concurrent consumer workers (default: 10).",
    )
    common.add_argument(
        "--batch-size",
        type=int,
        default=1000,
        metavar="N",
        help="Messages to fetch per worker per batch (default: 1 000).",
    )
    common.add_argument(
        "--stop-when-empty",
        action="store_true",
        default=False,
        help="Stop automatically when no messages remain.",
    )
    common.add_argument(
        "--config-json",
        metavar="FILE",
        help=(
            "Path to a JSON file containing system configuration. "
            "CLI flags override keys from this file."
        ),
    )

    # Kafka
    kafka = parser.add_argument_group("kafka / streamnative options")
    kafka.add_argument("--bootstrap-servers", metavar="BROKERS")
    kafka.add_argument("--topic", metavar="TOPIC")
    kafka.add_argument("--group-id", default="backlog-clearer")
    kafka.add_argument(
        "--auto-offset-reset", choices=["earliest", "latest"], default="earliest"
    )
    kafka.add_argument(
        "--security-protocol",
        choices=["PLAINTEXT", "SSL", "SASL_PLAINTEXT", "SASL_SSL"],
    )
    kafka.add_argument("--sasl-mechanism")
    kafka.add_argument("--sasl-username")
    kafka.add_argument("--sasl-password")
    kafka.add_argument("--ssl-ca-location", metavar="PATH")

    # Pulsar / StreamNative
    pulsar = parser.add_argument_group("pulsar / streamnative options")
    pulsar.add_argument("--service-url", metavar="URL")
    pulsar.add_argument("--subscription", default="backlog-clearer")
    pulsar.add_argument("--token", metavar="JWT")
    pulsar.add_argument("--tls-trust-certs-file", metavar="PATH")

    # Iggy
    iggy = parser.add_argument_group("iggy options")
    iggy.add_argument("--host", default="127.0.0.1")
    iggy.add_argument("--port", type=int, default=8090)
    iggy.add_argument("--stream-id", type=int, metavar="ID")
    iggy.add_argument("--topic-id", type=int, metavar="ID")
    iggy.add_argument(
        "--partition-ids",
        metavar="IDS",
        help="Comma-separated list of partition IDs, e.g. 1,2,3.",
    )
    iggy.add_argument("--username", default="iggy")
    iggy.add_argument("--password", default="iggy")
    iggy.add_argument("--consumer-name", default="backlog-clearer")

    # Google Pub/Sub
    gpubsub = parser.add_argument_group("google pub/sub options")
    gpubsub.add_argument("--project-id", metavar="PROJECT")
    gpubsub.add_argument("--subscription-id", metavar="SUB")
    gpubsub.add_argument("--credentials-file", metavar="PATH")

    # Azure Event Hubs
    eventhubs = parser.add_argument_group("azure event hubs options")
    eventhubs.add_argument("--connection-string", metavar="STR")
    eventhubs.add_argument("--eventhub-name", metavar="NAME")
    eventhubs.add_argument("--consumer-group", default="$Default")
    eventhubs.add_argument(
        "--eventhub-partition-ids",
        metavar="IDS",
        help="Comma-separated partition IDs (fetched automatically if omitted).",
    )
    eventhubs.add_argument(
        "--starting-position",
        default="-1",
        help='"-1" = beginning (default), "@latest" = end.',
    )

    return parser


def _build_config(args: argparse.Namespace) -> Dict[str, Any]:
    """Merge JSON config file (if provided) with CLI arguments."""
    cfg: Dict[str, Any] = {}

    if args.config_json:
        with open(args.config_json) as fh:
            cfg.update(json.load(fh))

    system = args.system

    # Kafka / StreamNative
    if system in ("kafka", "streamnative") or args.bootstrap_servers:
        _set_if(cfg, "bootstrap_servers", args.bootstrap_servers)
        _set_if(cfg, "topic", args.topic)
        _set_if(cfg, "group_id", args.group_id)
        _set_if(cfg, "auto_offset_reset", args.auto_offset_reset)
        _set_if(cfg, "security_protocol", args.security_protocol)
        _set_if(cfg, "sasl_mechanism", args.sasl_mechanism)
        _set_if(cfg, "sasl_username", args.sasl_username)
        _set_if(cfg, "sasl_password", args.sasl_password)
        _set_if(cfg, "ssl_ca_location", args.ssl_ca_location)

    # Pulsar / StreamNative
    if system in ("pulsar", "streamnative") or args.service_url:
        _set_if(cfg, "service_url", args.service_url)
        _set_if(cfg, "topic", args.topic)
        _set_if(cfg, "subscription", args.subscription)
        _set_if(cfg, "token", args.token)
        _set_if(cfg, "tls_trust_certs_file", args.tls_trust_certs_file)

    # Iggy
    if system == "iggy":
        _set_if(cfg, "host", args.host)
        _set_if(cfg, "port", args.port)
        _set_if(cfg, "stream_id", args.stream_id)
        _set_if(cfg, "topic_id", args.topic_id)
        _set_if(cfg, "username", args.username)
        _set_if(cfg, "password", args.password)
        _set_if(cfg, "consumer_name", args.consumer_name)
        if args.partition_ids:
            cfg["partition_ids"] = [
                int(p.strip()) for p in args.partition_ids.split(",")
            ]

    # Google Pub/Sub
    if system == "googlepubsub":
        _set_if(cfg, "project_id", args.project_id)
        _set_if(cfg, "subscription_id", args.subscription_id)
        _set_if(cfg, "credentials_file", args.credentials_file)

    # Azure Event Hubs
    if system == "eventhubs":
        _set_if(cfg, "connection_string", args.connection_string)
        _set_if(cfg, "eventhub_name", args.eventhub_name)
        _set_if(cfg, "consumer_group", args.consumer_group)
        _set_if(cfg, "starting_position", args.starting_position)
        if args.eventhub_partition_ids:
            cfg["partition_ids"] = [
                p.strip() for p in args.eventhub_partition_ids.split(",")
            ]

    cfg["num_workers"] = args.workers
    return cfg


def _set_if(cfg: dict, key: str, value: Any) -> None:
    if value is not None:
        cfg[key] = value


# ---------------------------------------------------------------------------
# Main async runner
# ---------------------------------------------------------------------------

async def run(system: str, config: dict, workers: int, batch_size: int, stop_when_empty: bool) -> None:
    clearer: BaseClearer = get_clearer(system, config)

    # Graceful shutdown on SIGINT / SIGTERM
    loop = asyncio.get_event_loop()

    def _shutdown_handler(*_: Any) -> None:
        logger.info("Shutdown signal received — stopping after current batch …")
        clearer.stop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _shutdown_handler)
        except (NotImplementedError, RuntimeError):
            # Windows or non-main thread
            signal.signal(sig, _shutdown_handler)

    logger.info(
        "Starting %s backlog clearer — workers: %d, batch_size: %d",
        system,
        workers,
        batch_size,
    )

    await clearer.connect()
    try:
        stats = await clearer.clear(
            num_workers=workers,
            batch_size=batch_size,
            stop_when_empty=stop_when_empty,
        )
    finally:
        await clearer.disconnect()

    logger.info("Done. Final stats: %s", stats)


def main(argv: Optional[List[str]] = None) -> None:
    parser = _build_parser()
    args = parser.parse_args(argv)
    config = _build_config(args)

    asyncio.run(
        run(
            system=args.system,
            config=config,
            workers=args.workers,
            batch_size=args.batch_size,
            stop_when_empty=args.stop_when_empty,
        )
    )


if __name__ == "__main__":
    main()
