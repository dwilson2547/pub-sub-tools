#!/usr/bin/env python3
"""
Lightweight HTTP backend for browsing pub-sub messages.

Endpoints:
- GET  /                                         (management UI)
- GET  /api/v1/systems
- POST /api/v1/connections
- GET  /api/v1/connections
- DELETE /api/v1/connections/{id}
- GET  /api/v1/connections/{id}/topics
- POST /api/v1/connections/{id}/messages/pull
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import uuid
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional

_UI_FILE = os.path.join(os.path.dirname(__file__), "browser_ui.html")


class ConnectorError(RuntimeError):
    """Raised for connector-level operational issues."""


class BaseBrowserConnector:
    def __init__(self, config: Dict[str, Any]) -> None:
        self.config = config

    def connect(self) -> None:  # pragma: no cover - interface default
        return None

    def close(self) -> None:  # pragma: no cover - interface default
        return None

    def list_topics(self) -> List[str]:  # pragma: no cover - interface default
        raise NotImplementedError

    def pull_messages(
        self,
        topic: str,
        max_messages: int,
        mode: str,
        timeout_seconds: float,
    ) -> List[Dict[str, Any]]:  # pragma: no cover - interface default
        raise NotImplementedError


class InMemoryBrowserConnector(BaseBrowserConnector):
    """Test-friendly in-memory connector used by integration tests and local demos."""

    def __init__(self, config: Dict[str, Any]) -> None:
        super().__init__(config)
        self._lock = threading.Lock()
        self._topics = {
            topic: list(messages)
            for topic, messages in config.get("topics", {}).items()
        }

    def list_topics(self) -> List[str]:
        return sorted(self._topics.keys())

    def pull_messages(
        self,
        topic: str,
        max_messages: int,
        mode: str,
        timeout_seconds: float,
    ) -> List[Dict[str, Any]]:
        del timeout_seconds
        if topic not in self._topics:
            raise ConnectorError(f"Unknown topic: {topic}")

        with self._lock:
            available = self._topics[topic]
            selected = available[:max_messages]
            if mode == "consume":
                del available[: len(selected)]

        return [
            {
                "id": str(i),
                "payload": msg,
                "metadata": {"topic": topic},
            }
            for i, msg in enumerate(selected)
        ]


class KafkaBrowserConnector(BaseBrowserConnector):
    """Kafka connector that supports topic listing and pull (peek/consume)."""

    def list_topics(self) -> List[str]:
        from confluent_kafka import Consumer  # type: ignore[import]

        bootstrap = self.config["bootstrap_servers"]
        consumer = Consumer(
            {
                "bootstrap.servers": bootstrap,
                "group.id": self.config.get("group_id", "pub-sub-tools-browser"),
                "enable.auto.commit": False,
                "auto.offset.reset": self.config.get("auto_offset_reset", "earliest"),
            }
        )
        try:
            md = consumer.list_topics(timeout=5.0)
            return sorted(name for name in md.topics.keys() if not name.startswith("__"))
        finally:
            consumer.close()

    def pull_messages(
        self,
        topic: str,
        max_messages: int,
        mode: str,
        timeout_seconds: float,
    ) -> List[Dict[str, Any]]:
        from confluent_kafka import Consumer, KafkaError  # type: ignore[import]

        group_id = self.config.get("group_id", "pub-sub-tools-browser")
        if mode == "peek":
            group_id = f"{group_id}-peek-{uuid.uuid4().hex[:8]}"

        consumer = Consumer(
            {
                "bootstrap.servers": self.config["bootstrap_servers"],
                "group.id": group_id,
                "enable.auto.commit": False,
                "auto.offset.reset": self.config.get("auto_offset_reset", "earliest"),
            }
        )

        try:
            consumer.subscribe([topic])
            messages: List[Dict[str, Any]] = []
            remaining_polls = max(1, int(timeout_seconds / 0.5))

            while len(messages) < max_messages and remaining_polls > 0:
                batch = consumer.consume(
                    num_messages=max_messages - len(messages),
                    timeout=0.5,
                )
                remaining_polls -= 1
                if not batch:
                    continue

                for msg in batch:
                    err = msg.error()
                    if err is not None:
                        if err.code() == KafkaError._PARTITION_EOF:
                            continue
                        raise ConnectorError(f"Kafka consumer error: {err}")

                    raw = msg.value()
                    payload = raw.decode("utf-8", errors="replace") if raw else ""
                    messages.append(
                        {
                            "id": f"{msg.topic()}:{msg.partition()}:{msg.offset()}",
                            "payload": payload,
                            "metadata": {
                                "topic": msg.topic(),
                                "partition": msg.partition(),
                                "offset": msg.offset(),
                                "timestamp": msg.timestamp()[1],
                                "key": (
                                    msg.key().decode("utf-8", errors="replace")
                                    if msg.key()
                                    else None
                                ),
                            },
                        }
                    )

            if mode == "consume" and messages:
                consumer.commit(asynchronous=False)

            return messages
        finally:
            consumer.close()


def build_connector(system: str, config: Dict[str, Any]) -> BaseBrowserConnector:
    key = system.lower().replace("-", "").replace("_", "")
    if key in {"memory", "inmemory"}:
        return InMemoryBrowserConnector(config)
    if key == "kafka":
        return KafkaBrowserConnector(config)
    raise ConnectorError(
        f"Unsupported system '{system}' for browser backend. Supported: memory, kafka"
    )


@dataclass
class Connection:
    id: str
    system: str
    connector: BaseBrowserConnector


class ConnectionManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._connections: Dict[str, Connection] = {}

    def create(self, system: str, config: Dict[str, Any]) -> Connection:
        connector = build_connector(system, config)
        connector.connect()
        connection = Connection(id=uuid.uuid4().hex, system=system, connector=connector)
        with self._lock:
            self._connections[connection.id] = connection
        return connection

    def list(self) -> List[Dict[str, str]]:
        with self._lock:
            return [
                {"id": c.id, "system": c.system}
                for c in self._connections.values()
            ]

    def get(self, connection_id: str) -> Connection:
        with self._lock:
            connection = self._connections.get(connection_id)
        if not connection:
            raise KeyError(connection_id)
        return connection

    def delete(self, connection_id: str) -> None:
        with self._lock:
            connection = self._connections.pop(connection_id, None)
        if not connection:
            raise KeyError(connection_id)
        connection.connector.close()


class BrowserAPIHandler(BaseHTTPRequestHandler):
    manager = ConnectionManager()

    def do_OPTIONS(self) -> None:  # noqa: N802
        self.send_response(HTTPStatus.NO_CONTENT)
        self._cors_headers()
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        if self.path in ("/", ""):
            self._serve_ui()
            return

        if self.path == "/api/v1/systems":
            self._json(HTTPStatus.OK, {"systems": ["memory", "kafka"]})
            return

        if self.path == "/api/v1/connections":
            self._json(HTTPStatus.OK, {"connections": self.manager.list()})
            return

        parts = self.path.strip("/").split("/")
        if len(parts) == 5 and parts[:3] == ["api", "v1", "connections"] and parts[4] == "topics":
            conn_id = parts[3]
            try:
                conn = self.manager.get(conn_id)
                self._json(HTTPStatus.OK, {"topics": conn.connector.list_topics()})
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND, {"error": "connection not found"})
            except ConnectorError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self) -> None:  # noqa: N802
        if self.path == "/api/v1/connections":
            body = self._read_json_body()
            if body is None:
                return
            system = body.get("system")
            config = body.get("config", {})
            if not system:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "system is required"})
                return
            try:
                connection = self.manager.create(system, config)
                self._json(
                    HTTPStatus.CREATED,
                    {"id": connection.id, "system": connection.system},
                )
            except ConnectorError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        parts = self.path.strip("/").split("/")
        if len(parts) == 6 and parts[:3] == ["api", "v1", "connections"] and parts[4] == "messages" and parts[5] == "pull":
            body = self._read_json_body()
            if body is None:
                return
            topic = body.get("topic")
            max_messages = int(body.get("max_messages", 10))
            mode = body.get("mode", "peek")
            timeout_seconds = float(body.get("timeout_seconds", 5.0))

            if not topic:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "topic is required"})
                return
            if mode not in {"peek", "consume"}:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "mode must be 'peek' or 'consume'"})
                return
            if max_messages < 1:
                self._json(HTTPStatus.BAD_REQUEST, {"error": "max_messages must be >= 1"})
                return

            conn_id = parts[3]
            try:
                conn = self.manager.get(conn_id)
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND, {"error": "connection not found"})
                return

            try:
                messages = conn.connector.pull_messages(
                    topic=topic,
                    max_messages=max_messages,
                    mode=mode,
                    timeout_seconds=timeout_seconds,
                )
                self._json(
                    HTTPStatus.OK,
                    {
                        "topic": topic,
                        "mode": mode,
                        "messages": messages,
                        "count": len(messages),
                    },
                )
            except ConnectorError as exc:
                self._json(HTTPStatus.BAD_REQUEST, {"error": str(exc)})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_DELETE(self) -> None:  # noqa: N802
        parts = self.path.strip("/").split("/")
        if len(parts) == 4 and parts[:3] == ["api", "v1", "connections"]:
            conn_id = parts[3]
            try:
                self.manager.delete(conn_id)
                self._json(HTTPStatus.OK, {"status": "deleted"})
            except KeyError:
                self._json(HTTPStatus.NOT_FOUND, {"error": "connection not found"})
            return

        self._json(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def log_message(self, format: str, *args: Any) -> None:  # noqa: A003
        return

    def _serve_ui(self) -> None:
        try:
            with open(_UI_FILE, "rb") as f:
                data = f.read()
        except OSError:
            self._json(HTTPStatus.NOT_FOUND, {"error": f"UI file not found: {_UI_FILE}"})
            return
        self.send_response(HTTPStatus.OK)
        self._cors_headers()
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _cors_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _read_json_body(self) -> Optional[Dict[str, Any]]:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid Content-Length"})
            return None

        raw = self.rfile.read(content_length) if content_length else b"{}"
        try:
            return json.loads(raw.decode("utf-8"))
        except json.JSONDecodeError:
            self._json(HTTPStatus.BAD_REQUEST, {"error": "invalid JSON body"})
            return None

    def _json(self, status: HTTPStatus, payload: Dict[str, Any]) -> None:
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self._cors_headers()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)


def run_server(host: str = "127.0.0.1", port: int = 8081) -> None:
    server = ThreadingHTTPServer((host, port), BrowserAPIHandler)
    print(f"message-browser-api listening on http://{host}:{port}")
    print(f"  UI: http://{host}:{port}/")
    server.serve_forever()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run pub-sub message browser backend API")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8081)
    args = parser.parse_args()
    run_server(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
