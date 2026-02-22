from __future__ import annotations

import json
import os
import sys
import threading
import types
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from browser_api import (
    BrowserAPIHandler,
    ConnectionManager,
    InMemoryBrowserConnector,
    KafkaBrowserConnector,
)


class TestInMemoryBrowserConnector:
    def test_peek_does_not_remove_messages(self):
        connector = InMemoryBrowserConnector({"topics": {"orders": ["a", "b", "c"]}})

        first = connector.pull_messages("orders", max_messages=2, mode="peek", timeout_seconds=1)
        second = connector.pull_messages("orders", max_messages=3, mode="peek", timeout_seconds=1)

        assert [m["payload"] for m in first] == ["a", "b"]
        assert [m["payload"] for m in second] == ["a", "b", "c"]

    def test_consume_removes_messages(self):
        connector = InMemoryBrowserConnector({"topics": {"orders": ["a", "b", "c"]}})

        consumed = connector.pull_messages("orders", max_messages=2, mode="consume", timeout_seconds=1)
        remaining = connector.pull_messages("orders", max_messages=10, mode="peek", timeout_seconds=1)

        assert [m["payload"] for m in consumed] == ["a", "b"]
        assert [m["payload"] for m in remaining] == ["c"]


class TestConnectionManager:
    def test_create_list_get_delete(self):
        manager = ConnectionManager()
        conn = manager.create("memory", {"topics": {"t1": ["x"]}})

        listed = manager.list()
        assert listed == [{"id": conn.id, "system": "memory"}]
        assert manager.get(conn.id).id == conn.id

        manager.delete(conn.id)
        with pytest.raises(KeyError):
            manager.get(conn.id)


class TestKafkaBrowserConnector:
    def test_consume_mode_commits_offsets(self):
        connector = KafkaBrowserConnector({"bootstrap_servers": "b:9092", "group_id": "g"})

        msg = MagicMock()
        msg.error.return_value = None
        msg.value.return_value = b"payload"
        msg.topic.return_value = "orders"
        msg.partition.return_value = 0
        msg.offset.return_value = 12
        msg.timestamp.return_value = (0, 123456)
        msg.key.return_value = b"k"

        fake_consumer = MagicMock()
        fake_consumer.consume.side_effect = [[msg], []]

        kafka_stub = types.ModuleType("confluent_kafka")
        kafka_stub.Consumer = MagicMock(return_value=fake_consumer)
        kafka_stub.KafkaError = MagicMock(_PARTITION_EOF=-1)

        with patch.dict(sys.modules, {"confluent_kafka": kafka_stub}):
            messages = connector.pull_messages("orders", max_messages=1, mode="consume", timeout_seconds=1)

        assert len(messages) == 1
        fake_consumer.commit.assert_called_once_with(asynchronous=False)


@pytest.fixture
def api_server():
    BrowserAPIHandler.manager = ConnectionManager()
    server = ThreadingHTTPServer(("127.0.0.1", 0), BrowserAPIHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base = f"http://{host}:{port}"
    try:
        yield base
    finally:
        server.shutdown()
        thread.join(timeout=3)
        server.server_close()


def _request_json(url: str, method: str = "GET", body: dict | None = None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url=url, method=method, data=data, headers=headers)
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.getcode(), json.loads(resp.read().decode("utf-8"))


class TestBrowserAPIIntegration:
    def test_create_connection_list_topics_and_pull_modes(self, api_server):
        code, created = _request_json(
            f"{api_server}/api/v1/connections",
            method="POST",
            body={
                "system": "memory",
                "config": {"topics": {"orders": ["m1", "m2", "m3"]}},
            },
        )
        assert code == 201
        conn_id = created["id"]

        code, topics = _request_json(f"{api_server}/api/v1/connections/{conn_id}/topics")
        assert code == 200
        assert topics == {"topics": ["orders"]}

        code, peeked = _request_json(
            f"{api_server}/api/v1/connections/{conn_id}/messages/pull",
            method="POST",
            body={"topic": "orders", "max_messages": 2, "mode": "peek"},
        )
        assert code == 200
        assert [m["payload"] for m in peeked["messages"]] == ["m1", "m2"]

        code, consumed = _request_json(
            f"{api_server}/api/v1/connections/{conn_id}/messages/pull",
            method="POST",
            body={"topic": "orders", "max_messages": 2, "mode": "consume"},
        )
        assert code == 200
        assert [m["payload"] for m in consumed["messages"]] == ["m1", "m2"]

        code, remaining = _request_json(
            f"{api_server}/api/v1/connections/{conn_id}/messages/pull",
            method="POST",
            body={"topic": "orders", "max_messages": 10, "mode": "peek"},
        )
        assert code == 200
        assert [m["payload"] for m in remaining["messages"]] == ["m3"]

    def test_bad_request_for_invalid_mode(self, api_server):
        code, created = _request_json(
            f"{api_server}/api/v1/connections",
            method="POST",
            body={"system": "memory", "config": {"topics": {"orders": ["m1"]}}},
        )
        assert code == 201
        conn_id = created["id"]

        req = urllib.request.Request(
            url=f"{api_server}/api/v1/connections/{conn_id}/messages/pull",
            method="POST",
            data=json.dumps({"topic": "orders", "mode": "bad"}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
        )
        with pytest.raises(urllib.error.HTTPError) as exc:
            urllib.request.urlopen(req, timeout=5)

        body = json.loads(exc.value.read().decode("utf-8"))
        assert exc.value.code == 400
        assert "mode must be" in body["error"]
