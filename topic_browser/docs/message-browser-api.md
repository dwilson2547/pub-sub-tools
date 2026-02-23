# Message Browser Backend API

The backend service is implemented in `topic_browser/browser_api.py`.

Run it locally:

```bash
python topic_browser/browser_api.py --host 127.0.0.1 --port 8081
```

Base URL: `http://127.0.0.1:8081/api/v1`

## Supported systems

`GET /systems`

Response:

```json
{
  "systems": ["memory", "kafka"]
}
```

- `memory`: in-memory connector for local development/testing.
- `kafka`: real Kafka connector (requires `confluent-kafka` and valid bootstrap config).

## Create a connection

`POST /connections`

Request body:

```json
{
  "system": "kafka",
  "config": {
    "bootstrap_servers": "localhost:9092",
    "group_id": "message-browser",
    "auto_offset_reset": "earliest"
  }
}
```

Response (`201 Created`):

```json
{
  "id": "<connection-id>",
  "system": "kafka"
}
```

## List active connections

`GET /connections`

Response:

```json
{
  "connections": [
    {"id": "<connection-id>", "system": "kafka"}
  ]
}
```

## Delete a connection

`DELETE /connections/{connectionId}`

Response:

```json
{"status": "deleted"}
```

## List topics

`GET /connections/{connectionId}/topics`

Response:

```json
{
  "topics": ["orders", "invoices"]
}
```

## Pull messages

`POST /connections/{connectionId}/messages/pull`

Request body:

```json
{
  "topic": "orders",
  "max_messages": 25,
  "mode": "peek",
  "timeout_seconds": 5
}
```

Parameters:

- `topic` (required): topic name
- `max_messages` (optional, default `10`): max messages to return
- `mode` (optional, default `peek`):
  - `peek` = read without consuming from the active feed
  - `consume` = read and commit offsets / remove from feed
- `timeout_seconds` (optional, default `5.0`): max read time window

Response:

```json
{
  "topic": "orders",
  "mode": "peek",
  "count": 2,
  "messages": [
    {
      "id": "orders:0:1001",
      "payload": "{\"order_id\":123}",
      "metadata": {
        "topic": "orders",
        "partition": 0,
        "offset": 1001,
        "timestamp": 1730000000000,
        "key": "123"
      }
    }
  ]
}
```

## Error format

All errors return JSON:

```json
{"error": "..."}
```

Common status codes: `400`, `404`.
