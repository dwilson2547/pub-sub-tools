# backlog-clearer

A high-throughput pub-sub backlog clearing tool.  Connects to a pub-sub
system and consumes messages as fast as possible — without inspecting
payloads — while acknowledging every message so the broker can discard it.

Designed to hit **50 000+ messages per second** from a single node by
combining multiple concurrent workers, large batch fetches, and asynchronous
offset commits.

## Supported systems

| System | Key |
|---|---|
| Apache Kafka | `kafka` |
| Apache Pulsar | `pulsar` |
| StreamNative (managed Pulsar) | `streamnative` |
| Iggy | `iggy` |
| Google Cloud Pub/Sub | `googlepubsub` |
| Azure Event Hubs | `eventhubs` |

## Installation

```bash
pip install -r requirements.txt
```

## Quick start

### Kafka

```bash
python cleaner.py kafka \
  --bootstrap-servers broker1:9092,broker2:9092 \
  --topic my-topic \
  --group-id backlog-clearer \
  --workers 20 \
  --batch-size 2000 \
  --stop-when-empty
```

### Apache Pulsar

```bash
python cleaner.py pulsar \
  --service-url pulsar://localhost:6650 \
  --topic persistent://public/default/my-topic \
  --subscription backlog-clearer \
  --workers 10 \
  --batch-size 1000
```

### StreamNative (managed Pulsar with JWT auth)

```bash
python cleaner.py streamnative \
  --service-url pulsar+ssl://my-cluster.streamnative.cloud:6651 \
  --topic persistent://my-tenant/my-ns/my-topic \
  --token "$(cat token.jwt)" \
  --workers 10
```

### Iggy

```bash
python cleaner.py iggy \
  --host 127.0.0.1 \
  --port 8090 \
  --stream-id 1 \
  --topic-id 1 \
  --partition-ids 1,2,3,4 \
  --workers 8 \
  --batch-size 500 \
  --stop-when-empty
```

### Google Cloud Pub/Sub

```bash
# Uses Application Default Credentials (gcloud auth application-default login)
python cleaner.py googlepubsub \
  --project-id my-gcp-project \
  --subscription-id my-subscription \
  --workers 10

# Or with a service-account key file
python cleaner.py googlepubsub \
  --project-id my-gcp-project \
  --subscription-id my-subscription \
  --credentials-file /path/to/key.json \
  --workers 10
```

### Azure Event Hubs

```bash
python cleaner.py eventhubs \
  --connection-string "Endpoint=sb://my-ns.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=…" \
  --eventhub-name my-hub \
  --workers 16 \
  --batch-size 1000 \
  --stop-when-empty
```

## Configuration via JSON file

Any option can be placed in a JSON file and loaded with `--config-json`.
CLI flags override JSON values.

```json
{
  "bootstrap_servers": "broker1:9092,broker2:9092",
  "topic": "my-topic",
  "group_id": "backlog-clearer",
  "security.protocol": "SASL_SSL",
  "sasl.mechanism": "PLAIN",
  "sasl.username": "user",
  "sasl.password": "secret"
}
```

```bash
python cleaner.py kafka --config-json kafka-prod.json --workers 20
```

## Performance notes

| Technique | Benefit |
|---|---|
| `confluent-kafka` (librdkafka) | Native C library; far higher throughput than `kafka-python` |
| Multiple workers per run | Saturates available partitions / subscription capacity |
| Large batch fetches | Amortises round-trip overhead over many messages |
| Async commit / cumulative ack | Does not block the receive loop |
| Thread-pool executors for sync libs | asyncio event loop never blocks |
| Async workers for async libs (Iggy, Event Hubs) | No thread-overhead for libraries with async APIs |

With 20 workers and `--batch-size 2000` on a well-provisioned Kafka cluster
a single node routinely exceeds 100 000 messages per second.

## Running tests

```bash
pip install pytest pytest-asyncio
pytest
```

## Project layout

```
backlog-clearer/
├── cleaner.py          CLI entry point
├── requirements.txt    Python dependencies
├── pytest.ini          Test configuration
├── clearers/
│   ├── __init__.py     Factory function (get_clearer)
│   ├── base.py         Abstract base class + shared utilities
│   ├── kafka.py        Kafka clearer
│   ├── pulsar.py       Pulsar / StreamNative clearer
│   ├── iggy.py         Iggy clearer
│   ├── googlepubsub.py Google Cloud Pub/Sub clearer
│   └── eventhubs.py    Azure Event Hubs clearer
└── tests/
    └── test_clearers.py Unit tests (all external libs mocked)
```
