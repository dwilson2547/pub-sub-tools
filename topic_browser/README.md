# topic_browser

Backend API and management UI for browsing pub-sub topic messages.

Run locally from repository root:

```bash
python topic_browser/browser_api.py --host 127.0.0.1 --port 8081
```

Then open the management UI in your browser:

```
http://127.0.0.1:8081/
```

The UI provides a three-panel management interface:

- **Connections sidebar** — add and remove server connections (memory or Kafka).
- **Topics panel** — lists topics for the active connection; click to select.
- **Messages panel** — pull messages from the selected topic with configurable max count and peek/consume mode.

API contract for UI integration:

- `topic_browser/docs/message-browser-api.md`

Run tests:

```bash
pytest -q topic_browser/tests/test_browser_api.py
```
