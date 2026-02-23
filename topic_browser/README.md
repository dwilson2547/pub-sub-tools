# topic_browser

Backend API tool for browsing pub-sub topic messages.

Run locally from repository root:

```bash
python topic_browser/browser_api.py --host 127.0.0.1 --port 8081
```

API contract for UI integration:

- `topic_browser/docs/message-browser-api.md`

Run tests:

```bash
pytest -q topic_browser/tests/test_browser_api.py
```
