"""`register.py --check`: offline tests against a fake Schema Registry HTTP stub."""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest
import register
from _schemas import TOPICS, inlined_schema


def _make_handler(store):
    class H(BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):  # noqa: N802
            parts = self.path.strip("/").split("/")
            subject = parts[1] if len(parts) > 1 else ""
            if parts[:1] == ["subjects"] and parts[2:] == ["versions", "latest"] and subject in store:
                body = json.dumps({"subject": subject, "version": 3,
                                   "schema": store[subject]}).encode()
                self.send_response(200)
            else:
                body = json.dumps({"error_code": 40401, "message": "not found"}).encode()
                self.send_response(404)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _reject(self):
            self.send_response(500)  # --check must be read-only
            self.end_headers()
            store.setdefault("__writes__", []).append(self.path)

        do_PUT = do_POST = _reject  # noqa: N815

    return H


@pytest.fixture
def registry():
    store = {f"{t}-value": json.dumps(inlined_schema(t)) for t in TOPICS}
    srv = HTTPServer(("127.0.0.1", 0), _make_handler(store))
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    yield store, f"http://127.0.0.1:{srv.server_port}"
    srv.shutdown()


def test_in_sync_passes(registry, capsys):
    store, url = registry
    assert register.main(["--check", "--registry", url]) == 0
    assert f"all {len(TOPICS)} local schemas match" in capsys.readouterr().out
    assert "__writes__" not in store


def test_key_order_is_not_a_difference(registry):
    store, url = registry
    s = json.loads(store["actions-value"])
    store["actions-value"] = json.dumps(dict(reversed(list(s.items()))))
    assert register.main(["--check", "--registry", url]) == 0


def test_non_compat_difference_fails(registry, capsys):
    """A doc-only change is BACKWARD-compatible yet must still fail --check."""
    store, url = registry
    s = json.loads(store["decisions-value"])
    s["doc"] = "edited in registry only"
    store["decisions-value"] = json.dumps(s)
    assert register.main(["--check", "--registry", url]) == 1
    err = capsys.readouterr().err
    assert "decisions-value: DIFFERS" in err
    assert "edited in registry only" in err
    assert "1 of" in err


def test_missing_subject_fails(registry, capsys):
    store, url = registry
    del store["admin-events-value"]
    assert register.main(["--check", "--registry", url]) == 1
    assert "admin-events-value: NOT REGISTERED" in capsys.readouterr().err
