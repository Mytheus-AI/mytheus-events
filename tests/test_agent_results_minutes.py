"""agent-results: the MeetingAgent / minutes contract (polish program "Minutes for everyone").

1. The curated examples (personal, shared, unavailable) round-trip and their payload_json
   matches the documented shape exactly (field names, types, limits), checked by
   validate_minutes below; the shared minutes never carry promises.
2. The enum evolution is BACKWARD: a reader with `minutes` reads data written before it; a
   writer-schema decode yields the new kind as a plain string; an old projecting reader maps
   the new kind to its default `note` (AgentClass is unchanged: MeetingAgent already exists),
   and every consumer drops (agent, kind) pairs it does not handle.
"""
import copy
import glob
import io
import json
import os
import re

import pytest
from fastavro import parse_schema, schemaless_reader, schemaless_writer

from _schemas import ENVELOPE_PATH, FIXTURES_DIR, load_raw, load_topic, topic_schema_path

EXAMPLES = sorted(glob.glob(os.path.join(FIXTURES_DIR, "examples", "agent-results.minutes.*.json")))
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
NIL = "00000000-0000-0000-0000-000000000000"
KEYS = {
    "scope", "prompt_version", "generation", "status", "reason", "summary", "decisions",
    "open_items", "unanswered_questions", "promises",
}
MAX_ITEMS = 20


def _uuid_or_none(v, where):
    assert v is None or (isinstance(v, str) and UUID_RE.match(v)), f"{where}: {v!r}"


def _text(v, limit, where):
    assert isinstance(v, str) and v.strip(), f"{where}: must be a non-empty string"
    assert len(v) <= limit, f"{where}: {len(v)} chars > {limit}"


def _opt_text(v, limit, where):
    if v is not None:
        _text(v, limit, where)


def _entries(payload, key, fields):
    items = payload[key]
    assert isinstance(items, list) and len(items) <= MAX_ITEMS, f"{key}: list of <= 20"
    for i, e in enumerate(items):
        assert isinstance(e, dict) and set(e) == fields, f"{key}[{i}] keys {sorted(e)}"
        _text(e["text"], 300, f"{key}[{i}].text")
    return items


def validate_minutes(payload, user_id) -> None:
    assert isinstance(payload, dict) and set(payload) == KEYS, f"keys {sorted(payload)}"
    assert payload["scope"] in ("personal", "shared")
    assert (payload["scope"] == "shared") == (user_id == NIL), "shared <=> the nil user"
    assert re.match(r"^minutes@v\d+$", payload["prompt_version"]), "prompt_version"
    assert isinstance(payload["generation"], str) and payload["generation"], "generation"
    assert payload["status"] in ("ready", "empty", "unavailable")
    assert payload["reason"] in (None, "no_transcript", "budget", "model_error")
    assert isinstance(payload["summary"], str) and len(payload["summary"]) <= 600
    for d in _entries(payload, "decisions", {"text", "status", "decision_id"}):
        assert d["status"] in ("agreed", "tentative", "deferred", "reversed")
        _uuid_or_none(d["decision_id"], "decision_id")
    for o in _entries(payload, "open_items", {"text", "owner_user_id", "owner_name", "due"}):
        _uuid_or_none(o["owner_user_id"], "owner_user_id")
        _opt_text(o["owner_name"], 120, "owner_name")
        _opt_text(o["due"], 60, "due")
    for q in _entries(
        payload, "unanswered_questions", {"text", "asked_by_user_id", "asked_by_name"}
    ):
        _uuid_or_none(q["asked_by_user_id"], "asked_by_user_id")
        _opt_text(q["asked_by_name"], 120, "asked_by_name")
    for p in _entries(payload, "promises", {"text", "to_name", "due"}):
        _opt_text(p["to_name"], 120, "to_name")
        _opt_text(p["due"], 60, "due")
    if payload["scope"] == "shared":
        assert payload["promises"] == [], "the shared minutes never carry promises"
    lists = ("decisions", "open_items", "unanswered_questions", "promises")
    if payload["status"] == "unavailable":
        assert payload["scope"] == "shared" and payload["reason"] is not None
        assert payload["summary"] == "" and all(payload[k] == [] for k in lists)
    if payload["status"] == "ready":
        assert payload["summary"].strip() or any(payload[k] for k in lists)


def _roundtrip(schema, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, rec)
    buf.seek(0)
    return schemaless_reader(buf, schema, reader_schema=reader)


def _previous_parsed():
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-results")))
    for f in raw["fields"]:
        if f["name"] == "kind":
            f["type"]["symbols"] = [s for s in f["type"]["symbols"] if s != "minutes"]
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    return parse_schema(raw, named_schemas=ns)


def _example(name):
    with open(os.path.join(FIXTURES_DIR, "examples", f"agent-results.minutes.{name}.json")) as f:
        return json.load(f)


def test_examples_cover_personal_shared_and_unavailable():
    names = {os.path.basename(p) for p in EXAMPLES}
    assert names == {
        "agent-results.minutes.personal.json",
        "agent-results.minutes.shared.json",
        "agent-results.minutes.unavailable.json",
    }


@pytest.mark.parametrize("path", EXAMPLES, ids=os.path.basename)
def test_example_roundtrips_and_matches_the_contract(path):
    with open(path) as f:
        rec = json.load(f)
    back = _roundtrip(load_topic("agent-results"), rec)
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "minutes")
    assert back["window_id"].startswith("minutes:")
    assert rec["header"]["idempotency_key"] == (
        f"agentresult:{rec['meeting_id']}:{rec['window_id']}:{rec['user_id']}:MeetingAgent"
    )
    validate_minutes(json.loads(rec["payload_json"]), rec["user_id"])


@pytest.mark.parametrize("mutate", [
    lambda p: p.update(promises=[{"text": "x", "to_name": None, "due": None}]),
    lambda p: p["decisions"][0].update(status="maybe"),
    lambda p: p["decisions"][0].update(text="x" * 301),
    lambda p: p["open_items"][0].update(extra=1),
    lambda p: p["open_items"][0].update(owner_user_id="nope"),
    lambda p: p.update(summary="x" * 601),
    lambda p: p.update(open_items=[dict(p["open_items"][0]) for _ in range(21)]),
    lambda p: p.update(scope="personal"),
    lambda p: p.pop("reason"),
], ids=["shared-promise", "bad-status", "text>300", "extra-key", "bad-owner", "summary>600",
        "too-many", "scope-user-mismatch", "reason-missing"])
def test_validator_rejects_off_contract_payloads(mutate):
    payload = json.loads(_example("shared")["payload_json"])
    mutate(payload)
    with pytest.raises(AssertionError):
        validate_minutes(payload, NIL)


def test_new_schema_reads_previous_writer_data_backward():
    with open(os.path.join(FIXTURES_DIR, "agent-results.json")) as f:
        rec = json.load(f)
    back = _roundtrip(_previous_parsed(), rec, reader=load_topic("agent-results"))
    assert (back["agent"], back["kind"]) == (rec["agent"], rec["kind"])


def test_writer_schema_decode_yields_minutes():
    back = _roundtrip(load_topic("agent-results"), _example("personal"))
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "minutes")


def test_old_projecting_reader_maps_minutes_to_note():
    back = _roundtrip(load_topic("agent-results"), _example("shared"), reader=_previous_parsed())
    assert back["kind"] == "note"
