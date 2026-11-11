"""agent-results: the MeetingAgent / room_signals contract ("After the meeting" layer 1: the last
word, the agreement check, pick up where we stopped).

1. The curated examples (ready, partial) round-trip and their payload_json matches the
   documented shape exactly (field names, enums, limits), checked by validate_room_signals
   below: at most one last-word entry per person, the ask list never names a reason outside
   hedged/silent/talked_over, at most two positions.
2. The enum evolution is BACKWARD: a reader with `room_signals` reads data written before it; a
   writer-schema decode yields the new kind as a plain string; an old projecting reader maps it
   to its default `note`, and every consumer drops (agent, kind) pairs it does not handle.
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

EXAMPLES = sorted(glob.glob(os.path.join(FIXTURES_DIR, "examples", "agent-results.room_signals*.json")))
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
NIL = "00000000-0000-0000-0000-000000000000"
KEYS = {"generation", "status", "prompt_versions", "last_word", "consensus", "resume_point"}
KINDS = {"spoken_over", "hand_not_called", "cut_off", "topic_silent"}
STRENGTHS = {"strong", "mixed", "weak", "unclear"}
ASK_REASONS = {"hedged", "silent", "talked_over"}
RESUME_REASONS = {"time_ran_out", "agenda_unfinished", "cut_off"}


def _uuid(v, where):
    assert isinstance(v, str) and UUID_RE.match(v), f"{where}: {v!r}"


def _uuid_or_none(v, where):
    if v is not None:
        _uuid(v, where)


def _text(v, limit, where):
    assert isinstance(v, str) and v.strip(), f"{where}: must be a non-empty string"
    assert len(v) <= limit, f"{where}: {len(v)} chars > {limit}"


def _ms(v, where, nullable=False):
    if nullable and v is None:
        return
    assert isinstance(v, int) and not isinstance(v, bool) and v >= 0, f"{where}: {v!r}"


def _count(v, where):
    assert isinstance(v, int) and not isinstance(v, bool) and v >= 0, f"{where}: {v!r}"


def validate_room_signals(payload) -> None:
    assert isinstance(payload, dict) and set(payload) == KEYS, f"keys {sorted(payload)}"
    assert isinstance(payload["generation"], str) and payload["generation"], "generation"
    assert payload["status"] in ("ready", "partial")
    pv = payload["prompt_versions"]
    assert isinstance(pv, dict) and set(pv) == {"last_word", "consensus", "resume_point"}
    for k, v in pv.items():
        assert v is None or re.match(r"^[a-z_]+@v\d+$", v), f"prompt_versions.{k}"

    lw = payload["last_word"]
    assert isinstance(lw, list) and len(lw) <= 50
    seen = set()
    for i, e in enumerate(lw):
        assert set(e) == {"user_id", "kind", "topic", "quote", "at_ms", "end_ms", "decision_id",
                          "signal_ids"}, f"last_word[{i}] keys {sorted(e)}"
        _uuid(e["user_id"], f"last_word[{i}].user_id")
        assert e["user_id"] not in seen, "at most one last-word entry per person"
        seen.add(e["user_id"])
        assert e["kind"] in KINDS
        _text(e["topic"], 80, f"last_word[{i}].topic")
        if e["quote"] is not None:
            _text(e["quote"], 300, f"last_word[{i}].quote")
        _ms(e["at_ms"], "at_ms", nullable=True)
        _ms(e["end_ms"], "end_ms", nullable=True)
        _uuid_or_none(e["decision_id"], "decision_id")
        assert isinstance(e["signal_ids"], list)
        for s in e["signal_ids"]:
            _uuid(s, "signal_ids[]")

    cons = payload["consensus"]
    assert isinstance(cons, list) and len(cons) <= 50
    for i, c in enumerate(cons):
        assert set(c) == {"decision_id", "strength", "participants", "yes", "hedged", "silent",
                          "objected", "talked_over", "ask"}, f"consensus[{i}] keys {sorted(c)}"
        _uuid(c["decision_id"], "decision_id")
        assert c["strength"] in STRENGTHS
        for k in ("participants", "yes", "hedged", "silent", "objected", "talked_over"):
            _count(c[k], f"consensus[{i}].{k}")
        assert sum(c[k] for k in ("yes", "hedged", "silent", "objected", "talked_over")) <= (
            c["participants"]
        ), "each present person is counted once"
        assert isinstance(c["ask"], list) and len(c["ask"]) <= 12
        asked = set()
        for a in c["ask"]:
            assert set(a) == {"user_id", "reason"}
            _uuid(a["user_id"], "ask.user_id")
            assert a["reason"] in ASK_REASONS
            assert a["user_id"] not in asked
            asked.add(a["user_id"])

    rp = payload["resume_point"]
    if rp is None:
        return
    assert set(rp) == {"reason", "topic", "agenda_item_id", "positions", "cut_off"}
    assert rp["reason"] in RESUME_REASONS
    _text(rp["topic"], 120, "resume_point.topic")
    _uuid_or_none(rp["agenda_item_id"], "agenda_item_id")
    assert isinstance(rp["positions"], list) and len(rp["positions"]) <= 2
    for p in rp["positions"]:
        assert set(p) == {"user_id", "quote", "start_ms", "end_ms"}
        _uuid_or_none(p["user_id"], "positions.user_id")
        _text(p["quote"], 300, "positions.quote")
        _ms(p["start_ms"], "start_ms")
        _ms(p["end_ms"], "end_ms")
    cut = rp["cut_off"]
    if cut is not None:
        assert set(cut) == {"user_id", "quote", "start_ms", "end_ms"}
        _uuid(cut["user_id"], "cut_off.user_id")
        _text(cut["quote"], 300, "cut_off.quote")
        _ms(cut["start_ms"], "start_ms")
        _ms(cut["end_ms"], "end_ms")


def _roundtrip(schema, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, rec)
    buf.seek(0)
    return schemaless_reader(buf, schema, reader_schema=reader)


def _previous_parsed():
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-results")))
    for f in raw["fields"]:
        if f["name"] == "kind":
            f["type"]["symbols"] = [s for s in f["type"]["symbols"] if s != "room_signals"]
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    return parse_schema(raw, named_schemas=ns)


def _example(name):
    with open(os.path.join(FIXTURES_DIR, "examples", f"agent-results.{name}.json")) as f:
        return json.load(f)


def test_examples_cover_ready_and_partial():
    names = {os.path.basename(p) for p in EXAMPLES}
    assert names == {"agent-results.room_signals.json", "agent-results.room_signals.partial.json"}


@pytest.mark.parametrize("path", EXAMPLES, ids=os.path.basename)
def test_example_roundtrips_and_matches_the_contract(path):
    with open(path) as f:
        rec = json.load(f)
    back = _roundtrip(load_topic("agent-results"), rec)
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "room_signals")
    assert str(back["user_id"]) == NIL
    assert back["window_id"].startswith("room-signals:")
    assert rec["header"]["idempotency_key"] == (
        f"agentresult:{rec['meeting_id']}:{rec['window_id']}:{NIL}:MeetingAgent"
    )
    payload = json.loads(rec["payload_json"])
    validate_room_signals(payload)
    assert rec["window_id"] == f"room-signals:{payload['generation']}"


@pytest.mark.parametrize("mutate", [
    lambda p: p["last_word"].append(dict(p["last_word"][0])),
    lambda p: p["last_word"][0].update(kind="interrupted"),
    lambda p: p["last_word"][0].update(topic="x" * 81),
    lambda p: p["last_word"][0].update(user_id="nope"),
    lambda p: p["consensus"][0].update(strength="maybe"),
    lambda p: p["consensus"][0]["ask"].append({"user_id": p["consensus"][0]["ask"][0]["user_id"],
                                               "reason": "silent"}),
    lambda p: p["consensus"][0]["ask"][0].update(reason="objected"),
    lambda p: p["consensus"][0].update(yes=9),
    lambda p: p["resume_point"]["positions"].append(dict(p["resume_point"]["positions"][0])),
    lambda p: p["resume_point"].update(reason="bored"),
    lambda p: p.update(status="empty"),
    lambda p: p.pop("prompt_versions"),
], ids=["two-per-person", "bad-kind", "topic>80", "bad-user", "bad-strength", "ask-twice",
        "ask-objector", "overcount", "three-positions", "bad-reason", "bad-status", "missing-key"])
def test_validator_rejects_off_contract_payloads(mutate):
    payload = json.loads(_example("room_signals")["payload_json"])
    mutate(payload)
    with pytest.raises(AssertionError):
        validate_room_signals(payload)


def test_new_schema_reads_previous_writer_data_backward():
    with open(os.path.join(FIXTURES_DIR, "agent-results.json")) as f:
        rec = json.load(f)
    back = _roundtrip(_previous_parsed(), rec, reader=load_topic("agent-results"))
    assert (back["agent"], back["kind"]) == (rec["agent"], rec["kind"])


def test_writer_schema_decode_yields_room_signals():
    back = _roundtrip(load_topic("agent-results"), _example("room_signals.partial"))
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "room_signals")


def test_old_projecting_reader_maps_room_signals_to_note():
    back = _roundtrip(load_topic("agent-results"), _example("room_signals"), reader=_previous_parsed())
    assert back["kind"] == "note"


def test_symbol_is_appended_at_the_tail():
    raw = load_raw(topic_schema_path("agent-results"))
    kind = next(f for f in raw["fields"] if f["name"] == "kind")
    assert kind["type"]["symbols"][-1] == "room_signals"
    assert kind["type"]["default"] == "note"
