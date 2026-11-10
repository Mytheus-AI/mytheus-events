"""agent-results: the polish program's optional fields (one card per moment, memory inside the
meeting): moment_id, why, updates_candidate_id, window_start_ms, window_end_ms.

1. They are optional with default null at the tail, so the change is BACKWARD: a record written
   with the previous schema reads with the new one (every new field null).
2. A record written with the new schema decodes with the writer schema (what every consumer
   does) with the fields intact, and still projects onto the previous schema as a reader (the
   new fields are skipped), so an un-upgraded consumer keeps working.
3. The curated ActionAgent update example matches the documented shape: the candidate's id
   equals its updates_candidate_id, and the top-level moment mirrors the candidate's.
"""

import copy
import io
import json
import os
import re

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from _schemas import ENVELOPE_PATH, FIXTURES_DIR, load_raw, load_topic, topic_schema_path

NEW_FIELDS = ("moment_id", "why", "updates_candidate_id", "window_start_ms", "window_end_ms")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
EXAMPLE = os.path.join(FIXTURES_DIR, "examples", "agent-results.action_candidate.update.json")


def _parsed(raw):
    named = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=named)
    return parse_schema(raw, named_schemas=named)


def _previous():
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-results")))
    raw["fields"] = [f for f in raw["fields"] if f["name"] not in NEW_FIELDS]
    return _parsed(raw)


def _io(writer, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, writer, rec)
    buf.seek(0)
    return schemaless_reader(buf, writer, reader_schema=reader)


def _example():
    with open(EXAMPLE) as f:
        return json.load(f)


def test_the_new_fields_are_optional_nullable_and_at_the_tail():
    fields = load_raw(topic_schema_path("agent-results"))["fields"]
    assert tuple(f["name"] for f in fields[-len(NEW_FIELDS):]) == NEW_FIELDS
    for f in fields[-len(NEW_FIELDS):]:
        assert isinstance(f["type"], list) and f["type"][0] == "null", f["name"]
        assert "default" in f and f["default"] is None, f["name"]
        assert f.get("doc"), f["name"]


def test_old_data_reads_with_the_new_schema_as_nulls():
    rec = _example()
    for name in NEW_FIELDS:
        rec.pop(name)
    back = _io(_previous(), rec, reader=load_topic("agent-results"))
    assert all(back[name] is None for name in NEW_FIELDS)
    assert back["agent"] == "ActionAgent"


def test_new_data_decodes_with_the_writer_and_projects_onto_the_old_reader():
    rec = _example()
    back = _io(load_topic("agent-results"), rec)
    assert str(back["moment_id"]) == rec["moment_id"]
    assert str(back["updates_candidate_id"]) == rec["updates_candidate_id"]
    assert (back["window_start_ms"], back["window_end_ms"]) == (752_000, 757_900)
    old = _io(load_topic("agent-results"), rec, reader=_previous())
    assert not set(NEW_FIELDS) & set(old)


def test_the_update_example_has_the_documented_shape():
    rec = _example()
    (cand,) = json.loads(rec["payload_json"])["candidates"]
    assert cand["id"] == cand["updates_candidate_id"] == rec["updates_candidate_id"]
    assert cand["moment_id"] == rec["moment_id"] and UUID_RE.match(cand["moment_id"])
    assert cand["why"] == rec["why"] and len(cand["why"]) <= 200
    assert len(cand["quote"]) <= 300 and cand["start_ms"] <= cand["end_ms"]
    assert rec["window_id"] == f"{rec['meeting_id']}:{rec['window_start_ms']}"
