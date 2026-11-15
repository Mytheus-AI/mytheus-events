"""agent-results: attribution fields prompt_version and model_id (journal plan Part 3 fix 11).

1. Optional with default null at the tail, so the change is BACKWARD: a record written with the
   previous schema reads with the new one (both fields null).
2. A record written with the new schema decodes with the writer schema with the fields intact,
   and projects onto the previous schema as a reader (the fields are skipped), so a consumer
   that has not upgraded keeps working.
"""

import copy
import io
import json
import os

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from _schemas import ENVELOPE_PATH, FIXTURES_DIR, load_raw, topic_schema_path

NEW_FIELDS = ("prompt_version", "model_id")
EXAMPLE = os.path.join(FIXTURES_DIR, "examples", "agent-results.action_candidate.update.json")


def _parsed(raw):
    named = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=named)
    return parse_schema(raw, named_schemas=named)


def _current():
    return _parsed(copy.deepcopy(load_raw(topic_schema_path("agent-results"))))


def _previous():
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-results")))
    raw["fields"] = [f for f in raw["fields"] if f["name"] not in NEW_FIELDS]
    return _parsed(raw)


def _io(writer, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, writer, rec)
    buf.seek(0)
    return schemaless_reader(buf, writer, reader_schema=reader)


def _record(**extra):
    # A real curated record (all pre-attribution fields), so this test tracks the schema.
    with open(EXAMPLE) as f:
        rec = json.load(f)
    for name in NEW_FIELDS:
        rec.pop(name, None)
    rec.update(extra)
    return rec


def test_the_fields_are_optional_nullable_documented_and_at_the_tail():
    fields = load_raw(topic_schema_path("agent-results"))["fields"]
    assert tuple(f["name"] for f in fields[-len(NEW_FIELDS):]) == NEW_FIELDS
    for f in fields[-len(NEW_FIELDS):]:
        assert f["type"] == ["null", "string"], f["name"]
        assert "default" in f and f["default"] is None, f["name"]
        assert f.get("doc"), f["name"]


def test_an_old_record_reads_with_the_new_schema_as_null():
    out = _io(_previous(), _record(), reader=_current())
    assert out["prompt_version"] is None and out["model_id"] is None


def test_a_new_record_round_trips_and_projects_onto_the_old_schema():
    rec = _record(prompt_version="decision@v3", model_id="claude-haiku-5-5")
    out = _io(_current(), rec)
    assert out["prompt_version"] == "decision@v3"
    assert out["model_id"] == "claude-haiku-5-5"
    old = _io(_current(), rec, reader=_previous())
    assert "prompt_version" not in old and "model_id" not in old
    assert old["content"] == rec["content"]
