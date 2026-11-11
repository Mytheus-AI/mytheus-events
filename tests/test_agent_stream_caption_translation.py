"""agent-stream: the caption_translation kind (polish program wave 3, live translation).

1. The curated examples round-trip and match the contract in the schema doc: user_id is the
   nil UUID, window_id is caption:{chunk_id}, seq 0, done true, a language code, a status, and
   text only when the status is translated.
2. The evolution is BACKWARD: the new reader reads bytes from the previous writer (the four new
   fields materialize as their defaults, so an old token delta reads as kind=delta), and an old
   reader that projects a caption_translation record keeps working (the extra fields are
   dropped), which is why an un-upgraded realtime-gateway routes it as a delta to the nil user,
   a socket that never exists, instead of failing.
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

EXAMPLES = sorted(glob.glob(os.path.join(FIXTURES_DIR, "examples", "agent-stream.*.json")))
NEW_FIELDS = ("kind", "chunk_id", "language", "status")
NIL = "00000000-0000-0000-0000-000000000000"
LANGUAGES = {"en", "es", "fr", "de", "pt", "it", "nl", "hi", "ja", "ko", "zh", "ar"}
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def _parsed(raw):
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    return parse_schema(raw, named_schemas=ns)


def _previous_raw():
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-stream")))
    raw["fields"] = [f for f in raw["fields"] if f["name"] not in NEW_FIELDS]
    return raw


def _roundtrip(schema, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, rec)
    buf.seek(0)
    return schemaless_reader(buf, schema, reader_schema=reader)


def validate_caption_translation(rec) -> None:
    assert rec["kind"] == "caption_translation"
    assert rec["user_id"] == NIL, "a translation is not addressed to a person"
    assert isinstance(rec["chunk_id"], str) and UUID_RE.match(rec["chunk_id"])
    assert rec["window_id"] == f"caption:{rec['chunk_id']}"
    assert rec["seq"] == 0 and rec["done"] is True
    assert rec["language"] in LANGUAGES
    assert rec["status"] in ("translated", "budget_denied", "unavailable")
    if rec["status"] == "translated":
        assert rec["delta"].strip(), "a translated record carries the text"
    else:
        assert rec["delta"] == "", "only a translated record carries text"
    assert rec["header"]["idempotency_key"] == f"caption:{rec['chunk_id']}:{rec['language']}"


def test_examples_cover_translated_and_budget_denied():
    names = {os.path.basename(p) for p in EXAMPLES}
    assert names == {"agent-stream.caption_translation.json",
                     "agent-stream.caption_translation.budget_denied.json"}


@pytest.mark.parametrize("path", EXAMPLES, ids=os.path.basename)
def test_example_roundtrips_and_matches_contract(path):
    with open(path) as f:
        rec = json.load(f)
    back = _roundtrip(load_topic("agent-stream"), rec)
    assert back["kind"] == "caption_translation"
    assert back["language"] == rec["language"] and back["status"] == rec["status"]
    validate_caption_translation(rec)


@pytest.mark.parametrize("mutate", [
    lambda r: r.update(user_id="00000000-0000-4000-8000-0000000000aa"),
    lambda r: r.update(window_id="w-1"),
    lambda r: r.update(language="klingon"),
    lambda r: r.update(delta=""),
    lambda r: r.update(status="budget_denied"),
], ids=["addressed", "bad-window", "bad-language", "no-text", "denied-with-text"])
def test_validator_rejects_off_contract_records(mutate):
    with open(os.path.join(FIXTURES_DIR, "examples", "agent-stream.caption_translation.json")) as f:
        rec = json.load(f)
    mutate(rec)
    with pytest.raises(AssertionError):
        validate_caption_translation(rec)


def test_new_reader_reads_previous_writer_as_delta():
    with open(os.path.join(FIXTURES_DIR, "agent-stream.json")) as f:
        rec = json.load(f)
    for name in NEW_FIELDS:
        rec.pop(name, None)
    back = _roundtrip(_parsed(_previous_raw()), rec, reader=load_topic("agent-stream"))
    assert back["kind"] == "delta"
    assert back["chunk_id"] is None and back["language"] is None and back["status"] is None


def test_previous_reader_projects_a_translation_without_failing():
    with open(os.path.join(FIXTURES_DIR, "examples", "agent-stream.caption_translation.json")) as f:
        rec = json.load(f)
    back = _roundtrip(load_topic("agent-stream"), rec, reader=_parsed(_previous_raw()))
    assert "kind" not in back and str(back["user_id"]) == NIL
