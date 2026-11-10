"""agent-results: the MeetingAgent / meeting_update contract (mytheus-docs ADR-0038 D7, D8).

1. The curated examples under fixtures/examples/ round-trip and their payload_json matches the
   D7 shape exactly (field names, types, limits), checked by validate_meeting_update below.
2. The enum evolution is BACKWARD (the registry's rule), and the behaviour of consumers on
   the other side of the upgrade is pinned: decoding with the writer schema (what every
   consumer does) yields the new symbols as plain strings, while a reader that projects onto
   the previous schema fails on AgentClass (no default, deliberately) and maps an unknown kind
   to its default `note`.
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

EXAMPLES = sorted(glob.glob(os.path.join(FIXTURES_DIR, "examples", "agent-results.*.json")))
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
NEW_AGENTS = {"MeetingAgent"}
NEW_KINDS = {"meeting_update"}


def _uuid(v, where):
    assert isinstance(v, str) and UUID_RE.match(v), f"{where}: not a uuid: {v!r}"


def _text(v, limit, where):
    assert isinstance(v, str) and v.strip(), f"{where}: must be a non-empty string"
    assert len(v) <= limit, f"{where}: {len(v)} chars > {limit}"


def validate_meeting_update(payload) -> None:
    """Raise AssertionError unless `payload` is a D7 meeting_update payload_json object."""
    assert isinstance(payload, dict), "payload_json must be a JSON object"
    assert set(payload) == {"updates", "nudge", "surfaced"}, f"keys {sorted(payload)}"
    assert payload["surfaced"] is True, "surfaced must be true"
    updates = payload["updates"]
    assert isinstance(updates, list), "updates must be a list"
    seen = set()
    for i, u in enumerate(updates):
        assert isinstance(u, dict) and set(u) == {"item_id", "status", "note"}, f"updates[{i}]"
        _uuid(u["item_id"], f"updates[{i}].item_id")
        assert u["item_id"] not in seen, f"updates[{i}]: second update for one item"
        seen.add(u["item_id"])
        assert u["status"] in ("in_progress", "covered"), f"updates[{i}].status"
        _text(u["note"], 300, f"updates[{i}].note")
    nudge = payload["nudge"]
    if nudge is not None:
        assert isinstance(nudge, dict), "nudge must be an object or null"
        assert set(nudge) == {"id", "type", "item_id", "content"}, f"nudge keys {sorted(nudge)}"
        _uuid(nudge["id"], "nudge.id")
        assert nudge["type"] in ("off_agenda", "over_time", "unaddressed"), "nudge.type"
        if nudge["item_id"] is not None:
            _uuid(nudge["item_id"], "nudge.item_id")
        _text(nudge["content"], 200, "nudge.content")
    assert updates or nudge is not None, "a window with nothing to say emits no record"


def _roundtrip(schema, rec, reader=None):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, rec)
    buf.seek(0)
    return schemaless_reader(buf, schema, reader_schema=reader)


def _previous_raw():
    """The agent-results schema as it was before ADR-0038: the new symbols removed."""
    raw = copy.deepcopy(load_raw(topic_schema_path("agent-results")))
    for f in raw["fields"]:
        if f["name"] == "agent":
            f["type"]["symbols"] = [s for s in f["type"]["symbols"] if s not in NEW_AGENTS]
        if f["name"] == "kind":
            f["type"]["symbols"] = [s for s in f["type"]["symbols"] if s not in NEW_KINDS]
    return raw


def _parsed(raw):
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    return parse_schema(raw, named_schemas=ns)


def _example(name):
    with open(os.path.join(FIXTURES_DIR, "examples", f"agent-results.{name}.json")) as f:
        return json.load(f)


def test_examples_exist_with_and_without_nudge():
    names = {os.path.basename(p) for p in EXAMPLES}
    assert names == {"agent-results.meeting_update.json",
                     "agent-results.meeting_update.no-nudge.json"}
    nudges = [json.loads(_example(n)["payload_json"])["nudge"]
              for n in ("meeting_update", "meeting_update.no-nudge")]
    assert nudges[0] is not None and nudges[1] is None


@pytest.mark.parametrize("path", EXAMPLES, ids=os.path.basename)
def test_example_roundtrips_and_matches_d7(path):
    with open(path) as f:
        rec = json.load(f)
    back = _roundtrip(load_topic("agent-results"), rec)
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "meeting_update")
    assert back["payload_json"] == rec["payload_json"]
    # Canonical key shape, unchanged by ADR-0038: one record per meeting, window, host, agent.
    h = rec["header"]
    assert h["idempotency_key"] == (
        f"agentresult:{rec['meeting_id']}:{rec['window_id']}:{rec['user_id']}:MeetingAgent"
    )
    validate_meeting_update(json.loads(rec["payload_json"]))


@pytest.mark.parametrize("mutate", [
    lambda p: p["updates"][0].update(note="x" * 301),
    lambda p: p["updates"][0].update(status="skipped"),
    lambda p: p["updates"][0].update(extra=1),
    lambda p: p["updates"].append(dict(p["updates"][0])),
    lambda p: p["nudge"].update(content="x" * 201),
    lambda p: p["nudge"].update(type="reminder"),
    lambda p: p["nudge"].update(item_id="not-a-uuid"),
    lambda p: p.update(surfaced=False),
    lambda p: p.pop("nudge"),
    lambda p: p.update(updates=[], nudge=None),
], ids=["note>300", "bad-status", "extra-key", "dup-item", "content>200", "bad-type",
        "bad-item-id", "not-surfaced", "nudge-missing", "nothing-to-say"])
def test_validator_rejects_off_contract_payloads(mutate):
    payload = json.loads(_example("meeting_update")["payload_json"])
    mutate(payload)
    with pytest.raises(AssertionError):
        validate_meeting_update(payload)


def test_validator_accepts_nudge_only_and_null_item_id():
    payload = json.loads(_example("meeting_update")["payload_json"])
    payload["updates"] = []
    payload["nudge"].update(type="off_agenda", item_id=None)
    validate_meeting_update(payload)


def test_new_schema_reads_previous_writer_data_backward():
    """BACKWARD, the registry rule: the new reader reads bytes from the previous writer."""
    with open(os.path.join(FIXTURES_DIR, "agent-results.json")) as f:
        rec = json.load(f)
    prev = _parsed(_previous_raw())
    back = _roundtrip(prev, rec, reader=load_topic("agent-results"))
    assert (back["agent"], back["kind"]) == (rec["agent"], rec["kind"])


def test_writer_schema_decode_yields_new_symbols():
    """What every consumer does today (registry AvroDeserializer / kafkajs decode, no reader
    schema): the new symbols arrive as plain strings, so tolerance is an app-level skip."""
    back = _roundtrip(load_topic("agent-results"), _example("meeting_update"))
    assert (back["agent"], back["kind"]) == ("MeetingAgent", "meeting_update")


def test_projection_onto_previous_reader_fails_on_agent_maps_kind_to_note():
    new = load_topic("agent-results")
    prev = _parsed(_previous_raw())
    # AgentClass has no default: an old projecting reader rejects the record (to the DLQ).
    with pytest.raises(Exception, match="MeetingAgent"):
        _roundtrip(new, _example("meeting_update"), reader=prev)
    # AgentResultKind defaults to note: a known agent with the new kind degrades to note.
    rec = _example("meeting_update")
    rec["agent"] = "PersonalListenerAgent"
    assert _roundtrip(new, rec, reader=prev)["kind"] == "note"
