"""Generate one golden sample event per topic under fixtures/<topic>.json.

Each fixture is a schema-valid value with a real EventEnvelopeHeader. Values are synthesized
type-directed from the schema (first enum symbol, one array element, non-null union branch),
then org_id / idempotency_key / producer are set to canonical-looking values. Every fixture is
round-tripped through fastavro before it is written, so a bad fixture fails generation.

fixtures/examples/<name>.json holds curated per-kind examples on top of the one-per-topic
goldens: a synthesized value cannot show a payload_json shape, so the kinds whose payload is
contracted in the schema doc get a hand-specified example here (EXAMPLES), generated and
round-tripped the same way. tests/ validates their payload_json against that contract.
"""
from __future__ import annotations

import io
import json
import os

from fastavro import schemaless_reader, schemaless_writer

from _schemas import (
    ENVELOPE_PATH, FIXTURES_DIR, IDEMPOTENCY_KEYS, TOPICS,
    load_raw, load_topic, topic_schema_path,
)

ORG = "00000000-0000-4000-8000-000000000001"
SAMPLE_UUID = "00000000-0000-4000-8000-0000000000aa"
EXAMPLES_DIR = os.path.join(FIXTURES_DIR, "examples")

# MeetingAgent meeting_update (ADR-0038 D7): one batched record per meeting per window, user_id
# is the host. Same meeting/window/host in both, so both share one idempotency key shape.
_MEETING = "00000000-0000-4000-8000-0000000000b1"
_HOST = "00000000-0000-4000-8000-0000000000b2"
_ITEM_1 = "00000000-0000-4000-8000-0000000000c1"
_ITEM_2 = "00000000-0000-4000-8000-0000000000c2"
_NUDGE = "00000000-0000-4000-8000-0000000000d1"
_CHUNK = "00000000-0000-4000-8000-0000000000e1"


def _meeting_update(window_id: str, event_id: str, payload: dict, content: str) -> dict:
    return {
        "header": {"event_id": event_id,
                   "idempotency_key": f"agentresult:{_MEETING}:{window_id}:{_HOST}:MeetingAgent",
                   "producer": "agent-runtime-service"},
        "meeting_id": _MEETING, "window_id": window_id, "user_id": _HOST,
        "agent": "MeetingAgent", "kind": "meeting_update",
        "relevance_score": 1.0, "confidence": 0.9, "content": content,
        "payload_json": json.dumps(payload), "source_chunk_ids": [_CHUNK],
        # an agenda update quotes no one: no moment (polish program fields stay null)
        "moment_id": None, "why": None, "updates_candidate_id": None,
        "window_start_ms": None, "window_end_ms": None,
    }


_ACTION = "00000000-0000-4000-8000-0000000000a1"
_MOMENT = "00000000-0000-4000-8000-0000000000b1"
_OWNER = "00000000-0000-4000-8000-0000000000b2"


def _action_update() -> dict:
    """Polish program, memory inside the meeting + one card per moment: an ActionAgent record
    that UPDATES an open card (same id, updates_candidate_id) and points at its moment."""
    cand = {
        "id": _ACTION, "content": "Send the pricing deck to Acme by Friday",
        "confidence": 0.88, "surfaced": True, "assignee_id": _OWNER,
        "due_date": "2026-11-13", "depends_on": [],
        "updates_candidate_id": _ACTION, "moment_id": _MOMENT,
        "quote": "I'll take the Acme deck, I can get it out by Friday.",
        "speaker_id": _OWNER, "speaker_label": "Priya Sharma",
        "start_ms": 754_200, "end_ms": 757_900, "why": "Refines an earlier card",
    }
    window_id = f"{_MEETING}:752000"
    return {
        "header": {"event_id": "00000000-0000-4000-8000-0000000000f3",
                   "idempotency_key": f"agentresult:{_MEETING}:{window_id}:{_MEETING}:ActionAgent",
                   "producer": "agent-runtime-service"},
        "meeting_id": _MEETING, "window_id": window_id, "user_id": _MEETING,
        "agent": "ActionAgent", "kind": "action_candidate",
        "relevance_score": 0.9, "confidence": 0.88, "content": cand["content"],
        "payload_json": json.dumps({"candidates": [cand], "prompt": "action_agent@v3",
                                    "model_tier": "sonnet"}),
        "source_chunk_ids": [_CHUNK],
        "moment_id": _MOMENT, "why": "Refines an earlier card",
        "updates_candidate_id": _ACTION, "window_start_ms": 752_000, "window_end_ms": 757_900,
    }


# name -> (topic, overrides applied on top of the synthesized golden for that topic).
EXAMPLES: dict[str, tuple[str, dict]] = {
    "agent-results.meeting_update": ("agent-results", _meeting_update(
        "w-000012", "00000000-0000-4000-8000-0000000000f1",
        {
            "updates": [
                {"item_id": _ITEM_1, "status": "covered",
                 "note": "Q3 budget agreed at 120k; finance to confirm the vendor split."},
                {"item_id": _ITEM_2, "status": "in_progress",
                 "note": "Hiring plan discussion started."},
            ],
            "nudge": {"id": _NUDGE, "type": "over_time", "item_id": _ITEM_1,
                      "content": "Budget review ran past its 10 minutes."},
            "surfaced": True,
        },
        "Budget review covered; hiring plan in progress.",
    )),
    "agent-results.meeting_update.no-nudge": ("agent-results", _meeting_update(
        "w-000013", "00000000-0000-4000-8000-0000000000f2",
        {
            "updates": [
                {"item_id": _ITEM_2, "status": "covered",
                 "note": "Two backend hires approved for Q4."},
            ],
            "nudge": None,
            "surfaced": True,
        },
        "Hiring plan covered.",
    )),
    "agent-results.action_candidate.update": ("agent-results", _action_update()),
}

# fullname/name -> record schema, for resolving referenced named types (the envelope).
NAMED: dict[str, dict] = {}


def _register_named(schema: dict) -> None:
    if isinstance(schema, dict) and schema.get("type") == "record":
        ns = schema.get("namespace", "")
        fullname = f"{ns}.{schema['name']}" if ns else schema["name"]
        NAMED[fullname] = schema
        NAMED[schema["name"]] = schema


def synth(t, field_name: str = ""):
    if isinstance(t, list):  # union: take the first non-null branch to exercise the field
        branches = [b for b in t if b != "null"]
        return synth(branches[0], field_name) if branches else None
    if isinstance(t, dict):
        logical, kind = t.get("logicalType"), t.get("type")
        if kind == "enum":
            return t["symbols"][0]
        if kind == "array":
            return [synth(t["items"], field_name)]
        if kind == "record":
            return {f["name"]: synth(f["type"], f["name"]) for f in t["fields"]}
        if logical == "uuid":
            return SAMPLE_UUID
        if logical == "timestamp-micros":
            return 1_700_000_000_000_000
        if logical == "date":
            return 19000
        return synth(kind, field_name)
    if t in NAMED:  # named reference, e.g. io.nexus.events.EventEnvelopeHeader
        return {f["name"]: synth(f["type"], f["name"]) for f in NAMED[t]["fields"]}
    return {
        "string": field_name or "sample",
        "int": 1, "long": 1, "float": 0.9, "double": 1.5, "boolean": True, "null": None,
    }.get(t, "sample")


def main() -> None:
    os.makedirs(FIXTURES_DIR, exist_ok=True)
    _register_named(load_raw(ENVELOPE_PATH))

    for t in TOPICS:
        raw = load_raw(topic_schema_path(t))
        record = {f["name"]: synth(f["type"], f["name"]) for f in raw["fields"]}
        header = record["header"]
        header["org_id"] = ORG
        header["producer"] = "sample-service"
        header["idempotency_key"] = IDEMPOTENCY_KEYS.get(t, f"{t}:sample").replace("{", "").replace("}", "")
        header["correlation_id"] = None
        header["causation_id"] = None

        parsed = load_topic(t)
        buf = io.BytesIO()
        schemaless_writer(buf, parsed, record)
        buf.seek(0)
        back = schemaless_reader(buf, parsed)
        # fastavro deserializes uuid logicalType to a uuid.UUID object; compare string forms.
        assert str(back["header"]["org_id"]) == ORG, f"{t}: round-trip org_id mismatch"

        with open(os.path.join(FIXTURES_DIR, f"{t}.json"), "w") as f:
            json.dump(record, f, indent=2)
            f.write("\n")

    os.makedirs(EXAMPLES_DIR, exist_ok=True)
    for name, (t, overrides) in EXAMPLES.items():
        with open(os.path.join(FIXTURES_DIR, f"{t}.json")) as f:
            record = json.load(f)
        for key, value in overrides.items():
            if key == "header":
                record["header"].update(value)
            else:
                record[key] = value
        parsed = load_topic(t)
        buf = io.BytesIO()
        schemaless_writer(buf, parsed, record)
        buf.seek(0)
        back = schemaless_reader(buf, parsed)
        assert back["agent"] == record["agent"] and back["kind"] == record["kind"], name
        with open(os.path.join(EXAMPLES_DIR, f"{name}.json"), "w") as f:
            json.dump(record, f, indent=2)
            f.write("\n")

    print(f"generated {len(TOPICS)} golden fixtures + {len(EXAMPLES)} per-kind examples "
          "(all round-tripped through fastavro)")


if __name__ == "__main__":
    main()
