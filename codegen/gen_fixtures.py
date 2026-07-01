"""Generate one golden sample event per topic under fixtures/<topic>.json.

Each fixture is a schema-valid value with a real EventEnvelopeHeader. Values are synthesized
type-directed from the schema (first enum symbol, one array element, non-null union branch),
then org_id / idempotency_key / producer are set to canonical-looking values. Every fixture is
round-tripped through fastavro before it is written, so a bad fixture fails generation.
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

    print(f"generated {len(TOPICS)} golden fixtures (all round-tripped through fastavro)")


if __name__ == "__main__":
    main()
