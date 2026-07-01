"""Parse + lint every .avsc and enforce the structural invariants of the event contract.

Asserts:
  - EventEnvelopeHeader field order matches the frozen 8-field list; org_id non-nullable.
  - exactly 21 topic schema folders, matching the canonical topic set.
  - every topic value's first field is `header` typed io.nexus.events.EventEnvelopeHeader.
  - every .avsc parses (this rejects invalid Avro, e.g. dotted enum symbols).
  - Decision/Action status enum symbols == ['confirmed'] (no-silent-commits, structural).

Non-zero exit on any failure. Usage: python codegen/validate.py [schemas_dir_ignored]
"""
from __future__ import annotations

import os
import sys

from _schemas import (
    ENVELOPE_PATH, HEADER_FIELDS, SCHEMAS_DIR, TOPICS,
    load_raw, load_topic, topic_schema_path,
)


def main() -> int:
    errors: list[str] = []

    # 1. Frozen header field order + org_id non-nullable.
    env = load_raw(ENVELOPE_PATH)
    names = [f["name"] for f in env["fields"]]
    if names != HEADER_FIELDS:
        errors.append(f"EventEnvelopeHeader field order {names} != frozen {HEADER_FIELDS}")
    org = next((f for f in env["fields"] if f["name"] == "org_id"), None)
    if org is None or isinstance(org["type"], list):
        errors.append("header.org_id must be present and non-nullable (it is the partition key)")

    # 2. Exactly 21 topic folders matching the canonical set.
    topic_dirs = sorted(
        d for d in os.listdir(SCHEMAS_DIR)
        if not d.startswith("_") and os.path.isdir(os.path.join(SCHEMAS_DIR, d))
    )
    if len(topic_dirs) != 21:
        errors.append(f"expected 21 topic folders, found {len(topic_dirs)}: {topic_dirs}")
    for missing in sorted(set(TOPICS) - set(topic_dirs)):
        errors.append(f"missing topic schema folder: {missing}")
    for extra in sorted(set(topic_dirs) - set(TOPICS)):
        errors.append(f"unexpected topic schema folder (not in canon): {extra}")

    # 3. Per-topic: header-first, typed EventEnvelopeHeader, parses cleanly.
    for t in TOPICS:
        path = topic_schema_path(t)
        if not os.path.exists(path):
            errors.append(f"{t}: missing v1.avsc")
            continue
        raw = load_raw(path)
        f0 = raw["fields"][0]
        if f0["name"] != "header":
            errors.append(f"{t}: first field is {f0['name']!r}, must be 'header'")
        if f0.get("type") not in ("io.nexus.events.EventEnvelopeHeader", "EventEnvelopeHeader"):
            errors.append(f"{t}: header type {f0.get('type')!r} must reference EventEnvelopeHeader")
        try:
            load_topic(t)
        except Exception as exc:  # invalid Avro (bad enum symbols, unknown types, ...)
            errors.append(f"{t}: parse failed: {exc}")

    # 4. Decision/Action status enum is exactly ['confirmed'].
    for t in ("decisions", "actions"):
        raw = load_raw(topic_schema_path(t))
        st = next((f for f in raw["fields"] if f["name"] == "status"), None)
        syms = st["type"].get("symbols") if st and isinstance(st["type"], dict) else None
        if syms != ["confirmed"]:
            errors.append(f"{t}: status enum symbols {syms} != ['confirmed']")

    if errors:
        print("VALIDATE FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print(f"validate OK: {len(TOPICS)} topics; envelope+dlq parse; header-first; frozen header order.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
