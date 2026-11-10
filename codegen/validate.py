"""Parse + lint every .avsc and enforce the structural invariants of the event contract.

Asserts:
  - EventEnvelopeHeader field order matches the frozen 8-field list; org_id non-nullable on
    every topic (org_id is the sole partition key on 19 topics, and the leading component of
    the composite {org_id}:{meeting_id} key on transcript-chunks/agent-stream — ADR-0017 in
    mytheus-docs. This check does not distinguish the two cases: org_id must be present and
    non-nullable regardless of which partition-key shape a topic uses).
  - exactly 22 topic schema folders, matching the canonical topic set.
  - every topic value's first field is `header` typed io.nexus.events.EventEnvelopeHeader.
  - every .avsc parses (this rejects invalid Avro, e.g. dotted enum symbols).
  - Decision/Action status enum symbols == ['confirmed'] (no-silent-commits, structural).
  - the two composite-partition-key topics (transcript-chunks, agent-stream) carry a
    non-nullable meeting_id field, since that is the second half of their partition key.

Non-zero exit on any failure. Usage: python codegen/validate.py [schemas_dir_ignored]
"""
from __future__ import annotations

import os
import sys

from _schemas import (
    COMPOSITE_PARTITION_KEY_TOPICS, ENVELOPE_PATH, HEADER_FIELDS, SCHEMAS_DIR, TOPICS,
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
        errors.append(
            "header.org_id must be present and non-nullable (it is the tenant key on every "
            "topic: the sole partition key on 19 topics, and the leading component of the "
            "composite {org_id}:{meeting_id} key on transcript-chunks/agent-stream, ADR-0017)"
        )

    # 2. Exactly 22 topic folders matching the canonical set.
    topic_dirs = sorted(
        d for d in os.listdir(SCHEMAS_DIR)
        if not d.startswith("_") and os.path.isdir(os.path.join(SCHEMAS_DIR, d))
    )
    if len(topic_dirs) != 22:
        errors.append(f"expected 22 topic folders, found {len(topic_dirs)}: {topic_dirs}")
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

    # 5. Composite-partition-key topics (transcript-chunks, agent-stream; ADR-0017) must carry
    #    a non-nullable meeting_id field: it is the second half of their {org_id}:{meeting_id}
    #    partition key, so a topic that loses it silently breaks per-meeting partitioning.
    for t in sorted(COMPOSITE_PARTITION_KEY_TOPICS):
        raw = load_raw(topic_schema_path(t))
        mid = next((f for f in raw["fields"] if f["name"] == "meeting_id"), None)
        if mid is None or isinstance(mid["type"], list):
            errors.append(
                f"{t}: meeting_id must be present and non-nullable (it is the second half of "
                f"this topic's composite {{org_id}}:{{meeting_id}} partition key, ADR-0017)"
            )

    if errors:
        print("VALIDATE FAILED:")
        for e in errors:
            print("  -", e)
        return 1
    print(f"validate OK: {len(TOPICS)} topics; envelope+dlq parse; header-first; frozen header order.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
