"""Shared Avro schema loading + canonical constants for mytheus-events codegen and tests.

Topic value schemas compose ``io.nexus.events.EventEnvelopeHeader`` by name. fastavro
resolves that reference from a shared ``named_schemas`` namespace: parse the envelope first,
then each topic schema into the same namespace. This module is the single place that knows
the 21 canonical topics, the frozen 8-field header order, and the 7 frozen schema names.
"""
from __future__ import annotations

import json
import os

from fastavro import parse_schema

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMAS_DIR = os.path.join(ROOT, "schemas")
SUBJECTS_DIR = os.path.join(ROOT, "subjects")
FIXTURES_DIR = os.path.join(ROOT, "fixtures")
ENVELOPE_PATH = os.path.join(SCHEMAS_DIR, "_envelope", "EventEnvelopeHeader.avsc")
DLQ_PATH = os.path.join(ROOT, "contracts", "DlqEnvelope.avsc")

# The 8 frozen EventEnvelopeHeader fields, in frozen order (07-event-architecture.md 3.1).
HEADER_FIELDS = [
    "event_id", "idempotency_key", "org_id", "schema_version",
    "producer", "occurred_at", "correlation_id", "causation_id",
]

# The 21 canonical topics, in canon order. Partition key is org_id alone on 19 of them;
# transcript-chunks and agent-stream use the composite {org_id}:{meeting_id} key instead
# (COMPOSITE_PARTITION_KEY_TOPICS below; ADR-0017 in mytheus-docs, _canon/03-event-taxonomy.md).
TOPICS = [
    "transcript-chunks", "document-chunks", "agent-results", "decisions", "actions",
    "notifications", "agent-requests", "agent-responses", "agent-stream",
    "tenant-events", "meeting-events", "meeting-room-events", "integration-events",
    "usage-events", "billing-events", "graph-events", "embedding-events",
    "presence-events", "notification-delivery-events", "user-sync-events", "admin-events",
]

# Topics partitioned by the composite {org_id}:{meeting_id} key rather than org_id alone
# (ADR-0017). org_id remains mandatory and non-nullable on these too — it is still the
# tenant key and the leading component of the composite key, just not the whole key.
# Every other topic in TOPICS keeps the org_id-only partition key.
COMPOSITE_PARTITION_KEY_TOPICS = {"transcript-chunks", "agent-stream"}

# The seven Avro schema names frozen by canon (_canon/06-tech-decisions.md 4). All others net-new.
FROZEN_SCHEMAS = {
    "TranscriptChunk", "AgentResult", "Decision", "Action",
    "Notification", "AgentRequest", "AgentResponse",
}

# Idempotency-key formulas per topic (verbatim from _canon/03-event-taxonomy.md / system.md).
IDEMPOTENCY_KEYS = {
    "transcript-chunks": "chunk:{chunk_id}",
    "document-chunks": "docchunk:{document_id}:{element_id}",
    "agent-results": "agentresult:{meeting_id}:{window_id}:{user_id}:{agent}",
    "decisions": "decision:{decision_id}:confirmed",
    "actions": "action:{action_id}:confirmed",
    "notifications": "notif:{source_event_id}:{channel}:{user_id}",
    "agent-requests": "areq:{correlation_id}",
    "usage-events": "usage:{org_id}:{event_type}:{occurred_at}:{source_id}",
}


def load_raw(path):
    with open(path) as f:
        return json.load(f)


def topic_schema_path(topic):
    return os.path.join(SCHEMAS_DIR, topic, "v1.avsc")


def build_named_schemas():
    """Register the envelope + DLQ so topic references resolve. Returns a fresh dict."""
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    parse_schema(load_raw(DLQ_PATH), named_schemas=ns)
    return ns


def load_topic(topic, ns=None):
    """Return a parsed, fully-resolved schema for one topic value.

    A fresh namespace per call keeps enum/record names from colliding across topics and
    yields a self-contained schema that schemaless_writer/reader resolve directly.
    """
    ns = build_named_schemas() if ns is None else ns
    return parse_schema(load_raw(topic_schema_path(topic)), named_schemas=ns)


def load_all():
    """Return {topic: parsed_schema} for all 21 topics."""
    return {t: load_topic(t) for t in TOPICS}


def inlined_schema(topic):
    """Return a fully self-contained parsed schema (EventEnvelopeHeader inlined).

    The Schema Registry needs each subject's schema to be self-contained (or to use the
    references API). Inlining the envelope via ``expand=True`` avoids the references machinery
    and keeps registration/compat one POST per subject.
    """
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns, _write_hint=False)
    return parse_schema(
        load_raw(topic_schema_path(topic)), named_schemas=ns, expand=True, _write_hint=False
    )
