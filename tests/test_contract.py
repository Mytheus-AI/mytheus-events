import glob
import os

from _schemas import (
    ENVELOPE_PATH, FROZEN_SCHEMAS, HEADER_FIELDS, SUBJECTS_DIR, TOPICS,
    load_raw, topic_schema_path,
)


def test_subject_per_topic_named_value_backward():
    for t in TOPICS:
        p = os.path.join(SUBJECTS_DIR, f"{t}-value.json")
        assert os.path.exists(p), f"missing subject descriptor {t}-value.json"
        d = load_raw(p)
        assert d["subject"] == f"{t}-value"
        assert d["compatibility"] == "BACKWARD"


def test_exactly_21_subjects():
    subs = glob.glob(os.path.join(SUBJECTS_DIR, "*-value.json"))
    assert len(subs) == 21


def test_every_value_record_starts_with_header():
    for t in TOPICS:
        raw = load_raw(topic_schema_path(t))
        f0 = raw["fields"][0]
        assert f0["name"] == "header", t
        assert f0["type"] in ("io.nexus.events.EventEnvelopeHeader", "EventEnvelopeHeader"), t


def test_header_frozen_order_and_org_id_mandatory():
    env = load_raw(ENVELOPE_PATH)
    assert [f["name"] for f in env["fields"]] == HEADER_FIELDS
    org = next(f for f in env["fields"] if f["name"] == "org_id")
    assert not isinstance(org["type"], list), "org_id must be non-nullable"


def test_decision_action_status_confirmed_only():
    for t in ("decisions", "actions"):
        raw = load_raw(topic_schema_path(t))
        st = next(f for f in raw["fields"] if f["name"] == "status")
        assert st["type"]["symbols"] == ["confirmed"], t


def test_seven_frozen_schema_names_present():
    found = {load_raw(topic_schema_path(t))["name"] for t in TOPICS}
    assert FROZEN_SCHEMAS.issubset(found)
