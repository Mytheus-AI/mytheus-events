import io
import json
import os

from fastavro import schemaless_reader, schemaless_writer

from _schemas import FIXTURES_DIR, TOPICS, load_topic


def test_all_21_topics_have_golden_and_roundtrip():
    assert len(TOPICS) == 22
    for t in TOPICS:
        schema = load_topic(t)
        with open(os.path.join(FIXTURES_DIR, f"{t}.json")) as f:
            rec = json.load(f)
        buf = io.BytesIO()
        schemaless_writer(buf, schema, rec)
        buf.seek(0)
        back = schemaless_reader(buf, schema)
        # uuid logicalType decodes to uuid.UUID; compare string forms.
        assert str(back["header"]["org_id"]) == str(rec["header"]["org_id"]), t
        assert "idempotency_key" in back["header"], t
