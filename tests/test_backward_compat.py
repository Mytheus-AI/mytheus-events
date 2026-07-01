import copy
import io
import json
import os

from fastavro import parse_schema, schemaless_reader, schemaless_writer

from _schemas import ENVELOPE_PATH, FIXTURES_DIR, TOPICS, load_raw, topic_schema_path


def _parsed(raw):
    ns = {}
    parse_schema(load_raw(ENVELOPE_PATH), named_schemas=ns)
    return parse_schema(raw, named_schemas=ns)


def test_additive_defaulted_field_is_backward_readable():
    """A v2 that appends one defaulted field must still read v1-written bytes (BACKWARD).

    Proves the additive-only discipline mechanically: bytes written with v1 are read with the
    newer v2 reader schema and the new field materializes as its default.
    """
    for t in TOPICS:
        v1_raw = load_raw(topic_schema_path(t))
        v1 = _parsed(copy.deepcopy(v1_raw))
        with open(os.path.join(FIXTURES_DIR, f"{t}.json")) as f:
            rec = json.load(f)

        buf = io.BytesIO()
        schemaless_writer(buf, v1, rec)
        v1_bytes = buf.getvalue()

        v2_raw = copy.deepcopy(v1_raw)
        v2_raw["fields"].append({"name": "extra_note", "type": ["null", "string"], "default": None})
        v2 = _parsed(v2_raw)

        back = schemaless_reader(io.BytesIO(v1_bytes), v1, reader_schema=v2)
        assert back["extra_note"] is None, t
