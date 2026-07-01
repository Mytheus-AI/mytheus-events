"""Register every {topic}-value subject with the Schema Registry under BACKWARD.

Pins each subject's compatibility to BACKWARD (PUT /config/{subject}), then registers the
(envelope-inlined) schema (POST /subjects/{subject}/versions). Registration is idempotent:
re-posting an identical schema returns the existing id and does not create a new version.

    python codegen/register.py --registry http://localhost:8081 --subjects subjects
"""
from __future__ import annotations

import argparse
import json
import sys

import httpx

from _schemas import TOPICS, inlined_schema


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="http://localhost:8081")
    ap.add_argument("--subjects", default="subjects")  # accepted for CLI symmetry; not read
    args = ap.parse_args()

    with httpx.Client(base_url=args.registry, timeout=15.0) as client:
        # registry-wide default
        client.put("/config", json={"compatibility": "BACKWARD"})
        for t in TOPICS:
            subject = f"{t}-value"
            client.put(f"/config/{subject}", json={"compatibility": "BACKWARD"})
            schema_str = json.dumps(inlined_schema(t))
            resp = client.post(
                f"/subjects/{subject}/versions",
                json={"schema": schema_str, "schemaType": "AVRO"},
            )
            resp.raise_for_status()
            print(f"registered {subject} -> id {resp.json().get('id')}")

    print(f"register OK: {len(TOPICS)} subjects under BACKWARD")
    return 0


if __name__ == "__main__":
    sys.exit(main())
