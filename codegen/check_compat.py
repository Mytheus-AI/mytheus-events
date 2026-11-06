"""BACKWARD compatibility gate against a Redpanda/Confluent Schema Registry.

For every {topic}-value subject, POST the (envelope-inlined) local schema to
/compatibility/subjects/{subject}/versions/latest and fail on is_compatible == false.
A subject with no registered version yet (404 / error 40401) is treated as compatible
(first registration). Non-zero exit on any incompatibility.

    python codegen/check_compat.py --registry http://localhost:8085 --subjects subjects
"""
from __future__ import annotations

import argparse
import json
import sys

import httpx

from _schemas import TOPICS, inlined_schema


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="http://localhost:8085")
    ap.add_argument("--subjects", default="subjects")  # accepted for CLI symmetry; not read
    args = ap.parse_args()

    incompatible: list[str] = []
    checked = 0
    with httpx.Client(base_url=args.registry, timeout=15.0) as client:
        for t in TOPICS:
            subject = f"{t}-value"
            schema_str = json.dumps(inlined_schema(t))
            url = f"/compatibility/subjects/{subject}/versions/latest"
            resp = client.post(url, json={"schema": schema_str, "schemaType": "AVRO"})
            if resp.status_code == 404:
                # no prior version registered -> first registration is trivially compatible
                continue
            resp.raise_for_status()
            checked += 1
            if not resp.json().get("is_compatible", False):
                incompatible.append(subject)

    if incompatible:
        print("CHECK-COMPATIBILITY FAILED (BACKWARD) for:")
        for s in incompatible:
            print("  -", s)
        return 1
    print(f"check-compatibility OK: {checked} subject(s) checked BACKWARD, "
          f"{len(TOPICS) - checked} not-yet-registered (first-registration).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
