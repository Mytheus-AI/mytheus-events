"""Register every {topic}-value subject with the Schema Registry under BACKWARD.

Pins each subject's compatibility to BACKWARD (PUT /config/{subject}), then registers the
(envelope-inlined) schema (POST /subjects/{subject}/versions). Registration is idempotent:
re-posting an identical schema returns the existing id and does not create a new version.

    python codegen/register.py --registry http://localhost:8081 --subjects subjects

``--check`` is read-only (no PUT/POST): it compares every local topic schema (envelope
inlined, exactly what ``register`` would POST) with the registry's LATEST version of
``{topic}-value`` and exits 1 on ANY difference (missing subject, differing schema, including
doc/default/ordering-of-fields changes), unlike check_compat.py which only tests BACKWARD
compatibility. Catches "schema edited locally but never registered" drift.

    python codegen/register.py --check --registry http://localhost:8081
"""
from __future__ import annotations

import argparse
import difflib
import json
import sys

import httpx

from _schemas import TOPICS, inlined_schema


def _pretty(obj) -> list[str]:
    return json.dumps(obj, indent=2, sort_keys=False).splitlines()


def check_registered(client, topics=TOPICS) -> list[str]:
    """Compare local schemas to the registry's latest versions. Returns problem strings.

    Comparison is structural JSON equality of the parsed schema documents (object key order is
    irrelevant; field order, defaults, docs and types all count).
    """
    problems: list[str] = []
    for t in topics:
        subject = f"{t}-value"
        resp = client.get(f"/subjects/{subject}/versions/latest")
        if resp.status_code == 404:
            problems.append(f"{subject}: NOT REGISTERED (local schema schemas/{t}/v1.avsc)")
            continue
        resp.raise_for_status()
        body = resp.json()
        remote = json.loads(body["schema"])
        local = json.loads(json.dumps(inlined_schema(t)))
        if remote == local:
            print(f"in sync    {subject} (registry v{body.get('version')})")
            continue
        diff = "\n".join(
            difflib.unified_diff(
                _pretty(remote), _pretty(local),
                fromfile=f"registry:{subject}@v{body.get('version')}",
                tofile=f"local:schemas/{t}/v1.avsc",
                lineterm="", n=2,
            )
        )
        problems.append(f"{subject}: DIFFERS from registry latest\n{diff}")
    return problems


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--registry", default="http://localhost:8081")
    ap.add_argument("--subjects", default="subjects")  # accepted for CLI symmetry; not read
    ap.add_argument("--check", action="store_true",
                    help="read-only: fail if any local schema differs from the registry latest")
    args = ap.parse_args(argv)

    if args.check:
        with httpx.Client(base_url=args.registry, timeout=15.0) as client:
            problems = check_registered(client)
        if problems:
            print(f"\ncheck-registered FAILED: {len(problems)} of {len(TOPICS)} subjects "
                  "differ from the registry:\n", file=sys.stderr)
            for p in problems:
                print(p + "\n", file=sys.stderr)
            print("Fix: run `make register` (additive/BACKWARD changes) or revert the local "
                  "schema.", file=sys.stderr)
            return 1
        print(f"check-registered OK: all {len(TOPICS)} local schemas match the registry")
        return 0

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
