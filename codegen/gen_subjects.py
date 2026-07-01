"""Generate one Schema Registry subject descriptor per topic + a compatibility manifest.

subjects/{topic}-value.json -> {subject, schema, compatibility: BACKWARD}
subjects/compatibility.json  -> registry-wide default BACKWARD + a per-subject pin map.

One subject per topic, named {topic}-value. No key schemas (the partition key is the raw
org_id UUID string).
"""
from __future__ import annotations

import json
import os

from _schemas import SUBJECTS_DIR, TOPICS


def main() -> None:
    os.makedirs(SUBJECTS_DIR, exist_ok=True)
    subjects = []
    for t in TOPICS:
        subject = f"{t}-value"
        desc = {"subject": subject, "schema": f"schemas/{t}/v1.avsc", "compatibility": "BACKWARD"}
        with open(os.path.join(SUBJECTS_DIR, f"{subject}.json"), "w") as f:
            json.dump(desc, f, indent=2)
            f.write("\n")
        subjects.append(subject)
    manifest = {"default": "BACKWARD", "subjects": {s: "BACKWARD" for s in subjects}}
    with open(os.path.join(SUBJECTS_DIR, "compatibility.json"), "w") as f:
        json.dump(manifest, f, indent=2)
        f.write("\n")
    print(f"generated {len(subjects)} subject descriptors + compatibility.json (all BACKWARD)")


if __name__ == "__main__":
    main()
