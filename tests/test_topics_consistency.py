"""Topic list / count / partition / retention consistency across the repo and canon.

schemas/ has 22 entries = 21 topics + `_envelope` (shared header, not a topic).
subjects/ has 22 entries = 21 `{topic}-value.json` + `compatibility.json`.
scripts/create_topics.sh must create exactly those 21 topics + 21 `.dlq` = 42.

Partition counts are NOT in _canon/03-event-taxonomy.md (it has retention/cleanup only); the
canonical counts live in 07-event-architecture.md 4.2, so that table is used for partitions
and canon 03 for retention/cleanup. Tests skip if the sibling mytheus-docs repo is absent.
"""
import os
import re
import shutil
import subprocess

import pytest
from _schemas import ROOT, SCHEMAS_DIR, SUBJECTS_DIR, TOPICS

SCRIPT = os.path.join(ROOT, "scripts", "create_topics.sh")
DOCS = os.path.join(os.path.dirname(ROOT), "mytheus-docs")
CANON = os.path.join(DOCS, "_canon", "03-event-taxonomy.md")
ARCH = os.path.join(DOCS, "07-event-architecture.md")

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
PRESETS = {"H1": HOUR_MS, "D1": DAY_MS, "D3": 3 * DAY_MS, "D7": 7 * DAY_MS,
           "D14": 14 * DAY_MS, "D30": 30 * DAY_MS}


def parse_script():
    """-> ({topic: (partitions, retention_ms|'compact')}, [dlq topic bases])."""
    topics, dlqs = {}, []
    for line in open(SCRIPT):
        for stmt in line.split(";"):
            stmt = stmt.strip()
            m = re.match(r"mk_compact\s+(\S+)\s+(\d+)$", stmt)
            if m:
                topics[m[1]] = (int(m[2]), "compact")
                continue
            m = re.match(r"mk\s+(\S+)\s+(\d+)\s+\$(\w+)$", stmt)
            if m:
                topics[m[1]] = (int(m[2]), PRESETS[m[3]])
                continue
            m = re.match(r"mk_dlq\s+(\S+)$", stmt)
            if m:
                dlqs.append(m[1])
    return topics, dlqs


def test_counts_and_lists_agree():
    schema_dirs = sorted(d for d in os.listdir(SCHEMAS_DIR)
                         if os.path.isdir(os.path.join(SCHEMAS_DIR, d)))
    assert [d for d in schema_dirs if d.startswith("_")] == ["_envelope"]
    assert len(schema_dirs) == 22
    topic_dirs = [d for d in schema_dirs if not d.startswith("_")]
    assert sorted(TOPICS) == topic_dirs and len(TOPICS) == 21

    subj = sorted(os.listdir(SUBJECTS_DIR))
    assert "compatibility.json" in subj and len(subj) == 22
    assert sorted(f[: -len("-value.json")] for f in subj if f.endswith("-value.json")) == topic_dirs

    script_topics, dlqs = parse_script()
    assert sorted(script_topics) == topic_dirs, "create_topics.sh drifted from schemas/"
    assert sorted(dlqs) == topic_dirs, "every topic needs exactly one .dlq in create_topics.sh"
    assert len(dlqs) == len(set(dlqs))


def _canon_rows():
    rows = {}
    for line in open(CANON):
        m = re.match(r"\| `([a-z-]+)` \|.*\| ([^|]+?) \| [^|]* \|\s*$", line)
        if m and m[1] in TOPICS:
            rows[m[1]] = line.split("|")[6].strip()  # Retention (cleanup) column
    return rows


def _arch_partitions():
    out = {}
    text = open(ARCH).read().split("### 4.2 Partition counts and retention")[1].split("\n---")[0]
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 4 or not cells[0].startswith("`"):
            continue
        names = re.findall(r"`([a-z-]+)`", cells[0])
        counts = [int(x) for x in re.findall(r"\d+", cells[1].replace("*", ""))]
        assert len(names) == len(counts), line
        out.update(zip(names, counts, strict=True))
    return out


def _retention_ms(cell):
    cell = cell.replace("*", "")
    if "compact" in cell:
        return "compact"
    m = re.match(r"(\d+) (day|hour)s?", cell)
    return int(m[1]) * (DAY_MS if m[2] == "day" else HOUR_MS)


needs_docs = pytest.mark.skipif(not (os.path.exists(CANON) and os.path.exists(ARCH)),
                                reason="sibling mytheus-docs repo not present")


@needs_docs
def test_partitions_match_canon():
    script_topics, _ = parse_script()
    canon = _arch_partitions()
    assert sorted(canon) == sorted(TOPICS)
    mism = {t: (script_topics[t][0], canon[t]) for t in TOPICS if script_topics[t][0] != canon[t]}
    assert not mism, f"partition mismatch (script, canon): {mism}"


@needs_docs
def test_retention_matches_canon():
    script_topics, _ = parse_script()
    canon = _canon_rows()
    assert sorted(canon) == sorted(TOPICS)
    mism = {t: (script_topics[t][1], _retention_ms(canon[t])) for t in TOPICS
            if script_topics[t][1] != _retention_ms(canon[t])}
    assert not mism, f"retention mismatch (script, canon): {mism}"


@pytest.mark.skipif(shutil.which("bash") is None, reason="needs bash")
def test_create_topics_is_idempotent(tmp_path):
    """Run the script twice against a fake `rpk` that fails TOPIC_ALREADY_EXISTS on repeats."""
    state = tmp_path / "state"
    state.mkdir()
    rpk = tmp_path / "bin" / "rpk"
    rpk.parent.mkdir()
    rpk.write_text(
        '#!/usr/bin/env bash\n'
        '# usage: rpk topic create --brokers B -r N <name> [-p N] [-c k=v ...]\n'
        f'S="{state}"\n'
        'shift 2; shift 2; shift 2   # topic create / --brokers B / -r N\n'
        'name="$1"; shift\n'
        'if [[ -e "$S/$name" ]]; then echo "$name  TOPIC_ALREADY_EXISTS" ; exit 1; fi\n'
        'echo "$name $*" > "$S/$name"\n'
    )
    rpk.chmod(0o755)
    env = {**os.environ, "PATH": f"{rpk.parent}:{os.environ['PATH']}"}
    first = subprocess.run(["bash", SCRIPT], env=env, capture_output=True, text=True)
    assert first.returncode == 0, first.stderr
    assert "42 created, 0 already present" in first.stdout
    second = subprocess.run(["bash", SCRIPT], env=env, capture_output=True, text=True)
    assert second.returncode == 0, second.stderr
    assert "0 created, 42 already present" in second.stdout
    assert len(os.listdir(state)) == 42
