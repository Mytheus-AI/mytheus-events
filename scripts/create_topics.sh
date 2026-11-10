#!/usr/bin/env bash
# Create the 22 NEXUS topics + their paired <topic>.dlq, with the canonical partition counts
# and retention from 07-event-architecture.md 4.2 and _canon/03-event-taxonomy.md.
#
# IDEMPOTENT: safe to re-run against a cluster that already has some or all of the topics.
# `rpk topic create` exits 1 with TOPIC_ALREADY_EXISTS for a topic that is already there, and
# with `set -e` that aborted the whole script at the FIRST existing topic, leaving any topic
# later in the list uncreated and reporting failure. That made the script unusable from a
# deploy path, which is what `mytheus-platform/deploy/deploy.sh` needs it for: a deploy that
# provisions topics has to survive the second deploy.
#
# Replication is -r 1, which suits a single-broker cluster (dev, and the single-node VPS that
# ADR-0016 locks the MVP to). A multi-broker cluster wants -r 3; override with REPLICAS.
#
# Usage:
#   BROKERS=localhost:9092 bash scripts/create_topics.sh
#   BROKERS=redpanda:9092 REPLICAS=3 bash scripts/create_topics.sh
set -euo pipefail
BROKERS="${BROKERS:-localhost:9092}"
REPLICAS="${REPLICAS:-1}"

created=0; existed=0

# Treat TOPIC_ALREADY_EXISTS as success and anything else as a real failure. `rpk` reports the
# per-topic outcome in its table rather than only through the exit code, so the output is
# inspected instead of the status alone; a topic that fails for any OTHER reason must still
# abort the run rather than being silently counted as present.
create() {
  local name="$1"; shift
  local out rc=0
  out="$(rpk topic create --brokers "${BROKERS}" -r "${REPLICAS}" "$name" "$@" 2>&1)" || rc=$?
  if [[ $rc -eq 0 ]]; then
    created=$((created + 1))
    return 0
  fi
  if grep -q "TOPIC_ALREADY_EXISTS" <<<"$out"; then
    existed=$((existed + 1))
    return 0
  fi
  echo "ERROR: could not create topic ${name}" >&2
  echo "$out" | sed 's/^/   | /' >&2
  return 1
}

# retention presets (ms)
H1=3600000; D1=86400000; D3=259200000; D7=604800000; D14=1209600000; D30=2592000000; D365=31536000000

mk()      { create "$1" -p "$2" -c "retention.ms=$3" -c "cleanup.policy=delete"; }
mk_compact() { create "$1" -p "$2" -c "cleanup.policy=compact"; }
mk_dlq()  { create "$1.dlq" -p 1 -c "retention.ms=${D14}" -c "cleanup.policy=delete"; }

# --- data-plane ---
mk transcript-chunks 180 $D7 ; mk_dlq transcript-chunks
mk document-chunks    24 $D7 ; mk_dlq document-chunks
mk agent-results     120 $D3 ; mk_dlq agent-results
mk agent-stream      120 $H1 ; mk_dlq agent-stream
mk decisions          24 $D30; mk_dlq decisions
mk actions            24 $D30; mk_dlq actions
mk notifications      24 $D3 ; mk_dlq notifications
mk agent-requests     12 $D1 ; mk_dlq agent-requests
mk agent-responses    12 $D1 ; mk_dlq agent-responses

# --- control-plane / observability ---
mk_compact tenant-events 6   ; mk_dlq tenant-events
mk meeting-events      24 $D7 ; mk_dlq meeting-events
mk meeting-room-events 24 $D3 ; mk_dlq meeting-room-events
mk integration-events  12 $D7 ; mk_dlq integration-events
mk usage-events        48 $D7 ; mk_dlq usage-events
mk billing-events      12 $D30; mk_dlq billing-events
mk graph-events         6 $D7 ; mk_dlq graph-events
mk embedding-events    12 $D1 ; mk_dlq embedding-events
mk presence-events     12 $H1 ; mk_dlq presence-events
mk notification-delivery-events 6 $D30; mk_dlq notification-delivery-events
mk user-sync-events     6 $D7 ; mk_dlq user-sync-events
# Not compacted (polish wave 3): keyed by org_id, compaction could drop an older
# erasure_requested before every store's erasure consumer has read it.
mk admin-events 6 $D365     ; mk_dlq admin-events

# --- learning (polish wave 2): core-api outbox -> agent-runtime-service, core-api memory ---
mk agent-feedback      12 $D7 ; mk_dlq agent-feedback

echo "topics on ${BROKERS}: ${created} created, ${existed} already present (44 expected in total)"
