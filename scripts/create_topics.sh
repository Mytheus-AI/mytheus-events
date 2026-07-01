#!/usr/bin/env bash
# Create the 21 NEXUS topics + their paired <topic>.dlq locally, with the canonical partition
# counts and retention from 07-event-architecture.md 4.2 and _canon/03-event-taxonomy.md.
# Prod partition counts are kept as-is; -r 1 (single replica) suits a local single-broker dev cluster.
# Usage: BROKERS=localhost:9092 bash scripts/create_topics.sh
set -euo pipefail
BROKERS="${BROKERS:-localhost:9092}"
RPK="rpk topic create --brokers ${BROKERS} -r 1"

# retention presets (ms)
H1=3600000; D1=86400000; D3=259200000; D7=604800000; D14=1209600000; D30=2592000000

mk()      { $RPK "$1" -p "$2" -c "retention.ms=$3" -c "cleanup.policy=delete"; }
mk_compact() { $RPK "$1" -p "$2" -c "cleanup.policy=compact"; }
mk_dlq()  { $RPK "$1.dlq" -p 1 -c "retention.ms=${D14}" -c "cleanup.policy=delete"; }

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
mk_compact admin-events 6    ; mk_dlq admin-events

echo "created 21 topics + 21 .dlq on ${BROKERS}"
