# Mytheus - Event Contracts (`mytheus-events`)

The single home for every Mytheus event contract: the Avro schemas, Schema Registry subjects, topic taxonomy, partition-key conventions, and the outbox/inbox/DLQ delivery contract that every service produces and consumes against.

## What this is

`mytheus-events` is the **event contracts & schema registry** repo for Mytheus (internal codename NEXUS). It owns:

- **Avro schemas** - the value schema for every Redpanda topic, opening with the shared `EventEnvelopeHeader` and a topic-specific payload.
- **Schema Registry subjects** - one subject per topic, named `{topic}-value`, registered under **BACKWARD** compatibility.
- **The topic taxonomy** - the canonical list of every topic, its producer, its consumers, its partition key, its retention, and its delivery/idempotency rule.
- **The delivery contract** - the transactional outbox on produce, the inbox + Redis dedupe on consume, and the per-topic DLQ that every service honors.
- **Generated client types** - the source of generated Python and TypeScript producer/consumer types that `mytheus-backend`, `mytheus-intelligence`, and `mytheus-realtime` depend on.

It is the **shared event vocabulary for all services** - the engineering realization of the frozen `_canon/03-event-taxonomy.md`. No service may invent a topic, rename a field, or break a subject; the contract lives here.

## The topic catalog

Every topic is partitioned by `org_id` (the tenant UUID string). Delivery is **at-least-once, idempotent in effect**: every consumer dedupes via the durable `inbox` table plus the Redis cache `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL). A paired `<topic>.dlq` (14-day retention) exists for every topic.

### Data-plane topics (PDF-carried)

| Topic | Producer | Consumers | Partition key | Purpose |
|---|---|---|---|---|
| `transcript-chunks` | `transcription-service` | `embedding-service`, `agent-runtime-service`, `search-service` | `org_id` | Finalized diarized utterances from the live meeting |
| `document-chunks` | `ingestion-service` | `embedding-service`, `agent-runtime-service` | `org_id` | Typed, split document elements from ingestion |
| `agent-results` | `agent-runtime-service` | `core-api`, `realtime-gateway`, `notification-service` | `org_id` | Agent output (notes, scores, suggestions) |
| `decisions` | `core-api` (outbox, on confirm) | `knowledge-graph-service`, `notification-service` | `org_id` | Confirmed decisions only |
| `actions` | `core-api` (outbox, on confirm) | `knowledge-graph-service`, `workflow-service`, `integration-service` | `org_id` | Confirmed action items only |
| `notifications` | multiple | `notification-service`, `realtime-gateway` | `org_id` | Cross-channel notification intents |
| `agent-requests` | `agent-runtime-service` | `agent-runtime-service` (peer agents) | `org_id` | Agent-to-agent context-fill requests |
| `agent-responses` | `agent-runtime-service` | `agent-runtime-service` (requester) | `org_id` | Agent-to-agent context-fill responses |
| `agent-stream` | `agent-runtime-service` (LiteLLM stream bridge) | `realtime-gateway` → WebSocket | `org_id` | Streamed LLM token deltas to the UI |

### Control-plane & observability topics (SaaS-scale)

| Topic | Producer | Consumers | Partition key | Purpose |
|---|---|---|---|---|
| `tenant-events` | `identity-tenant-service` | `billing-metering-service`, `admin-ops-service`, `workflow-service` | `org_id` | Tenant provisioned / suspended / deleted |
| `meeting-events` | `core-api` | `meeting-service`, `workflow-service`, `integration-service` | `org_id` | Meeting created / updated / ended |
| `meeting-room-events` | `meeting-service` | `transcription-service`, `core-api` | `org_id` | LiveKit joined / left / recording-ready |
| `integration-events` | `integration-service` | `ingestion-service`, `core-api` | `org_id` | Calendar→meeting, attachment-to-ingest, external status → action update |
| `usage-events` | `agent-runtime-service`, `rag-service` | `billing-metering-service` | `org_id` | Metered units (tokens/minutes/seconds/storage) |
| `billing-events` | `billing-metering-service` | `workflow-service`, `notification-service`, `admin-ops-service` | `org_id` | Quota-exceeded / dunning / plan-changed |
| `graph-events` | `knowledge-graph-service` | `admin-ops-service` | `org_id` | Edge created / lineage updated |
| `embedding-events` | `embedding-service` | (lag / metrics) | `org_id` | Upsert acks for lag tracking |
| `presence-events` | `realtime-gateway` | `core-api` (analytics) | `org_id` | Socket.io presence join/leave |
| `notification-delivery-events` | `notification-service` | `admin-ops-service`, `core-api` | `org_id` | Delivery receipts |
| `user-sync-events` | `identity-tenant-service` (Clerk inbox) | `identity-tenant-service` | `org_id` | Clerk webhook user-sync inbox |
| `admin-events` | `admin-ops-service` | `workflow-service`, `admin-ops-service` (audit-log write) | `org_id` | Flag changed / tenant suspended / erasure requested |

> Producer/consumer assignments are reconciled to the canon. Notably: `usage-events` is produced **only** by `agent-runtime-service` and `rag-service`; the `admin-events` audit-log write lands in `admin-ops-service` (there is no separate `audit-service`).

## Conventions

- **Avro values.** Every event value opens with the shared `EventEnvelopeHeader` (namespace `io.nexus.events`) then a topic-specific `payload`. No key schemas - the partition key is the raw `org_id` UUID string.
- **One subject per topic.** Subjects are named `{topic}-value`. There is exactly one value subject per topic.
- **BACKWARD compatibility.** A new consumer schema can always read data written by the previous producer schema. Additive, default-bearing changes only.
- **Partitioned by tenant.** Every topic is partitioned by `org_id`. `org_id` is mandatory on every event for tenant partitioning and isolation. No cross-tenant topic mixing.
- **At-least-once, idempotent in effect.** Transactional outbox on produce; inbox + Redis dedupe on consume; per-topic DLQ for poison messages.

## Tech stack

- **Avro** - the value serialization format for every topic.
- **Confluent/Redpanda Schema Registry** - subject registry and BACKWARD compatibility enforcement; the Confluent wire format (magic byte + schema id) lets consumers resolve the exact writer schema.
- **Code generation** - generated **Python** (pydantic / dataclasses) and **TypeScript** client types for producers and consumers, published as packages the other repos depend on.

## Repo structure

```
mytheus-events/
├── schemas/            # Avro schemas, versioned: schemas/<topic>/vN.avsc
│   └── <topic>/
│       └── v1.avsc
├── subjects/           # Schema Registry subject definitions ({topic}-value) + compat config
├── codegen/            # generators + config for Python (pydantic/dataclasses) + TypeScript clients
├── contracts/          # outbox / inbox / DLQ delivery-contract specs and shared envelope definition
├── docs/               # taxonomy, conventions, evolution guides
└── README.md
```

## Quick start

```bash
# 1. Add or evolve a schema (one subject per topic; new versions are additive).
#    schemas/<topic>/vN.avsc

# 2. Validate the Avro schemas.
make validate            # parse + lint every .avsc

# 3. Run BACKWARD compatibility checks against the registered subject.
make check-compatibility # gates the change against {topic}-value in the Registry

# 4. Register the subject version.
make register            # publishes {topic}-value to the Schema Registry

# 5. Generate clients.
make codegen             # Python (pydantic/dataclasses) + TypeScript types
```

To **add a topic**, you must first land it in `_canon/03-event-taxonomy.md` - topic names and partition keys are frozen there. This repo only realizes the canon; it never invents a topic.

## Versioning & evolution

The compatibility rule is **BACKWARD**, enforced in CI against the registered subject.

**Allowed**

- Add a field **with a default**.
- Remove a field that **had a default**.
- Widen a field via a union with `null` plus a default.
- Append enum symbols at the tail with a default.

**Forbidden**

- Add a required field **without** a default (breaks every lagging consumer).
- Remove a non-defaulted field.
- Rename a field (that is a remove + add - handle it as a Glossary entry + ADR first).
- Narrow a type.

To **add a field safely**: append it to the payload record with a default (or a `["null", T]` union defaulting to `null`), bump `schema_version`, run `check-compatibility`, register the new subject version, regenerate clients.

## Related repos

- `mytheus-backend` - CRUD / control-plane services (core-api, identity-tenant, meeting, billing-metering, integration, admin-ops, notification).
- `mytheus-intelligence` - AI / inference tier (agent-runtime, embedding, reranker, rag, ingestion, knowledge-graph, search).
- `mytheus-realtime` - realtime-gateway and transcription-service (WebSocket + live transcription).
- `mytheus-events` - **this repo**: event contracts, Avro schemas, Schema Registry subjects, delivery contract.
- `mytheus-platform` - infrastructure, Terraform/Kubernetes, Redpanda cluster, CI/CD.
- `mytheus-ui` - Next.js 14 web application (meeting room, agent sidebar).
- `mytheus-mobile` - mobile client.
- `mytheus-docs` - architecture documentation and ADRs.

## License

Proprietary © Mytheus. All rights reserved.
