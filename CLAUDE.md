# CLAUDE.md

This is `reprise-events`, the frozen event vocabulary for Reprise AI: the Avro schemas, Schema Registry subjects, topic taxonomy, and outbox/inbox/DLQ delivery contract that every service produces and consumes against. The single most important thing: BACKWARD compatibility is mandatory (never break a subject), and every event carries `org_id` plus `idempotency_key` in its envelope. This repo realizes the canon; it never invents naming.

## Repo at a glance

`reprise-events` is the shared event contract consumed by `reprise-backend`, `reprise-intelligence`, and `reprise-realtime`. They depend on the generated Python and TypeScript client packages produced here rather than hand-rolling serializers, so a schema change here propagates as a versioned package bump there.

This repo is downstream of `_canon/03-event-taxonomy.md` (the frozen naming authority: topics, schemas, partition keys, retention) and `07-event-architecture.md` (the delivery machinery: outbox/inbox, dedupe, DLQ, backpressure). Those documents are authoritative. This repo implements them and must never contradict them. Any change to a frozen value lands in the canon first.

## Architecture orientation

Three layers define the system. Internalize them before touching a schema.

The backbone:
- One bus, Kafka API. Redpanda (Kafka API compatible), Avro values, Redpanda Schema Registry.
- One subject per topic, named `{topic}-value`, registered under BACKWARD compatibility.
- Every topic is partitioned by `org_id` (the tenant UUID string). This gives per-tenant ordering, isolation, and replay; a noisy tenant cannot starve or reorder another tenant's stream.
- The Confluent wire format prepends a magic byte plus a 4-byte schema id to every value, so a consumer resolves the exact writer schema from the Registry and projects it onto its (possibly newer) reader schema. Producers cache schema ids; the Registry is off the hot path after warm-up.

The delivery contract (at-least-once, idempotent in effect, which nets exactly-once application):
- Transactional outbox on produce. A producer never does "write DB, then publish" as two steps. The state change and the outbox row commit in one Postgres transaction. A separate relay publishes the outbox row to Redpanda. The outbox row carries `org_id`, `topic`, `partition_key` (the `org_id` string), `idempotency_key`, `schema_subject` (`{topic}-value`), and the payload. A unique index `(topic, idempotency_key)` means a double-confirm cannot enqueue a second event.
- Inbox dedupe on consume, keyed on `idempotency_key`, in two layers:
  1. Redis dedupe cache, a fast `SET NX EX` reject of obvious replays, keyed `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` with a 24h TTL.
  2. Durable inbox table, primary key `(consumer_group, org_id, idempotency_key)`, the authority when the cache has evicted.
  The inbox insert and the business effect commit in the same transaction, so a crash after the effect cannot double-apply. Each consumer group applies a given fact exactly once, independently of the others.
- Per-topic DLQ. A paired `<topic>.dlq` (14-day retention) exists for every topic. Transient failures (downstream timeout, 5xx, lock) retry in place with bounded exponential backoff plus jitter (5 attempts), offset not advanced. Poison failures (deserialize failure, schema-incompatible, business-invalid, max-retries) publish to the DLQ with full failure context (`DlqEnvelope`), advance the offset, and keep the partition moving. DLQ replay is an `admin-ops-service` operation that re-produces with the original `idempotency_key`, so the consumer's dedupe/inbox decides whether it is genuinely new.

The envelope. Every event value opens with the shared `EventEnvelopeHeader` (namespace `io.nexus.events`, frozen field order) then a topic-specific `payload`. Header fields:

| Field | Type | Role |
|---|---|---|
| `event_id` | `string` (uuid) | Unique per physical emission; distinct from `idempotency_key`. |
| `idempotency_key` | `string` | Business dedupe key; stable across producer retries of the same fact. |
| `org_id` | `string` (uuid) | Tenant root; the Kafka partition key; isolation anchor. Mandatory on every event. |
| `schema_version` | `int` (default 1) | Producer's payload schema version (observability; Registry enforces compat independently). |
| `producer` | `string` | Owning service name (kebab-case, from the Service Catalog). |
| `occurred_at` | `long` (timestamp-micros) | When the fact happened (UTC); not enqueue time. |
| `correlation_id` | `["null","string"]` (default null) | Ties a causal chain across topics. |
| `causation_id` | `["null","string"]` (default null) | `event_id` of the direct cause; null for root events. |

## Topic catalog

All 21 topics. Partition key is `org_id` on every topic. The value subject is always `{topic}-value`. A paired `<topic>.dlq` (14-day retention) exists for every topic. Producer/consumer assignments are reconciled to `_canon/01-service-catalog.md`.

Data-plane topics:

| Topic | Producer | Consumers | Partition key |
|---|---|---|---|
| `transcript-chunks` | `transcription-service` | `embedding-service`, `agent-runtime-service`, `search-service` | `org_id` |
| `document-chunks` | `ingestion-service` | `embedding-service`, `agent-runtime-service` | `org_id` |
| `agent-results` | `agent-runtime-service` | `core-api`, `realtime-gateway`, `notification-service` | `org_id` |
| `decisions` | `core-api` (outbox, on confirm) | `knowledge-graph-service`, `notification-service` | `org_id` |
| `actions` | `core-api` (outbox, on confirm) | `knowledge-graph-service`, `workflow-service`, `integration-service` | `org_id` |
| `notifications` | multiple | `notification-service`, `realtime-gateway` | `org_id` |
| `agent-requests` | `agent-runtime-service` | `agent-runtime-service` (peer agents) | `org_id` |
| `agent-responses` | `agent-runtime-service` | `agent-runtime-service` (requester) | `org_id` |
| `agent-stream` | `agent-runtime-service` (LiteLLM stream bridge) | `realtime-gateway` (to WebSocket) | `org_id` |

Control-plane and observability topics:

| Topic | Producer | Consumers | Partition key |
|---|---|---|---|
| `tenant-events` | `identity-tenant-service` | `billing-metering-service`, `admin-ops-service`, `workflow-service` | `org_id` |
| `meeting-events` | `core-api` | `meeting-service`, `workflow-service`, `integration-service` | `org_id` |
| `meeting-room-events` | `meeting-service` | `transcription-service`, `core-api` | `org_id` |
| `integration-events` | `integration-service` | `ingestion-service`, `core-api` | `org_id` |
| `usage-events` | `agent-runtime-service`, `rag-service` (only) | `billing-metering-service` | `org_id` |
| `billing-events` | `billing-metering-service` | `workflow-service`, `notification-service`, `admin-ops-service` | `org_id` |
| `graph-events` | `knowledge-graph-service` | `admin-ops-service` | `org_id` |
| `embedding-events` | `embedding-service` | (lag / metrics) | `org_id` |
| `presence-events` | `realtime-gateway` | `core-api` (analytics) | `org_id` |
| `notification-delivery-events` | `notification-service` | `admin-ops-service`, `core-api` | `org_id` |
| `user-sync-events` | `identity-tenant-service` (Clerk inbox) | `identity-tenant-service` | `org_id` |
| `admin-events` | `admin-ops-service` | `workflow-service`, `admin-ops-service` (audit-log write) | `org_id` |

Reconciliation notes that bite if you get them wrong: `usage-events` is produced only by `agent-runtime-service` and `rag-service` (transcription minutes are metered by `billing-metering-service`, not emitted by `transcription-service`). The `admin-events` audit-log write lands in `admin-ops-service`; there is no separate `audit-service`.

Frozen Avro schema names (these seven are canon): `TranscriptChunk`, `AgentResult`, `Decision`, `Action`, `Notification`, `AgentRequest`, `AgentResponse`. The schema names `DocumentChunk`, `AgentStreamDelta`, and the `Tenant/Meeting/...Event` family are net-new in this repo and not frozen by canon, but their topic names are.

## Where things live

```
schemas/     # Avro schemas, versioned: schemas/<topic>/vN.avsc (shared EventEnvelopeHeader + topic payload)
subjects/    # Schema Registry subject definitions ({topic}-value) + compatibility config
codegen/     # generators + config for Python (pydantic/dataclasses) and TypeScript clients
contracts/   # outbox / inbox / DLQ delivery-contract specs and the shared envelope definition
docs/        # taxonomy, conventions, evolution guides
```

- `schemas/<topic>/vN.avsc`: the value schema per topic, one file per version. This is the artifact CI validates and registers.
- `subjects/`: maps each `{topic}-value` subject to its schema and pins compatibility to BACKWARD.
- `codegen/`: turns the `.avsc` files into the Python and TypeScript packages the other repos import. Do not hand-edit generated output.
- `contracts/`: the outbox/inbox/DLQ specs and `EventEnvelopeHeader`/`DlqEnvelope` definitions that producers and consumers implement.
- `docs/`: human-facing taxonomy and evolution guides; secondary to the canon.

## Invariants you must not break

| Rule | Why | If you break it |
|---|---|---|
| Every event carries `org_id` and `idempotency_key` in the `EventEnvelopeHeader` | `org_id` is the partition key and tenant isolation anchor; `idempotency_key` is the dedupe key stable across retries | Events land on the wrong partition or cannot be deduped; tenant isolation and exactly-once-in-effect both fail |
| BACKWARD compatibility is mandatory; never break a subject | At SaaS scale ~20 services upgrade independently; a lagging consumer must still read newer data | A single non-defaulted required field breaks every lagging consumer on the partition |
| One subject per topic, named `{topic}-value` | The subject naming is frozen; there are no key schemas, the key is the raw `org_id` string | Producers and consumers resolve the wrong subject; codegen and Registry diverge |
| Renames go via the Glossary plus an ADR first, never silently | A rename is a remove plus add, which BACKWARD forbids | The compatibility gate fails, or worse, a silent rename ships and consumers lose the field |
| Producers use the outbox; consumers dedupe via inbox plus DLQ | At-least-once delivery only becomes exactly-once-in-effect with idempotency on both ends | Dropped events on producer crash, or duplicate application, or a poison message wedges a partition |

## Naming authority

This repo implements `_canon/03-event-taxonomy.md`. Topic names, partition keys, and Avro field names are frozen there. Never invent a topic, never add a second subject for a topic, never rename a field, and never use a non-`org_id` partition key. Keep every identifier verbatim:

- The 21 topic names listed in the Topic catalog above.
- The Redis dedupe pattern: `nexus:{org_id}:dedupe:{topic}:{idempotency_key}`.
- The envelope fields: `event_id`, `org_id`, `idempotency_key`, `correlation_id`, `occurred_at`, `schema_version` (plus `producer`, `causation_id`).
- The agent-to-agent field names on `agent-requests` and `agent-responses`: `requester_agent_id`, `responder_target`, `correlation_id`, `deadline`.

To add a topic or change a partition key, land it in the canon first. This repo only realizes the canon.

## Idempotency keys per topic

These are the canonical key shapes. A new event must build its key from these:

- `transcript-chunks`: `chunk:{chunk_id}`
- `document-chunks`: `docchunk:{document_id}:{element_id}`
- `agent-results`: `agentresult:{meeting_id}:{window_id}:{user_id}:{agent}`
- `decisions`: `decision:{decision_id}:confirmed`
- `actions`: `action:{action_id}:confirmed`
- `notifications`: `notif:{source_event_id}:{channel}:{user_id}`
- `agent-requests`: `areq:{correlation_id}`
- `usage-events`: `usage:{org_id}:{event_type}:{occurred_at}:{source_id}`

## Agent-to-agent contract

`agent-requests` and `agent-responses` carry the agent-to-agent collaboration used by the `ContextGapAgent`. The payloads carry `requester_agent_id`, `responder_target`, `correlation_id`, and `deadline`. Correlation is by `correlation_id`, not by partition. Both topics are partitioned by `org_id`, so a request and its response stay within one tenant. Deny-by-default authorization is enforced by the consuming agents (org/role-scoped and audited, per ADR-0013): a peer agent answers only requests it is authorized to serve. Past the `deadline`, a request has no value and is not served.

## Confirmation-only edges

`decisions` and `actions` are produced only on user confirmation by `core-api`, never speculatively by an agent. The flow:
1. The agent surfaces a candidate via `agent-results`.
2. The user confirms in the sidebar.
3. `core-api` writes the row and the outbox record in one transaction (`UPDATE ... SET status = confirmed` plus `INSERT outbox`), with key `decision:{decision_id}:confirmed` or `action:{action_id}:confirmed`.
4. The relay publishes to `decisions` / `actions`.
5. `knowledge-graph-service` consumes and applies idempotent Cypher `MERGE` (never `CREATE`).

Four guards make this duplicate-proof: stable idempotency key, outbox unique index, consumer inbox/dedupe, then `MERGE`.

## Common tasks (recipes)

1. Add a field to an existing schema (BACKWARD-safe).
   - Append the field to the topic payload record in `schemas/<topic>/vN.avsc` (or a new `vN+1.avsc`) with a default. For a nullable field use a `["null", T]` union defaulting to `null`. Never make it required without a default.
   - Bump `schema_version` in the payload.
   - Run `make validate` then `make check-compatibility` (the BACKWARD gate against `{topic}-value`).
   - `make register`, then `make codegen` to refresh clients.
   - Update or add the golden sample for the topic and run `make test`.

2. Add a new topic/subject (only after it exists in the canon).
   - Confirm the topic name and partition key are frozen in `_canon/03-event-taxonomy.md`. If not, stop and land it in the canon first.
   - Create `schemas/<topic>/v1.avsc` with the `EventEnvelopeHeader` plus the topic payload.
   - Add the `{topic}-value` subject in `subjects/` pinned to BACKWARD.
   - Add the topic to the Topic catalog with its producer/consumers reconciled to `_canon/01-service-catalog.md`.
   - `make validate`, `make register`, `make codegen`, add a golden sample, `make test`.

3. Bump a schema version.
   - Add `schemas/<topic>/vN+1.avsc` (additive changes only) and bump `schema_version`.
   - Subject versions register monotonically; never rewrite a registered version.
   - Gate with `make check-compatibility` before `make register`.

4. Regenerate Python and TypeScript clients.
   - `make codegen`. Commit the regenerated packages; downstream repos pick them up as a version bump. Do not hand-edit generated output.

5. Add a golden sample event plus contract test.
   - Add a canonical sample event for the topic under the golden fixtures, serialized and round-tripped through the generated clients.
   - The contract test asserts producer/consumer assignment, subject naming `{topic}-value`, envelope presence, and `org_id` mandatory-ness. Run `make test`.

## Testing and CI

- Schema validation: parse and lint every `.avsc`.
- Schema Registry compatibility checks: `check-compatibility` runs against the registered subject under BACKWARD. This is the gate that blocks breaking changes before merge.
- Contract tests: verify producer/consumer assignments, subject naming (`{topic}-value`), envelope presence, and `org_id` mandatory-ness for every topic.
- Golden samples: checked-in canonical events per topic, round-tripped through the generated clients, so codegen changes are caught against fixtures.

CI on every PR: validate Avro, run the BACKWARD compatibility check (block breaking changes), then build and (on merge to trunk) publish the Python and TypeScript packages. An incompatible subject change fails the build and cannot merge.

## Local development

Prerequisites: a running Redpanda Schema Registry (local or shared) for the compatibility check, plus the codegen toolchain (Python and Node).

```bash
make validate            # parse + lint every Avro schema
make check-compatibility # BACKWARD check against the registered {topic}-value subject
make register            # register the new subject version
make codegen             # generate Python (pydantic/dataclasses) + TypeScript clients
make test                # schema-compatibility tests, contract tests, golden samples
```

Run `make validate` and `make check-compatibility` before opening a PR. CI runs the same compatibility check.

## Pitfalls

- Adding a required field without a default. Breaks BACKWARD and every lagging consumer. Always give a default or a `["null", T]` union defaulting to `null`.
- Renaming a field. A rename is a remove plus add; BACKWARD forbids it. Route it through the Glossary plus an ADR first.
- Removing a field that is still consumed, or removing a non-defaulted field. Breaks readers.
- Forgetting `org_id` or `idempotency_key` in the envelope. The event cannot be partitioned or deduped.
- Introducing a topic not in the canon, or a second subject for a topic. Naming is frozen in `_canon/03-event-taxonomy.md`.
- Mismatched partition key. Every topic is partitioned by `org_id`; the proposed `{org_id}:{meeting_id}` sub-key is not ratified and is not used.
- Narrowing a type, or reordering the frozen `EventEnvelopeHeader` fields.

## Working with other repos

`reprise-backend`, `reprise-intelligence`, and `reprise-realtime` depend on the generated client packages published from this repo rather than hand-rolling serializers, so a schema change here propagates as a versioned package bump there. Every producer must publish through the transactional outbox, and every consumer must dedupe via the inbox plus Redis cache and route poison messages to `<topic>.dlq`. The sanctioned producer (outbox relay, Avro-serializing against `{topic}-value`) and the base consumer (Avro-deserializing, manual offset commit after the effect, dedupe gate, DLQ routing) consume these generated types.

## Commit and PR conventions

- A schema change is an API change: open a PR with review required. No schema lands without review.
- Semantic subject versioning: `schemas/<topic>/vN.avsc`, registered monotonically.
- Conventional commits; trunk-based development with short-lived branches behind the compatibility gate.
- Include the BACKWARD compatibility check result and updated golden samples in the PR.
- Commits in this project must NOT include a `Co-Authored-By` footer.
