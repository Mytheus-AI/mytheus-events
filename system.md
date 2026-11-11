# Mytheus - Event Contracts: System Design (`mytheus-events`)

Deep reference for the event contracts that bind every Mytheus service together. This repo is the single source of event truth, downstream of the frozen `_canon/03-event-taxonomy.md` and `07-event-architecture.md`. Every topic name, partition key, and Avro field name is reproduced here verbatim from the canon.

---

## Purpose

`mytheus-events` is the **single source of event truth** for Mytheus (internal codename NEXUS). It holds the Avro schemas, the Schema Registry subject definitions, the topic taxonomy, the partition-key conventions, and the outbox/inbox/DLQ delivery contract. It is the engineering realization of the frozen naming authority `_canon/03-event-taxonomy.md` (the topic catalog) and `07-event-architecture.md` (the delivery machinery). Those documents are authoritative; this repo implements them and must never contradict them. Any change to a frozen value lands in the canon first.

The repo is also the **producer of generated client types**: `mytheus-backend`, `mytheus-intelligence`, and `mytheus-realtime` consume the generated Python and TypeScript packages rather than hand-rolling serializers.

---

## The event backbone

Five invariants bind every topic:

1. **One bus, Kafka API.** Redpanda (Kafka API compatible), Avro values, Redpanda Schema Registry. One subject per topic, named `{topic}-value`, **BACKWARD** compatibility.
2. **Partition key = tenant.** Every topic is partitioned by `org_id`. Per-tenant ordering, isolation, replay, and throughput control. A noisy tenant cannot starve or reorder another's stream.
3. **At-least-once, idempotent in effect.** Producers write through a transactional **outbox** in the same DB transaction as their state change. Consumers dedupe through an **inbox** table plus a Redis dedupe cache keyed by `idempotency_key`. Net effect is exactly-once application.
4. **Poison messages quarantine, never block.** Every topic has a paired **DLQ** with a bounded retry policy; one bad message never wedges a partition.
5. **No silent commits.** `decisions` and `actions` are emitted **only on user confirmation**.

The Confluent wire format prepends a magic byte + 4-byte schema id to every value, so consumers resolve the exact writer schema from the Registry and project it onto their (possibly newer) reader schema. Producers cache schema ids; the Registry is off the hot path after warm-up.

---

## The complete topic taxonomy

All 22 topics (9 PDF-original data-plane + 12 SaaS-scale control-plane/observability + agent-feedback). Partition key is `org_id` on **every** topic. The proposed `{org_id}:{meeting_id}` hot-tenant sub-key is **not ratified** and is not used. A paired `<topic>.dlq` (14-day retention) exists for every topic.

Every event value opens with the shared `EventEnvelopeHeader` (namespace `io.nexus.events`) then a topic-specific payload. Each topic's value subject is `{topic}-value`.

### Data-plane topics

#### `transcript-chunks`
- **Producer:** `transcription-service`
- **Consumers:** `embedding-service`, `agent-runtime-service`, `search-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `transcript-chunks-value` (schema `TranscriptChunk`)
- **Purpose:** Finalized diarized utterances from the live meeting. Only finals hit the bus; interims never do.
- **Idempotency key:** `chunk:{chunk_id}`

#### `document-chunks`
- **Producer:** `ingestion-service`
- **Consumers:** `embedding-service`, `agent-runtime-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `document-chunks-value` (schema `DocumentChunk` †)
- **Purpose:** Typed, split document elements from ingestion. Carries no mandatory `meeting_id`.
- **Idempotency key:** `docchunk:{document_id}:{element_id}`

#### `agent-results`
- **Producer:** `agent-runtime-service`
- **Consumers:** `core-api`, `realtime-gateway`, `notification-service`
- **Partition key:** `org_id`
- **Retention:** 3 days (delete)
- **Avro subject:** `agent-results-value` (schema `AgentResult`)
- **Purpose:** Agent output (notes, scores, suggestions). Bus is transient transport; read models persist in `core-api`.
- **Idempotency key:** `agentresult:{meeting_id}:{window_id}:{user_id}:{agent}`
- **`agent` / `kind` evolution:** symbols are appended at the tail only. `AgentResultKind` defaults to `note`; `AgentClass` deliberately has no default (a fallback would relabel one agent's output as another's). Consumers decode with the writer schema and ignore `(agent, kind)` pairs they do not handle, and must ship that before any producer emits a new symbol.
- **`kind = meeting_update`** (`agent = MeetingAgent`, ADR-0038 D7): one record per meeting per window, `user_id` = the host. `payload_json` = `{"updates": [{"item_id": uuid, "status": "in_progress" | "covered", "note": str (up to 300 chars)}], "nudge": {"id": uuid, "type": "off_agenda" | "over_time" | "unaddressed", "item_id": uuid | null, "content": str (up to 200 chars)} | null, "surfaced": true}`, at most one update per item. `core-api` applies `updates` to `meeting_agenda_items`; the `nudge` is not persisted and goes to the host only. Examples: `fixtures/examples/agent-results.meeting_update*.json`.
- **Moments and updates** (polish program; optional top-level fields, default null): `moment_id` (uuid5 over meeting_id and the quoted final's chunk_id, shared by every result about the same words), `why` (<= 200 chars), `updates_candidate_id` (a decision/action candidate that updates an existing card; the candidate's id is that same id), `window_start_ms` / `window_end_ms` (meeting time). They mirror the top item of `payload_json`, whose items (candidates, suggestions, a note's top level and commitments) carry their own `moment_id`, `quote`, `speaker_id`, `speaker_label`, `start_ms`, `end_ms`, `why`. Example: `fixtures/examples/agent-results.action_candidate.update.json`.
- **`kind = minutes`** (`agent = MeetingAgent`, polish program "Minutes for everyone"): emitted once the meeting ended (`meeting-events` `ended`), `window_id = "minutes:{generation}"` (one generation per meeting end). One record per participant (`user_id` = that participant, `scope: "personal"`) and one meeting-level record (`user_id` = the nil uuid, `scope: "shared"`). `payload_json` = `{"scope", "prompt_version" ("minutes@v1"), "generation", "status": "ready" | "empty" | "unavailable", "reason": null | "no_transcript" | "budget" | "model_error", "summary": str (up to 600), "decisions": [{"text", "status": "agreed" | "tentative" | "deferred" | "reversed", "decision_id": uuid | null}], "open_items": [{"text", "owner_user_id": uuid | null, "owner_name": str | null, "due": str | null}], "unanswered_questions": [{"text", "asked_by_user_id": uuid | null, "asked_by_name": str | null}], "promises": [{"text", "to_name": str | null, "due": str | null}]}`; texts up to 300 chars, lists up to 20. The shared record never carries promises; `unavailable` is shared-only, with every list empty. Nothing naming a never-record topic is in it. `core-api` persists one `meeting_minutes` row per (meeting, participant) plus the shared row. Examples: `fixtures/examples/agent-results.minutes.*.json`.
- **`kind = room_signals`** (`agent = MeetingAgent`, "After the meeting" layer 1): emitted once the meeting ended, `window_id = "room-signals:{generation}"`, `user_id` = the nil uuid, one record per meeting end. `payload_json` = `{"generation", "status": "ready" | "partial", "prompt_versions": {"last_word", "consensus", "resume_point"} (each `name@vN` or null), "last_word": [{"user_id", "kind": "spoken_over" | "hand_not_called" | "cut_off" | "topic_silent", "topic" (up to 80), "quote" (up to 300) | null, "at_ms" | null, "end_ms" | null, "decision_id" | null, "signal_ids": [uuid]}] (one per person, up to 50), "consensus": [{"decision_id", "strength": "strong" | "mixed" | "weak" | "unclear", "participants", "yes", "hedged", "silent", "objected", "talked_over", "ask": [{"user_id", "reason": "hedged" | "silent" | "talked_over"}] (up to 12)}] (up to 50), "resume_point": null | {"reason": "time_ran_out" | "agenda_unfinished" | "cut_off", "topic" (up to 120), "agenda_item_id" | null, "positions": [{"user_id" | null, "quote", "start_ms", "end_ms"}] (up to 2), "cut_off": null | {"user_id", "quote", "start_ms", "end_ms"}}}`. `partial` means a model step was budget-blocked or failed and only the deterministic rules ran. Quotes are verbatim transcript text and nothing naming a never-record topic is in it. `core-api` files the last-word entries as private nudges, the consensus entries as decision agreement reads plus private agreement checks (it never exposes a per-person stance), and the resume point for the next meeting in the series. Examples: `fixtures/examples/agent-results.room_signals*.json`.

#### `decisions`
- **Producer:** `core-api` (outbox, on confirm)
- **Consumers:** `knowledge-graph-service`, `notification-service`
- **Partition key:** `org_id`
- **Retention:** 30 days (delete)
- **Avro subject:** `decisions-value` (schema `Decision`)
- **Purpose:** Confirmed decisions only. Feeds the `nexus_knowledge` graph via idempotent `MERGE`.
- **Idempotency key:** `decision:{decision_id}:confirmed`

#### `actions`
- **Producer:** `core-api` (outbox, on confirm)
- **Consumers:** `knowledge-graph-service`, `workflow-service`, `integration-service`
- **Partition key:** `org_id`
- **Retention:** 30 days (delete)
- **Avro subject:** `actions-value` (schema `Action`)
- **Purpose:** Confirmed action items only. Feeds follow-up workflows and integrations.
- **Idempotency key:** `action:{action_id}:confirmed`

#### `notifications`
- **Producer:** multiple
- **Consumers:** `notification-service`, `realtime-gateway`
- **Partition key:** `org_id`
- **Retention:** 3 days (delete)
- **Avro subject:** `notifications-value` (schema `Notification`)
- **Purpose:** Cross-channel notification intents.
- **Idempotency key:** `notif:{source_event_id}:{channel}:{user_id}`

#### `agent-requests`
- **Producer:** `agent-runtime-service`
- **Consumers:** `agent-runtime-service` (peer agents)
- **Partition key:** `org_id`
- **Retention:** 1 day (delete)
- **Avro subject:** `agent-requests-value` (schema `AgentRequest`)
- **Purpose:** Agent-to-agent context-fill requests (ContextGapAgent).
- **Idempotency key:** `areq:{correlation_id}`

#### `agent-responses`
- **Producer:** `agent-runtime-service`
- **Consumers:** `agent-runtime-service` (requester)
- **Partition key:** `org_id`
- **Retention:** 1 day (delete)
- **Avro subject:** `agent-responses-value` (schema `AgentResponse`)
- **Purpose:** Agent-to-agent context-fill responses.

#### `agent-stream`
- **Producer:** `agent-runtime-service` (LiteLLM stream bridge)
- **Consumers:** `realtime-gateway` → WebSocket
- **Partition key:** `org_id`
- **Retention:** 1 hour (delete)
- **Avro subject:** `agent-stream-value` (schema `AgentStreamDelta` †)
- **Purpose:** Streamed LLM token deltas to the UI. Throwaway; shed first under backpressure.
- **Kinds:** `kind` (enum `AgentStreamKind`, default `delta`) selects the record. `delta` is the token delta above. `caption_translation` (polish wave 3, live translation) is one live caption final translated into one language a viewer chose: `chunk_id` (the `transcript-chunks` chunk), `language` (ISO 639-1), `status` (`translated` | `budget_denied` | `unavailable`), `delta` = the text, `user_id` = the nil UUID, `window_id` = `caption:{chunk_id}`, idempotency key `caption:{chunk_id}:{language}`. realtime-gateway delivers it only to the meeting's sockets that chose that language; it is never persisted.

> † `DocumentChunk` and `AgentStreamDelta` are net-new schema names for the `document-chunks` chunk envelope and the `agent-stream` token-delta envelope; they are **not** frozen canon. Canon freezes only seven schema names: `TranscriptChunk`, `AgentResult`, `Decision`, `Action`, `Notification`, `AgentRequest`, `AgentResponse`. The `Tenant/Meeting/…Event` names below are likewise net-new.

### Control-plane & observability topics

#### `tenant-events`
- **Producer:** `identity-tenant-service`
- **Consumers:** `billing-metering-service`, `admin-ops-service`, `workflow-service`
- **Partition key:** `org_id`
- **Retention:** retain (compact)
- **Avro subject:** `tenant-events-value` (schema `TenantEvent`)
- **Purpose:** Tenant provisioned / suspended / deleted.

#### `meeting-events`
- **Producer:** `core-api`
- **Consumers:** `meeting-service`, `workflow-service`, `integration-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `meeting-events-value` (schema `MeetingEvent`)
- **Purpose:** Meeting created / updated / ended.

#### `meeting-room-events`
- **Producer:** `meeting-service`
- **Consumers:** `transcription-service`, `core-api`
- **Partition key:** `org_id`
- **Retention:** 3 days (delete)
- **Avro subject:** `meeting-room-events-value` (schema `MeetingRoomEvent`)
- **Purpose:** LiveKit joined / left / recording-ready. Drives transcription start.

#### `integration-events`
- **Producer:** `integration-service`
- **Consumers:** `ingestion-service`, `core-api`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `integration-events-value` (schema `IntegrationEvent`)
- **Purpose:** Calendar→meeting, attachment-to-ingest, external status → action update.

#### `usage-events`
- **Producer:** `agent-runtime-service`, `rag-service` (only)
- **Consumers:** `billing-metering-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `usage-events-value` (schema `UsageEvent`)
- **Purpose:** Metered units (tokens/minutes/seconds/storage). Aggregated nightly into `usage_aggregates`.
- **Idempotency key:** `usage:{org_id}:{event_type}:{occurred_at}:{source_id}`

#### `billing-events`
- **Producer:** `billing-metering-service`
- **Consumers:** `workflow-service`, `notification-service`, `admin-ops-service`
- **Partition key:** `org_id`
- **Retention:** 30 days (delete)
- **Avro subject:** `billing-events-value` (schema `BillingEvent`)
- **Purpose:** Quota-exceeded / dunning / plan-changed.

#### `graph-events`
- **Producer:** `knowledge-graph-service`
- **Consumers:** `admin-ops-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `graph-events-value` (schema `GraphEvent`)
- **Purpose:** Edge created / lineage updated.

#### `embedding-events`
- **Producer:** `embedding-service`
- **Consumers:** (lag / metrics)
- **Partition key:** `org_id`
- **Retention:** 1 day (delete)
- **Avro subject:** `embedding-events-value` (schema `EmbeddingEvent`)
- **Purpose:** Upsert acks for lag tracking.

#### `presence-events`
- **Producer:** `realtime-gateway`
- **Consumers:** `core-api` (analytics)
- **Partition key:** `org_id`
- **Retention:** 1 hour (delete)
- **Avro subject:** `presence-events-value` (schema `PresenceEvent`)
- **Purpose:** Socket.io presence join/leave.

#### `notification-delivery-events`
- **Producer:** `notification-service`
- **Consumers:** `admin-ops-service`, `core-api`
- **Partition key:** `org_id`
- **Retention:** 30 days (delete)
- **Avro subject:** `notification-delivery-events-value` (schema `NotificationDeliveryEvent`)
- **Purpose:** Delivery receipts.

#### `user-sync-events`
- **Producer:** `identity-tenant-service` (Clerk inbox)
- **Consumers:** `identity-tenant-service`
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `user-sync-events-value` (schema `UserSyncEvent`)
- **Purpose:** Clerk webhook user-sync inbox.

#### `admin-events`
- **Producer:** `admin-ops-service`; since polish wave 3 also `transcription-service`, `embedding-service`, `search-service`, `knowledge-graph-service` (`erasure_completed`, one per store)
- **Consumers:** `workflow-service`, `admin-ops-service` (audit-log write, erasure status), and the erasure consumer groups `transcription-service.erasure`, `embedding-service.erasure`, `search-service.erasure`, `knowledge-graph-service.erasure` (`erasure_requested` only)
- **Partition key:** `org_id`
- **Retention:** 365 days, delete (was compact: keyed by org_id, compaction could drop an older `erasure_requested` before every store had read it)
- **Avro subject:** `admin-events-value` (schema `AdminEvent`)
- **Purpose:** Flag changed / tenant suspended / erasure requested / erasure completed (`detail_json` = `{"erasure_id", "store", "service", "counts"}`, idempotency key `erasure_completed:{erasure_id}:{store}`; stores `transcript_chunks`, `vectors`, `search_index`, `knowledge_graph`). Mirrored to `audit_logs`.

#### `agent-feedback`
- **Producer:** `core-api` (transactional outbox, with the `agent_feedback` row)
- **Consumers:** `agent-runtime-service` (refreshes thresholds, guidance examples and person memory caches), `core-api` (incremental `person_memory` consumer)
- **Partition key:** `org_id`
- **Retention:** 7 days (delete)
- **Avro subject:** `agent-feedback-value` (schema `AgentFeedback`)
- **Idempotency key:** `feedback:{feedback_id}`
- **Purpose:** A person confirmed, dismissed, edited, asked or saved something an agent proposed (polish wave 2, agents that learn).

> Producer/consumer notes reconciled to `_canon/01-service-catalog.md`: `usage-events` producers are `agent-runtime-service` and `rag-service` **only** (transcription minutes are metered by `billing-metering-service`, not emitted by `transcription-service`). The `admin-events` audit-log write is `admin-ops-service`; there is no `audit-service` in the catalog.

---

## The delivery & idempotency contract

At-least-once is the only honest guarantee across a network with retries, rebalances, and at-least-once offset commits. It becomes exactly-once *in effect* by pairing it with idempotency on both ends.

### Transactional outbox (produce side)

A producer never does "write DB, then publish" as two steps. The state change and the outbox row commit in **one** Postgres transaction; a separate relay publishes the outbox row to Redpanda. The relay is at-least-once to the bus - a crash after produce but before marking published re-publishes on restart, which the consumer side absorbs. The outbox row carries `org_id`, `topic`, `partition_key` (the `org_id` string), `idempotency_key`, `schema_subject` (`{topic}-value`), and the payload. A unique index `(topic, idempotency_key)` means a double-confirm cannot enqueue a second event.

### Inbox + dedupe cache (consume side)

Every consumer is idempotent against the envelope `idempotency_key`, in two layers:

1. **Redis dedupe cache** - a fast `SET NX EX` reject of obvious replays, keyed:

   ```
   nexus:{org_id}:dedupe:{topic}:{idempotency_key}    TTL 24h
   ```

2. **Durable inbox table** - primary key `(consumer_group, org_id, idempotency_key)`, the authority when the cache has evicted.

The inbox insert and the business effect commit in the **same transaction**, so a crash after the effect cannot double-apply. Each consumer group applies a given fact exactly once, independently of the others.

### Per-topic DLQ

A paired `<topic>.dlq` (14-day retention) exists for every topic. Consumer failures split two ways:

- **Transient** (downstream timeout / 5xx / lock): retry in place with bounded exponential backoff + jitter (5 attempts), offset **not** advanced.
- **Poison** (deserialize failure, schema-incompatible, business-invalid, max-retries): publish to the DLQ with full failure context (`DlqEnvelope`), advance the offset, keep the partition moving.

DLQ replay is an `admin-ops-service` operation that re-produces to the original topic with the **original `idempotency_key`**, so the consumer's dedupe/inbox decides whether it is genuinely new. SLO: DLQ rate < 0.1% of events.

---

## Schema design rules

Every event value opens with the shared `EventEnvelopeHeader` (namespace `io.nexus.events`, frozen field order) then a topic-specific payload. Header fields:

| Field | Type | Role |
|---|---|---|
| `event_id` | `string` (uuid) | Unique per emitted event; distinct from `idempotency_key`. |
| `idempotency_key` | `string` | Business dedupe key; stable across producer retries of the same fact. |
| `org_id` | `string` (uuid) | Tenant root; the Kafka partition key; RLS/cell scoping anchor. |
| `schema_version` | `int` (default 1) | Producer's payload schema version (observability; Registry enforces compat independently). |
| `producer` | `string` | Owning service name (kebab-case, from the Service Catalog). |
| `occurred_at` | `long` (timestamp-micros) | When the fact happened (UTC); not enqueue time. |
| `correlation_id` | `["null","string"]` (default null) | Ties a causal chain across topics. |
| `causation_id` | `["null","string"]` (default null) | `event_id` of the direct cause; null for root events. |

`org_id` is **mandatory on every event** - it is the tenant partition key and the isolation anchor; no event may omit it. `event_id` is distinct from `idempotency_key`: the former is unique per physical emission, the latter is stable across retries of the same logical fact.

---

## Schema evolution governance

Compatibility is **BACKWARD**, enforced in CI against the registered subject.

- **Additive only.** Add a field with a default; remove a field that had a default; widen via a `null` union with a default; append enum symbols at the tail with a default.
- **Forbidden.** Add a required field without a default; remove a non-defaulted field; rename a field; narrow a type. Header field order is frozen by convention.
- **Renames go through the Glossary + an ADR first.** A rename is a remove + add, which BACKWARD forbids. It must be recorded as an explicit `old → new` entry in the Glossary (`24-glossary-and-naming.md`) and superseded via an ADR before any schema change - never silently.
- **CI compatibility gate.** `check-compatibility` runs against the registered subject before any producer image is built. Incompatible PRs fail before merge - at SaaS scale ~20 services upgrade independently and a single non-defaulted required field would break every lagging consumer on a partition.

---

## Agent-to-agent contract

`agent-requests` and `agent-responses` carry the agent-to-agent (A2A) collaboration used by the `ContextGapAgent`. The request/response payloads carry `requester_agent_id`, `responder_target`, `correlation_id`, and `deadline`; correlation is by `correlation_id`, not by partition. Both topics are partitioned by `org_id`, so a request and its response stay within one tenant. **Deny-by-default authorization is enforced by the consuming agents** - org/role-scoped and audited (per ADR-0013); a peer agent answers only requests it is authorized to serve. Past the `deadline`, a request has no value and is not served. `agent-requests` uses idempotency key `areq:{correlation_id}`.

---

## Confirmation-only edges

`decisions` and `actions` are produced **only on user confirmation by `core-api`** - never speculatively by an agent. The contract:

1. The agent surfaces a candidate via `agent-results`.
2. The user confirms in the sidebar.
3. `core-api` writes the row **and** the outbox record in **one transaction** (`UPDATE … SET status = confirmed` + `INSERT outbox`), with idempotency key `decision:{decision_id}:confirmed` / `action:{action_id}:confirmed`.
4. The relay publishes to `decisions` / `actions`.
5. `knowledge-graph-service` consumes and applies idempotent Cypher `MERGE` (never `CREATE`), so the `PRODUCED` / `ASSIGNED_TO` edge is created at most once.

This is the structural enforcement of the "no silent commits" rule, and it is duplicate-proof through four guards: stable idempotency key → outbox unique index → consumer inbox/dedupe → `MERGE`.

---

## Codegen & consumption

The repo generates typed clients from the Avro schemas:

- **Python** - pydantic models / dataclasses for producers and consumers (used by `mytheus-backend` and `mytheus-intelligence`).
- **TypeScript** - types for `mytheus-realtime` and any TS consumer.

`mytheus-backend`, `mytheus-intelligence`, and `mytheus-realtime` depend on the published generated packages rather than hand-rolling serializers - so a schema change here propagates as a versioned package bump there. The sanctioned producer (outbox relay, Avro-serializing against `{topic}-value`) and the base consumer (Avro-deserializing, manual offset commit after effect, dedupe gate, DLQ routing) consume these generated types.

---

## Multi-tenancy

`org_id` is the partition key on **every** topic and the mandatory header field on **every** event. Consequences:

- **Per-tenant ordering and replay** - one tenant's events on one topic land on one partition, consumed in offset order; reprocessing a tenant means replaying its partition slice.
- **Noisy-neighbor isolation** - a bursting tenant saturates only its own partition's lag, observable per tenant, without head-of-line-blocking another tenant.
- **No cross-tenant topic mixing** - there is no shared, tenant-agnostic topic; the bus-level isolation mirrors RLS, per-tenant Qdrant collections, and the org-partitioned `nexus_knowledge` graph.

---

## Testing

- **Schema compatibility tests** - every schema version is checked BACKWARD against the registered subject; the CI gate is the same check.
- **Contract tests** - verify producer/consumer assignments, subject naming (`{topic}-value`), envelope presence, and `org_id` mandatory-ness for every topic.
- **Golden sample events** - checked-in canonical sample events per topic, serialized and round-tripped through the generated clients, so codegen changes are caught against fixtures.

---

## Dev practices

- **PR review for any schema change.** No schema lands without review; a schema change is an API change.
- **Semantic subject versioning.** Schemas are versioned `schemas/<topic>/vN.avsc`; subject versions are registered monotonically.
- **Conventional commits.** Commit messages follow the conventional-commits convention.
- **Trunk-based development.** Short-lived branches, frequent merges to trunk behind the compatibility gate.

---

## CI/CD

GitHub Actions pipeline on every PR:

1. **Validate Avro** - parse and lint every `.avsc`.
2. **Schema Registry compatibility checks** - run `check-compatibility` against the registered subject under **BACKWARD**; block breaking changes before merge.
3. **Generate & publish client packages** - build Python (pydantic/dataclasses) and TypeScript packages; publish on merge to trunk.
4. **Block breaking changes** - an incompatible subject change fails the build and cannot merge.

---

## Reference

- `_canon/03-event-taxonomy.md` - frozen event-naming authority (topics, schemas, partition keys, retention, delivery).
- `07-event-architecture.md` - the full delivery machinery (outbox/inbox, dedupe, DLQ, backpressure).
- `03-container-architecture.md` §4.2 - the canonical + control-plane topic set on the async backbone.
- `_canon/01-service-catalog.md` - producer/consumer assignments (single source of truth for service names).
- `24-glossary-and-naming.md` - the published naming authority and any recorded `old → new` rename.
