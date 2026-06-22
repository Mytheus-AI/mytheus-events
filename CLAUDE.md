# CLAUDE.md — `reprise-events`

Working guide for AI agents in the **event contracts & schema registry** repo for Reprise AI (internal codename NEXUS). This repo is the single home for the Avro schemas, Schema Registry subjects, topic taxonomy, partition-key conventions, and the outbox/inbox/DLQ delivery contract that every service in `reprise-backend`, `reprise-intelligence`, and `reprise-realtime` produces and consumes against. It is the source of generated Python and TypeScript client types and the engineering realization of the frozen `_canon/03-event-taxonomy.md`.

## Layout

```
schemas/     # Avro schemas, versioned: schemas/<topic>/vN.avsc (shared EventEnvelopeHeader + payload)
subjects/    # Schema Registry subject definitions ({topic}-value) + compatibility config
codegen/     # generators for Python (pydantic/dataclasses) + TypeScript clients
contracts/   # outbox / inbox / DLQ delivery-contract specs + the shared envelope
docs/        # taxonomy, conventions, evolution guides
```

## Cross-cutting rules (always)

- **EVERY event carries `org_id` + `idempotency_key`** in the `EventEnvelopeHeader`. `org_id` is the tenant partition key and isolation anchor; `idempotency_key` is the dedupe key, stable across producer retries of the same fact. Never emit an event without both.
- **BACKWARD compatibility is mandatory — never break a subject.** A new consumer schema must read data written by the previous producer schema.
- **One subject per topic, named `{topic}-value`.** No key schemas — the partition key is the raw `org_id` UUID string.
- **Renames go through the Glossary + ADR first, never silently.** A rename is a remove + add, which BACKWARD forbids. Record it as an explicit `old → new` entry in `24-glossary-and-naming.md` and supersede via an ADR before touching a schema.
- **At-least-once + outbox/inbox/DLQ is the contract.** Consumers must honor it: dedupe via the durable inbox plus the Redis cache `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` (24h TTL); poison messages go to `<topic>.dlq`. Delivery is at-least-once everywhere; idempotency makes it exactly-once in effect.

## Naming authority

This repo **implements** `_canon/03-event-taxonomy.md`. Topic names, partition keys, and Avro field names are **frozen there**. NEVER invent a topic or rename one. Keep every identifier verbatim, including:

- Data-plane topics: `transcript-chunks`, `document-chunks`, `agent-results`, `decisions`, `actions`, `notifications`, `agent-requests`, `agent-responses`, `agent-stream`.
- Control-plane topics: `tenant-events`, `meeting-events`, `meeting-room-events`, `integration-events`, `usage-events`, `billing-events`, `graph-events`, `embedding-events`, `presence-events`, `notification-delivery-events`, `user-sync-events`, `admin-events`.
- The Redis dedupe pattern `nexus:{org_id}:dedupe:{topic}:{idempotency_key}` and the A2A field names `requester_agent_id`, `responder_target`, `correlation_id`, `deadline`.
- Frozen Avro schema names: `TranscriptChunk`, `AgentResult`, `Decision`, `Action`, `Notification`, `AgentRequest`, `AgentResponse`.

To add a topic or change a partition key, land it in the canon first; this repo only realizes the canon.

## Do / Don't

**Do**

- Add a field with a default (or a `["null", T]` union defaulting to `null`); bump `schema_version`.
- Keep the `EventEnvelopeHeader` field order frozen.
- Keep producer/consumer assignments matching `_canon/01-service-catalog.md` (e.g. `usage-events` is produced only by `agent-runtime-service` and `rag-service`; `admin-events` audit-log write is `admin-ops-service`).
- Treat `decisions`/`actions` as confirmation-only edges produced by `core-api`.

**Don't**

- **Never add a required field without a default** — it breaks BACKWARD and every lagging consumer.
- Never remove a non-defaulted field, narrow a type, or rename a field silently.
- Never invent a topic, a second subject for a topic, or a non-`org_id` partition key.
- Never bypass the outbox on produce or the inbox/dedupe on consume.

## Validate / test / generate

```bash
make validate            # parse + lint every Avro schema
make check-compatibility # BACKWARD check against the registered {topic}-value subject
make register            # register the new subject version
make codegen             # generate Python (pydantic/dataclasses) + TypeScript clients
make test                # schema-compatibility tests, contract tests, golden sample events
```

Run `make validate` and `make check-compatibility` before opening a PR. The CI gate runs the same compatibility check and blocks breaking changes.

## PR conventions for schema changes

- A schema change is an API change — open a PR with review.
- Conventional commits; trunk-based; short-lived branches.
- Semantic subject versioning: `schemas/<topic>/vN.avsc`, registered monotonically.
- Include the BACKWARD-compatibility check result and updated golden samples.
- **Commits in this project must NOT include a `Co-Authored-By` footer.**
