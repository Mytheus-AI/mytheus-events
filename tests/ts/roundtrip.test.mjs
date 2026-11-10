// Golden round-trip for the TypeScript/avsc side: every topic value encodes + decodes and
// preserves org_id through the generated-client serialization path.
import { test } from "node:test";
import assert from "node:assert";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import avro from "avsc";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const SCHEMAS = join(ROOT, "schemas");
const FIXTURES = join(ROOT, "fixtures");
const readJson = (p) => JSON.parse(readFileSync(p, "utf8"));

const TOPICS = readdirSync(SCHEMAS)
  .filter((d) => !d.startsWith("_") && statSync(join(SCHEMAS, d)).isDirectory())
  .sort();

function typeFor(topic) {
  const registry = {};
  // Register the envelope first so the topic's `header` reference resolves.
  avro.Type.forSchema(readJson(join(SCHEMAS, "_envelope", "EventEnvelopeHeader.avsc")), {
    registry,
    wrapUnions: false,
  });
  return avro.Type.forSchema(readJson(join(SCHEMAS, topic, "v1.avsc")), {
    registry,
    wrapUnions: false,
  });
}

test("21 topics round-trip through avsc and preserve org_id", () => {
  assert.strictEqual(TOPICS.length, 21);
  for (const topic of TOPICS) {
    const type = typeFor(topic);
    const rec = readJson(join(FIXTURES, `${topic}.json`));
    const back = type.fromBuffer(type.toBuffer(rec));
    assert.strictEqual(back.header.org_id, rec.header.org_id, topic);
    assert.ok("idempotency_key" in back.header, topic);
  }
});

test("agent-results per-kind examples round-trip through avsc (MeetingAgent meeting_update)", () => {
  const dir = join(FIXTURES, "examples");
  const files = readdirSync(dir).filter((f) => f.startsWith("agent-results.") && f.endsWith(".json"));
  assert.ok(files.length >= 2, "expected the meeting_update examples");
  const type = typeFor("agent-results");
  for (const f of files) {
    const rec = readJson(join(dir, f));
    const back = type.fromBuffer(type.toBuffer(rec));
    assert.strictEqual(back.agent, "MeetingAgent", f);
    assert.strictEqual(back.kind, "meeting_update", f);
    const payload = JSON.parse(back.payload_json);
    assert.deepStrictEqual(Object.keys(payload).sort(), ["nudge", "surfaced", "updates"], f);
    assert.strictEqual(payload.surfaced, true, f);
  }
});
