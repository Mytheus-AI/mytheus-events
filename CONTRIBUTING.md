# Contributing to mytheus-events

## Naming comes from canon — never invent
Topic names, Avro subjects, and schema fields are frozen in `mytheus-docs/_canon/` (chiefly
`03-event-taxonomy.md`) and published in `24-glossary-and-naming.md`. Use them verbatim. A rename
or a new topic goes **Glossary + ADR first** in `mytheus-docs`, then propagates here — never the
reverse.

**Canon location (recorded decision):** canon authority lives in `mytheus-docs`. For MVP the
contract-binding repos consume it as a **sibling checkout** — clone all Mytheus repos under one
parent directory. Revisit git-submodule / vendoring post-W36.

## Contracts are frozen; changes must stay BACKWARD-compatible
Every subject is `{topic}-value` under **BACKWARD** compatibility. Never remove or retype a field
in place; add optional fields with defaults. Every payload carries the `EventEnvelopeHeader` (with
`org_id`) and topics are partitioned by `org_id` — tenant isolation is a bus-level invariant.

## Every PR includes an `org_id` authz review
Confirm new/changed schemas keep `org_id` non-nullable in the header and that the topic's
producer/consumer set in `03-event-taxonomy.md` is unchanged (or the canon edit is in the same
change set). Shift-left security from day one.

## Standards
- **Conventional Commits**; **no `Co-Authored-By` footer** (per this repo's CLAUDE.md).
- **Trunk-based**: short-lived branches, frequent PRs, mandatory review.
- CI green before merge: `make ci` (validate + codegen + py/ts round-trip tests) and, against a
  live Schema Registry, `make ci-compat` (register + BACKWARD gate). Install hooks once:
  `uv run pre-commit install`.
