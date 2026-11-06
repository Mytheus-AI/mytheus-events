.PHONY: install lint validate check-compatibility register check-registered build-python-client codegen codegen-py codegen-ts \
        test test-py test-ts typecheck create-topics ci ci-compat verify-all
SR_URL ?= http://localhost:8085

install:                 ## install python (uv) + node deps
	uv sync
	npm install

lint:                    ## ruff lint the codegen + tests
	uv run ruff check codegen tests

validate:                ## parse + lint every .avsc, enforce structural invariants
	uv run python codegen/validate.py schemas

register:                ## register {topic}-value subjects under BACKWARD (needs SR_URL up)
	uv run python codegen/register.py --registry $(SR_URL) --subjects subjects

check-registered:        ## read-only: fail if ANY local schema differs from registry latest (needs SR_URL up)
	uv run python codegen/register.py --check --registry $(SR_URL)

check-compatibility:     ## BACKWARD gate against registered subjects (needs SR_URL up)
	uv run python codegen/check_compat.py --registry $(SR_URL) --subjects subjects

codegen: codegen-py codegen-ts   ## regenerate subjects, fixtures, python + typescript clients

codegen-py:
	uv run python codegen/gen_subjects.py
	uv run python codegen/gen_fixtures.py
	uv run python codegen/py_from_avsc.py schemas clients/python/mytheus_events

codegen-ts:
	node codegen/ts_from_avsc.mjs

test: test-py test-ts    ## schema-compat + contract + golden round-trip (py + ts)

test-py:
	uv run pytest tests -q

test-ts:
	node --test tests/ts/*.test.mjs

typecheck:               ## tsc type-check the generated TypeScript client
	npx tsc -p clients/typescript/tsconfig.json --noEmit

# Wheel for mytheus-intelligence & co: dist/mytheus_events-<ver>-py3-none-any.whl (see README
# "Python client wheel"). Needs hatchling (fetched by uv's build isolation).
build-python-client:     ## build the mytheus_events pydantic client wheel into dist/
	uv build --wheel --out-dir dist

create-topics:           ## create the 21 topics + 21 .dlq = 42 (schemas/ also has _envelope, not a topic; idempotent; needs rpk + a broker)
	bash scripts/create_topics.sh

# One command for "make this cluster ready to carry events", because the two halves are
# useless apart: topics without subjects means every produce fails serialization, and subjects
# without topics means auto-created 1-partition topics with default retention. Both halves are
# idempotent, so this is safe to re-run and is what a deploy path should call.
# mytheus-platform/deploy/deploy.sh does not call it (that would need this repo on the deploy
# box); it VERIFIES the result and fails the deploy if the backbone is missing.
provision: create-topics register   ## topics + subjects, idempotent; BROKERS= and SR_URL=

# Infra-free everyday gate (no Schema Registry required).
ci: validate codegen test

# Registry gate: run where a Schema Registry is up (CI avro-compat job / local compose).
ci-compat: register check-compatibility

# Full local demo sequence (requires SR_URL up).
verify-all: validate register check-compatibility codegen test
