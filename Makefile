.PHONY: install lint validate check-compatibility register codegen codegen-py codegen-ts \
        test test-py test-ts typecheck create-topics ci ci-compat verify-all
SR_URL ?= http://localhost:8081

install:                 ## install python (uv) + node deps
	uv sync
	npm install

lint:                    ## ruff lint the codegen + tests
	uv run ruff check codegen tests

validate:                ## parse + lint every .avsc, enforce structural invariants
	uv run python codegen/validate.py schemas

register:                ## register {topic}-value subjects under BACKWARD (needs SR_URL up)
	uv run python codegen/register.py --registry $(SR_URL) --subjects subjects

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
	node --test "tests/ts/*.test.mjs"

typecheck:               ## tsc type-check the generated TypeScript client
	npx tsc -p clients/typescript/tsconfig.json --noEmit

create-topics:           ## create the 21 topics + 21 .dlq locally (needs rpk + a broker)
	bash scripts/create_topics.sh

# Infra-free everyday gate (no Schema Registry required).
ci: validate codegen test

# Registry gate: run where a Schema Registry is up (CI avro-compat job / local compose).
ci-compat: register check-compatibility

# Full local demo sequence (requires SR_URL up).
verify-all: validate register check-compatibility codegen test
