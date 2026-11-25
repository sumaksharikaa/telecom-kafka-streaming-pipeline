PY ?= python

.PHONY: install lint fmt test e2e e2e-kafka up produce batch down clean

install:    ## Package + dev tools (needs Java 17 for Spark)
	$(PY) -m pip install -e ".[dev]"

lint:
	ruff check . && ruff format --check .

fmt:
	ruff check --fix . && ruff format .

test:       ## Unit tests, incl. real streaming queries on a file source
	$(PY) -m pytest -q

e2e:        ## Full two-phase scenario in file mode (no broker), checked against ground truth
	$(PY) scripts/run_e2e.py

e2e-kafka:  ## Same scenario against Kafka on localhost:9092 (make up first)
	TELECOM_SOURCE_MODE=kafka $(PY) scripts/run_e2e.py

up:         ## Kafka + the continuous streaming job
	docker compose up -d --build kafka stream

produce:    ## Send one phase of traffic into Kafka: make produce PHASE=1
	docker compose run --rm pipeline python -m telecom_stream.simulator --phase $(or $(PHASE),1)

batch:      ## SCD2 dim, late backfill, daily usage, Hive tables, reconciliation
	docker compose run --rm pipeline python -m telecom_stream.batch all

down:
	docker compose down

clean:
	rm -rf data spark-warehouse metastore_db derby.log .pytest_cache
