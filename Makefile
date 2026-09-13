# Every routine operation in one place. `make help` lists them.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MVN ?= ./mvnw
ifeq (,$(wildcard ./mvnw))
MVN := mvn
endif

COMPOSE ?= docker compose
SERVER_URL ?= http://localhost:8080
BENCH_OUT ?= benchmarks/out
AB_EVENTS ?= 12000
AB_AXIS ?= writer

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- Start here ------------------------------------------------------------------------------

.PHONY: preflight
preflight: ## Prove the toolchain, Docker transport and ports are usable (~2s)
	@bash scripts/preflight.sh

# --- Build -----------------------------------------------------------------------------------

.PHONY: build
build: ## Compile and package every module
	$(MVN) -B clean package -DskipTests

.PHONY: test
test: ## Run the unit suite (no Docker required)
	$(MVN) -B test

.PHONY: it
it: ## Run the container-backed integration suite (requires Docker)
	$(MVN) -B -Pintegration verify -DfailIfNoTests=false

# Reuse is opt-in per invocation and never on by default, so `make it`, `make verify`, `make
# ci-local` and CI all keep starting from cold containers. Testcontainers' own opt-in is
# ~/.testcontainers.properties, a file outside the repository, which makes "does this machine
# reuse?" invisible to everyone reading the build. TESTCONTAINERS_REUSE_ENABLE is the same switch
# read from the environment, so the decision lives here where it can be reviewed.
#
# The containers survive the JVM: Testcontainers excludes reusable containers from Ryuk's reaping,
# by design. `make it-reuse-stop` is how you get back to cold, and you must run it after changing
# anything the containers' state depends on.
.PHONY: it-reuse
it-reuse: export TESTCONTAINERS_REUSE_ENABLE=true
it-reuse: ## Integration suite against reusable containers (fast re-runs; leaves them running)
	$(MVN) -B -Pintegration verify -DfailIfNoTests=false

.PHONY: it-reuse-stop
it-reuse-stop: ## Remove the containers `make it-reuse` left running
	@ids=$$(docker ps -aq --filter 'label=org.testcontainers.hash'); \
	if [ -n "$$ids" ]; then docker rm -f $$ids; else echo "no reusable containers"; fi

.PHONY: verify
verify: ## Unit tests + coverage gate + integration tests
	$(MVN) -B -Pintegration,coverage-gate verify

.PHONY: format
format: ## Apply google-java-format to every module
	$(MVN) -B spotless:apply

.PHONY: format-check
format-check: ## Fail if any file is unformatted
	$(MVN) -B spotless:check

.PHONY: lint
lint: format-check ## Alias for format-check

# --- Local stack -----------------------------------------------------------------------------

.PHONY: up
up: ## Start the whole system locally (broker, cache, DynamoDB, both services, Prometheus, Grafana)
	$(COMPOSE) up -d --build
	@$(MAKE) --no-print-directory wait
	@echo ""
	@echo "  API           $(SERVER_URL)"
	@echo "  OpenAPI       $(SERVER_URL)/swagger-ui.html"
	@echo "  Health        $(SERVER_URL)/actuator/health"
	@echo "  Metrics       $(SERVER_URL)/actuator/prometheus"
	@echo "  RabbitMQ      http://localhost:15672  (guest/guest)"
	@echo "  Prometheus    http://localhost:9090"
	@echo "  Grafana       http://localhost:3000   (anonymous viewer)"
	@echo ""

.PHONY: wait
wait: ## Block until the server reports ready
	@printf "Waiting for the server to become ready"
	@for i in $$(seq 1 90); do \
		if curl -fsS $(SERVER_URL)/actuator/health/readiness >/dev/null 2>&1; then \
			echo " ready."; exit 0; \
		fi; \
		printf "."; sleep 2; \
	done; \
	echo " timed out."; \
	echo "Recent logs:"; $(COMPOSE) logs --tail=40 server; \
	exit 1

.PHONY: down
down: ## Stop the stack and delete its volumes
	$(COMPOSE) down -v --remove-orphans

.PHONY: logs
logs: ## Follow logs from both services
	$(COMPOSE) logs -f server consumer

.PHONY: ps
ps: ## Show container status
	$(COMPOSE) ps

.PHONY: schema
schema: ## Re-apply the DynamoDB schema (idempotent)
	$(COMPOSE) run --rm schema

.PHONY: smoke
smoke: ## Post one event and read it back through every query endpoint
	@bash scripts/smoke.sh

# Deliberately not folded into `smoke`: one run costs the longest TTL it measures (5 minutes with
# --include-totals), which is too slow for a check that gates `make ci-local`.
.PHONY: queryability
queryability: ## Measure accept-to-queryable, SLO objective 5 plus the read cache's TTL term
	@bash scripts/queryability.sh $(QUERYABILITY_ARGS)

# --- Benchmarks ------------------------------------------------------------------------------

.PHONY: bench
bench: ## Run the default benchmark scenario against the local stack
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	java -jar Client/SkierClient/target/skier-client.jar run \
		--scenario benchmarks/scenarios/closed-loop-200k.yaml \
		--base-url $(SERVER_URL) \
		--out $(BENCH_OUT)

.PHONY: bench-open
bench-open: ## Run the open-loop (fixed arrival rate) scenario
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	java -jar Client/SkierClient/target/skier-client.jar run \
		--scenario benchmarks/scenarios/open-loop-sweep.yaml \
		--base-url $(SERVER_URL) \
		--out $(BENCH_OUT)

.PHONY: ab
ab: ## A/B the write strategy (single vs batched BatchWriteItem)
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	bash scripts/ab.sh writer $(AB_EVENTS)

.PHONY: ab-cardinality
ab-cardinality: ## A/B the cardinality strategy (exact sentinel vs Redis HyperLogLog)
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	bash scripts/ab.sh cardinality $(AB_EVENTS)

# The check that the per-arm state reset in scripts/ab.sh actually removed the ordering bias: swap
# the arms and confirm the result does not move. A fix verified in one arm order has not been
# verified. `make ab-reverse AB_AXIS=cardinality` does the other axis.
.PHONY: ab-reverse
ab-reverse: ## Re-run an A/B ($(AB_AXIS)) with the arms in the opposite order
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	AB_REVERSE=1 bash scripts/ab.sh $(AB_AXIS) $(AB_EVENTS)

# Not folded into `make it`: it needs a running Compose stack, kills a container, and takes a couple
# of minutes. It is the check that the delivery claims survive a process death, which no test in the
# suite covers, a Testcontainers consumer cannot be SIGKILLed out from under its own JVM.
.PHONY: recovery
recovery: ## Kill the consumer mid-drain and reconcile every published event against DynamoDB
	@bash scripts/recovery.sh

.PHONY: bench-smoke
bench-smoke: ## A 2,000-request benchmark, for checking the harness works
	$(MVN) -B -q -pl Client/SkierClient -am package -DskipTests
	java -jar Client/SkierClient/target/skier-client.jar run \
		--scenario benchmarks/scenarios/smoke.yaml \
		--base-url $(SERVER_URL) \
		--out $(BENCH_OUT)

# --- Housekeeping ----------------------------------------------------------------------------

.PHONY: clean
clean: ## Remove build output
	$(MVN) -B clean
	rm -rf $(BENCH_OUT)
