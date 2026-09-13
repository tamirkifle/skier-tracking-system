# Every routine operation in one place. `make help` lists them.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MVN ?= ./mvnw
ifeq (,$(wildcard ./mvnw))
MVN := mvn
endif

SERVER_URL ?= http://localhost:8080
BENCH_OUT ?= benchmarks/out

.PHONY: help
help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

# --- Build -----------------------------------------------------------------------------------

.PHONY: build
build: ## Compile and package every module
	$(MVN) -B clean package -DskipTests

.PHONY: test
test: ## Run the unit suite (no Docker required)
	$(MVN) -B test

.PHONY: format
format: ## Apply google-java-format to every module
	$(MVN) -B spotless:apply

.PHONY: format-check
format-check: ## Fail if any file is unformatted
	$(MVN) -B spotless:check

.PHONY: lint
lint: format-check ## Alias for format-check

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
