# Every routine operation in one place. `make help` lists them.

SHELL := /bin/bash
.DEFAULT_GOAL := help

MVN ?= ./mvnw
ifeq (,$(wildcard ./mvnw))
MVN := mvn
endif

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
