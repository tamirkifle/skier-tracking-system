#!/usr/bin/env bash
# Runs every gate .github/workflows/ci.yml runs, locally, in the same order and with the same
# commands. A contributor's feedback loop is local, so if CI can fail on something that cannot be
# run here, the push becomes the test.
#
# Usage:
#   bash scripts/ci-local.sh                 # every job
#   bash scripts/ci-local.sh build coverage  # named jobs only
#   Jobs: secrets build integration coverage dependencies images smoke rules
#
# Known parity deviations, read these before trusting a green run:
#   1. Architecture. CI is ubuntu-latest amd64 and this machine is arm64 macOS, so a failure that
#      depends on the platform cannot be caught here. No local workaround.
#   2. A GitHub Actions marketplace pin is never resolved locally, so a bad `uses:` reference is
#      invisible to every local run and surfaces only on push. The other unfixable one.
#   3. The secrets job SKIPs when gitleaks is not installed, and that gate is then untested.
#   4. Trivy is advisory in CI (exit-code 0) and SKIPs here when absent, so the gate is unaffected.
#   5. Images build sequentially with plain `docker build`; CI uses a buildx matrix with a cache.

set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1

MAVEN_ARGS=${MAVEN_ARGS:--B -ntp}
export MAVEN_ARGS

ALL_JOBS=(secrets build integration coverage dependencies images smoke rules)
if [[ $# -gt 0 ]]; then JOBS=("$@"); else JOBS=("${ALL_JOBS[@]}"); fi

RESULTS=()
FAILED=0
STACK_UP=0

teardown() {
  if [[ "$STACK_UP" == "1" ]]; then
    echo "--- tearing down the stack"
    docker compose down -v --remove-orphans >/dev/null 2>&1
    STACK_UP=0
  fi
}
trap teardown EXIT

banner() { printf '\n\033[1m=== %s\033[0m\n' "$1"; }

run_step() {
  local job=$1 label=$2
  shift 2
  echo "--- $label"
  if "$@"; then
    return 0
  fi
  echo "!!! FAILED: $job / $label"
  return 1
}

record() {
  local job=$1 status=$2 secs=$3 note=${4-}
  RESULTS+=("$(printf '%-13s %-6s %5ds  %s' "$job" "$status" "$secs" "$note")")
  [[ "$status" == "FAIL" ]] && FAILED=$((FAILED + 1))
  return 0
}

job_secrets() {
  if ! command -v gitleaks >/dev/null 2>&1; then
    JOB_SKIPPED=1
    JOB_NOTE="gitleaks not installed"
    return 0
  fi
  run_step secrets "gitleaks detect (full history)" \
    gitleaks detect --no-banner --redact
}

job_build() {
  run_step build "spotless:check" mvn $MAVEN_ARGS spotless:check || return 1
  run_step build "unit tests" mvn $MAVEN_ARGS test || return 1
}

job_integration() {
  run_step integration "pull localstack" docker pull -q localstack/localstack:3.8 || return 1
  run_step integration "pull rabbitmq" docker pull -q rabbitmq:3.13-management-alpine || return 1
  run_step integration "integration tests" \
    mvn $MAVEN_ARGS -Pintegration verify -DskipITs=false || return 1
}

job_coverage() {
  run_step coverage "coverage floor" mvn $MAVEN_ARGS -Pcoverage-gate verify
}

job_dependencies() {
  run_step dependencies "dependency:tree" \
    mvn $MAVEN_ARGS dependency:tree -DoutputFile=target/deps.txt || return 1
  run_step dependencies "aws sdk v1 absent" bash scripts/check-no-sdk-v1.sh || return 1
  if command -v trivy >/dev/null 2>&1; then
    trivy fs --scanners vuln --severity CRITICAL,HIGH --exit-code 0 --format table . || true
  else
    echo "--- trivy not installed; filesystem scan skipped (advisory in CI as well)"
  fi
}

job_images() {
  local rc=0
  local pairs=(
    "server:Server/SkierServer/Dockerfile"
    "consumer:Consumer/SkierConsumer/Dockerfile"
    "schema:Infra/SchemaTool/Dockerfile"
  )
  for pair in "${pairs[@]}"; do
    local name=${pair%%:*} df=${pair#*:}
    run_step images "build $name image" \
      docker build -q -f "$df" -t "skiers-$name:ci" . || rc=1
  done
  return $rc
}

job_smoke() {
  run_step smoke "compose up" docker compose up -d --build || return 1
  STACK_UP=1
  run_step smoke "wait for readiness" make --no-print-directory wait || return 1
  run_step smoke "end-to-end smoke" make --no-print-directory smoke || return 1
  run_step smoke "benchmark harness check" \
    mvn $MAVEN_ARGS -q -pl Client/SkierClient -am package -DskipTests || return 1
  run_step smoke "2,000-request load" \
    java -jar Client/SkierClient/target/skier-client.jar run \
      --scenario benchmarks/scenarios/smoke.yaml \
      --base-url http://localhost:8080 \
      --out benchmarks/out \
      --fail-under-success-rate 0.99 || return 1
  teardown
}

job_rules() {
  run_step rules "promtool test rules" \
    docker run --rm -v "$PWD:/w" --entrypoint promtool prom/prometheus:v2.54.1 \
      test rules /w/Infra/observability/alerts.test.yml
}

for job in "${JOBS[@]}"; do
  banner "$job"
  start=$SECONDS
  JOB_SKIPPED=0
  JOB_NOTE=""
  if declare -F "job_$job" >/dev/null; then
    if "job_$job"; then
      if [[ "$JOB_SKIPPED" == "1" ]]; then
        record "$job" SKIP $((SECONDS - start)) "$JOB_NOTE"
      else
        record "$job" PASS $((SECONDS - start)) "$JOB_NOTE"
      fi
    else
      record "$job" FAIL $((SECONDS - start)) "$JOB_NOTE"
      if [[ "$job" == "build" ]]; then
        echo ""
        echo "build failed; the remaining jobs depend on it (as they do in CI). Stopping."
        break
      fi
    fi
  else
    record "$job" ERROR 0 "unknown job"
  fi
done

teardown

banner "summary"
for line in "${RESULTS[@]}"; do echo "  $line"; done
echo ""

if [[ "$FAILED" -gt 0 ]]; then
  echo "  $FAILED job(s) failed. This is what CI would report."
  echo ""
  exit 1
fi

if [[ ${#JOBS[@]} -eq ${#ALL_JOBS[@]} ]]; then
  echo "  Every gate CI runs passes here. Read the parity deviations at the top of this file before"
  echo "  reading that as a guarantee, the amd64/arm64 difference is real and unfixable locally."
else
  echo "  ${#JOBS[@]} of ${#ALL_JOBS[@]} jobs passed. This is NOT a green CI run: ${ALL_JOBS[*]} is the"
  echo "  full set. Run \`make ci-local\` before claiming a change is done."
fi
echo ""
