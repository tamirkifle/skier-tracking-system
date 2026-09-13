#!/usr/bin/env bash
# Checks that this machine can do the work before the work starts: repository, toolchain, Docker as
# Testcontainers sees it, the ports the local stack needs, and optional tooling.
#
# FAIL means stop and fix. SKIP means a capability is absent, so anything needing it cannot be
# verified here. Exit 0 means every required check passed.

set -uo pipefail

PASS=0
FAIL=0
SKIP=0

pass() { printf '  \033[32mPASS\033[0m  %-34s %s\n' "$1" "${2-}"; PASS=$((PASS + 1)); }
fail() { printf '  \033[31mFAIL\033[0m  %-34s %s\n' "$1" "${2-}"; FAIL=$((FAIL + 1)); }
skip() { printf '  \033[33mSKIP\033[0m  %-34s %s\n' "$1" "${2-}"; SKIP=$((SKIP + 1)); }

cd "$(dirname "${BASH_SOURCE[0]}")/.." || exit 1
ROOT=$(pwd)

echo ""
echo "Preflight: $ROOT"
echo ""

if [[ -f pom.xml ]] && grep -q '<module>Server/SkierServer</module>' pom.xml; then
  pass "repository root" "$(basename "$ROOT")"
else
  fail "repository root" "run this from the repository root; the reactor POM is not here"
fi

BRANCH=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
if [[ -n "$BRANCH" ]]; then
  DIRTY=$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')
  if [[ "$DIRTY" == "0" ]]; then
    pass "working tree" "clean, on $BRANCH"
  else
    fail "working tree" "$DIRTY uncommitted path(s) on $BRANCH"
  fi

  AHEAD=$(git rev-list --count origin/main..HEAD 2>/dev/null || echo '?')
  pass "ahead of origin/main" "$AHEAD commit(s)"
else
  fail "git" "not a git repository"
fi

JAVA_VER=$(java -version 2>&1 | head -1)
if [[ "$JAVA_VER" == *'"17'* ]]; then
  pass "java 17" "$(printf '%s' "$JAVA_VER" | cut -c1-48)"
elif [[ -n "$JAVA_VER" ]]; then
  fail "java 17" "found: $JAVA_VER"
else
  fail "java 17" "no java on PATH"
fi

MVN_VER=$(mvn -v 2>/dev/null | head -1)
if [[ -n "$MVN_VER" ]]; then
  pass "maven" "$(printf '%s' "$MVN_VER" | cut -c1-48)"
else
  fail "maven" "no mvn on PATH (and no ./mvnw in the tree)"
fi

DOCKER_SERVER=$(docker info --format '{{.ServerVersion}}' 2>/dev/null)
if [[ -n "$DOCKER_SERVER" ]]; then
  pass "docker daemon" "Engine $DOCKER_SERVER"
else
  fail "docker daemon" "\`docker info\` failed; container-backed work cannot run"
fi

# The discriminating check. `docker ps` can succeed while this returns HTTP 400, and Testcontainers
# reports that as an absent daemon, which sends you to the daemon rather than to the transport.
SOCK=""
if [[ "${DOCKER_HOST-}" == unix://* ]]; then
  SOCK="${DOCKER_HOST#unix://}"
else
  for candidate in "$HOME/.docker/run/docker.sock" /var/run/docker.sock; do
    [[ -S "$candidate" ]] && SOCK="$candidate" && break
  done
fi
if [[ -n "$SOCK" && -S "$SOCK" ]]; then
  CODE=$(curl -s -o /dev/null -w '%{http_code}' --unix-socket "$SOCK" http://localhost/info 2>/dev/null)
  if [[ "$CODE" == "200" ]]; then
    pass "docker /info probe" "200 on $(basename "$SOCK")"
  else
    fail "docker /info probe" "HTTP ${CODE:-none}, Testcontainers will report an absent daemon"
  fi
else
  fail "docker socket" "no socket found (checked \$DOCKER_HOST, ~/.docker/run, /var/run)"
fi

if grep -q 'api\.version' pom.xml; then
  pass "api.version pin" "present in the integration profile"
else
  fail "api.version pin" "gone from pom.xml, the docker-java negotiation bug returns"
fi

BUSY=""
for p in 8080 5672 15672 6379 4566 9090 3000; do
  if lsof -nP -iTCP:"$p" -sTCP:LISTEN >/dev/null 2>&1; then BUSY="$BUSY $p"; fi
done
if [[ -z "$BUSY" ]]; then
  pass "compose ports free" "8080 5672 15672 6379 4566 9090 3000"
else
  fail "compose ports free" "in use:$BUSY, run \`make down\` or stop the owner"
fi

if command -v gitleaks >/dev/null 2>&1; then
  pass "gitleaks" "$(gitleaks version 2>&1 | head -1)"
else
  skip "gitleaks" "not installed; \`make secrets-scan\` and the CI parity of that job cannot run"
fi

GH_ACCT=$(gh auth status 2>&1 | sed -n 's/.*account \([^ ]*\).*/\1/p' | head -1)
if [[ -n "$GH_ACCT" ]]; then
  pass "gh authenticated" "$GH_ACCT"
else
  skip "gh authenticated" "no gh auth; CI runs cannot be triggered or polled"
fi

echo ""
printf '  %d passed, %d failed, %d skipped\n' "$PASS" "$FAIL" "$SKIP"
echo ""

if [[ "$FAIL" -gt 0 ]]; then
  echo "  Not ready. Fix the failures above before starting work."
  echo ""
  exit 1
fi

echo "  Ready."
echo ""
