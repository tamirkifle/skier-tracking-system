#!/usr/bin/env bash
# Recounts SkierCounts.uniqueSkierCount from SkierTracking sentinels against a running stack.

set -euo pipefail

COMPOSE="${COMPOSE:-docker compose}"

info() { printf '\033[36m%s\033[0m\n' "$*"; }
warn() { printf '\033[33m%s\033[0m\n' "$*"; }

if [[ " $* " == *" --apply "* ]]; then
  warn "Applying repairs. Stop the consumer first, 'docker compose stop consumer', or the recount"
  warn "will omit whatever is claimed while the scan runs."
else
  info "Dry run. Pass --apply --confirm-ingest-paused to write."
fi

exec $COMPOSE run --rm --no-deps \
  --entrypoint java \
  schema -cp app.jar skiers.infra.CardinalityRepair "$@"
