#!/usr/bin/env bash
# Greps tracked source, then the resolved dependency trees, where a transitive reintroduction hides.
set -euo pipefail

fail=0

hits=$(git grep -l -e 'com\.amazonaws' -e 'aws-java-sdk' \
  -- '*.java' 'pom.xml' '*/pom.xml' '*/*/pom.xml' || true)
if [[ -n $hits ]]; then
  echo "AWS SDK v1 is referenced in tracked source:"
  echo "$hits" | sed 's/^/    /'
  fail=1
fi

trees=$(find . -path ./.git -prune -o -name deps.txt -print || true)
if [[ -z $trees ]]; then
  echo "No dependency:tree output found. Run 'mvn dependency:tree -DoutputFile=deps.txt' first;"
  echo "without it this check cannot see a transitive reintroduction and must not report a pass."
  exit 1
fi

hits=$(echo "$trees" | xargs grep -l 'com\.amazonaws' || true)
if [[ -n $hits ]]; then
  echo "AWS SDK v1 is on the resolved classpath transitively:"
  echo "$hits" | sed 's/^/    /'
  fail=1
fi

if (( fail )); then
  exit 1
fi
echo "AWS SDK v1 absent from tracked source and from every resolved dependency tree."
