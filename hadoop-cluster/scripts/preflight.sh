#!/usr/bin/env bash
set -euo pipefail
for tool in git docker java sbt; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "Missing executable: $tool" >&2
    exit 1
  }
done
docker info >/dev/null
docker compose version
printf 'All local tools found.\n'
