#!/usr/bin/env bash
set -euo pipefail

# Resets only the isolated EXP-0003 Compose project before each independent run.
# Requires JDK 21 in JAVA_HOME, Docker Desktop, and a running Docker daemon.
mode="${1:?Use POLLING or CDC}"
batch="${2:?Use a positive batch size (ignored for CDC)}"
run_id="${3:?Provide a run ID such as c0-r2}"
if [[ "$mode" != "POLLING" && "$mode" != "CDC" ]]; then
  echo "mode must be POLLING or CDC" >&2
  exit 2
fi
if [[ ! "$batch" =~ ^[0-9]+$ || "$batch" -lt 1 ]]; then
  echo "batch must be a positive integer" >&2
  exit 2
fi
if [[ ! "$run_id" =~ ^[a-z0-9-]+$ ]]; then
  echo "run ID must contain only lowercase letters, digits, and hyphens" >&2
  exit 2
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "Set JAVA_HOME to a JDK 21 installation" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
compose_file="$repo_root/load-tests/outbox/compose.yaml"
result_file="$repo_root/apps/control-plane/build/experiment-results/exp0003-$run_id.json"

docker compose -f "$compose_file" down -v
docker compose -f "$compose_file" up -d --wait
cd "$repo_root"
./gradlew :apps:control-plane:test \
  --tests com.evcharging.controlplane.transaction.Exp0003OutboxBurstTest \
  -Pexp0003Burst=true \
  -Pexp0003BurstMode="$mode" \
  -Pexp0003BurstBatchSize="$batch" \
  -Pexp0003BurstEvents=1000 \
  -Pexp0003BurstOutput="$result_file" \
  --no-daemon --console=plain
cat "$result_file"
