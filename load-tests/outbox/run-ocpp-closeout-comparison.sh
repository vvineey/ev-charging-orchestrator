#!/usr/bin/env bash
set -euo pipefail

# One fresh Compose project per trial. Existing projects and volumes are never reset.
mode="${1:?Use BATCH_ACK or CDC}"
rate="${2:?Provide target closeouts per second}"
seconds="${3:?Provide input duration in seconds}"
run_id="${4:?Provide a unique lowercase run ID}"
scenario="${5:-normal}"
backlog_count="${6:-32}"
outage_seconds="${7:-10}"
if [[ "$mode" != "BATCH_ACK" && "$mode" != "CDC" ]]; then
  echo "mode must be BATCH_ACK or CDC" >&2; exit 2
fi
if [[ ! "$rate" =~ ^[0-9]+$ || "$rate" -lt 1 || ! "$seconds" =~ ^[0-9]+$ || "$seconds" -lt 1 ]]; then
  echo "rate and seconds must be positive integers" >&2; exit 2
fi
if [[ ! "$run_id" =~ ^[a-z0-9-]+$ ]]; then
  echo "run ID must contain only lowercase letters, digits, and hyphens" >&2; exit 2
fi
if [[ "$scenario" != "normal" && "$scenario" != "recovery" && "$scenario" != "cdc-backlog" ]]; then
  echo "scenario must be normal, recovery, or cdc-backlog" >&2; exit 2
fi
if [[ "$scenario" == "cdc-backlog" && "$mode" != "CDC" ]]; then
  echo "cdc-backlog requires CDC mode" >&2; exit 2
fi
if [[ "$scenario" == "cdc-backlog" && (
      ! "$backlog_count" =~ ^[0-9]+$ || "$backlog_count" -lt 1 || "$backlog_count" -gt 1000 ||
      ! "$outage_seconds" =~ ^[0-9]+$ || "$outage_seconds" -lt 1 || "$outage_seconds" -gt 120
    ) ]]; then
  echo "backlog count must be 1..1000 and outage seconds 1..120" >&2; exit 2
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "Set JAVA_HOME to JDK 21" >&2; exit 2
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
compose_file="$repo_root/load-tests/outbox/compose.yaml"
project="exp0004-${run_id}"
result_dir="$repo_root/apps/control-plane/build/experiment-results"
resource_file="$result_dir/exp0004-resources-${run_id}.csv"
log_file="$result_dir/exp0004-run-${run_id}.log"
mkdir -p "$result_dir"
if [[ -n "$(docker compose -p "$project" -f "$compose_file" ps -a -q)" ]]; then
  echo "Compose project already exists: $project; choose a new run ID" >&2; exit 2
fi
docker compose -p "$project" -f "$compose_file" up -d --wait
if [[ "$mode" == "BATCH_ACK" ]]; then
  docker compose -p "$project" -f "$compose_file" stop connect
fi
printf 'sampledAtUtc,container,cpuPercent,memoryUsage\n' > "$resource_file"
(
  while :; do
    ids=()
    while IFS= read -r id; do [[ -n "$id" ]] && ids+=("$id"); done < <(
      docker compose -p "$project" -f "$compose_file" ps --status running -q)
    if (( ${#ids[@]} > 0 )); then
      sampled_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      while IFS= read -r sample; do
        [[ -n "$sample" ]] && printf '%s,%s\n' "$sampled_at" "$sample" >> "$resource_file"
      done < <(docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "${ids[@]}" 2>/dev/null)
    fi
    sleep 1
  done
) &
sampler_pid=$!
cleanup() {
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
  docker compose -p "$project" -f "$compose_file" stop >/dev/null 2>&1 || true
}
trap cleanup EXIT
cd "$repo_root"
test_method="measuresOcppDerivedCloseoutCommitToKafkaObservation"
if [[ "$scenario" != "normal" ]]; then
  test_method="deliveryResumesAfterDispatcherUnavailable"
fi
COMPOSE_PROJECT_NAME="$project" \
EVC_EXP0004_RATE_PILOT="$([[ "$scenario" == "normal" ]] && echo true || echo false)" \
EVC_EXP0004_RECOVERY="$([[ "$scenario" != "normal" ]] && echo true || echo false)" \
EVC_EXP0004_BACKLOG_COUNT="$([[ "$scenario" == "cdc-backlog" ]] && echo "$backlog_count" || echo 1)" \
EVC_EXP0004_OUTAGE_SECONDS="$([[ "$scenario" == "cdc-backlog" ]] && echo "$outage_seconds" || echo 1)" \
EVC_EXP0004_EXTERNAL=true \
EVC_EXP0004_RELAY_MODE="$mode" \
EVC_EXP0004_RATE_PER_SECOND="$rate" \
EVC_EXP0004_SECONDS="$seconds" \
EVC_EXP0004_RUN_ID="$run_id" \
  ./gradlew :apps:control-plane:test \
    --tests "com.evcharging.controlplane.transaction.Exp0004CloseoutRatePilotTest.$test_method" \
    --rerun-tasks --no-daemon --console=plain > "$log_file" 2>&1 || {
      tail -80 "$log_file" >&2
      exit 1
    }
tail -10 "$log_file"
