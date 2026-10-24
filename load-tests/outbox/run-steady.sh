#!/usr/bin/env bash
set -euo pipefail

# One isolated normal-running input trial. Reset only the EXP-0003 Compose project.
mode="${1:?Use POLLING or CDC}"
rate="${2:?Provide target events per second}"
seconds="${3:?Provide input duration in seconds}"
run_id="${4:?Provide a lowercase run ID}"
cdc_poll_ms="${5:-default}"
if [[ "$mode" != "POLLING" && "$mode" != "CDC" ]]; then
  echo "mode must be POLLING or CDC" >&2
  exit 2
fi
if [[ ! "$rate" =~ ^[0-9]+$ || "$rate" -lt 1 || ! "$seconds" =~ ^[0-9]+$ || "$seconds" -lt 1 ]]; then
  echo "rate and seconds must be positive integers" >&2
  exit 2
fi
if [[ ! "$run_id" =~ ^[a-z0-9-]+$ ]]; then
  echo "run ID must contain only lowercase letters, digits, and hyphens" >&2
  exit 2
fi
if [[ "$cdc_poll_ms" != "default" && "$cdc_poll_ms" != "500" && "$cdc_poll_ms" != "50" ]]; then
  echo "CDC poll interval must be default, 500, or 50 ms" >&2
  exit 2
fi
if [[ "$mode" != "CDC" && "$cdc_poll_ms" != "default" ]]; then
  echo "CDC poll interval applies only to CDC mode" >&2
  exit 2
fi
if [[ -z "${JAVA_HOME:-}" ]]; then
  echo "Set JAVA_HOME to a JDK 21 installation" >&2
  exit 2
fi

repo_root="$(cd "$(dirname "$0")/../.." && pwd)"
compose_file="$repo_root/load-tests/outbox/compose.yaml"
result_file="$repo_root/apps/control-plane/build/experiment-results/exp0003-steady-$run_id.json"
resource_file="$repo_root/apps/control-plane/build/experiment-results/exp0003-resources-$run_id.csv"

docker compose -f "$compose_file" down -v
docker compose -f "$compose_file" up -d --wait
mkdir -p "$(dirname "$result_file")"
printf 'sampledAtUtc,container,cpuPercent,memoryUsage\n' > "$resource_file"
(
  while :; do
    container_ids=()
    while IFS= read -r container_id; do
      [[ -n "$container_id" ]] && container_ids+=("$container_id")
    done < <(docker compose -f "$compose_file" ps --status running -q)
    if (( ${#container_ids[@]} > 0 )); then
      sampled_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      while IFS= read -r sample; do
        [[ -n "$sample" ]] && printf '%s,%s\n' "$sampled_at" "$sample" >> "$resource_file"
      done < <(docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' "${container_ids[@]}" 2>/dev/null)
    fi
    sleep 1
  done
) &
sampler_pid=$!
stop_sampler() {
  kill "$sampler_pid" 2>/dev/null || true
  wait "$sampler_pid" 2>/dev/null || true
}
trap stop_sampler EXIT
cd "$repo_root"
./gradlew :apps:control-plane:test \
  --tests com.evcharging.controlplane.transaction.Exp0003OutboxSteadyTest \
  -Pexp0003Steady=true \
  -Pexp0003SteadyMode="$mode" \
  -Pexp0003SteadyRate="$rate" \
  -Pexp0003SteadySeconds="$seconds" \
  -Pexp0003SteadyCdcPollMs="$cdc_poll_ms" \
  -Pexp0003SteadyOutput="$result_file" \
  --no-daemon --console=plain
stop_sampler
trap - EXIT
cat "$result_file"
