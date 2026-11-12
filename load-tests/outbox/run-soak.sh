#!/usr/bin/env bash
set -euo pipefail

# Isolated EXP-0003 CDC soak. Pilot: 300 seconds; formal: 21600 seconds.
seconds="${1:?Provide duration in seconds: 300 or 21600}"
run_id="${2:?Provide a lowercase run ID}"
if [[ "$seconds" != "300" && "$seconds" != "21600" ]]; then
  echo "duration must be 300 (pilot) or 21600 (formal) seconds" >&2
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
result_dir="$repo_root/apps/control-plane/build/experiment-results"
result_file="$result_dir/exp0003-soak-$run_id.json"
checkpoint_file="$result_dir/exp0003-soak-$run_id.jsonl"
resource_file="$result_dir/exp0003-soak-resources-$run_id.csv"
code_commit="$(git -C "$repo_root" rev-parse HEAD)"

docker compose -f "$compose_file" down -v
docker compose -f "$compose_file" up -d --wait
mkdir -p "$result_dir"
rm -f "$result_file" "$checkpoint_file" "$resource_file"
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
    sleep 30
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
  --tests com.evcharging.controlplane.transaction.Exp0003OutboxSoakTest \
  -Pexp0003Soak=true \
  -Pexp0003SoakRate=200 \
  -Pexp0003SoakSeconds="$seconds" \
  -Pexp0003SoakCdcPollMs=50 \
  -Pexp0003SoakCodeCommit="$code_commit" \
  -Pexp0003SoakOutput="$result_file" \
  -Pexp0003SoakCheckpoints="$checkpoint_file" \
  --no-daemon --console=plain
stop_sampler
trap - EXIT
cat "$result_file"
