#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
OUTPUT_FILE="${1:-$ROOT_DIR/apps/telemetry-worker/build/experiment-results/kafka-three-broker-failover.json}"
case "$OUTPUT_FILE" in
  /*) ;;
  *) OUTPUT_FILE="$ROOT_DIR/$OUTPUT_FILE" ;;
esac

cd "$ROOT_DIR"
if [[ -z "${JAVA_HOME:-}" && -x /usr/libexec/java_home ]]; then
  export JAVA_HOME="$(/usr/libexec/java_home -v 21)"
fi
docker compose -f "$COMPOSE_FILE" down -v --remove-orphans
cleanup() { docker compose -f "$COMPOSE_FILE" down -v --remove-orphans; }
trap cleanup EXIT
docker compose -f "$COMPOSE_FILE" up -d --wait
EVC_KAFKA_FAILOVER_TEST=true EVC_KAFKA_FAILOVER_OUTPUT="$OUTPUT_FILE" \
  ./gradlew :apps:telemetry-worker:test \
  --tests '*KafkaThreeBrokerFailoverTest' --rerun-tasks
printf 'Result: %s\n' "$OUTPUT_FILE"
