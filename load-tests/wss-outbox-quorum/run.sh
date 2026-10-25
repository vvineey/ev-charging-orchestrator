#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/compose.yaml"
OUTPUT_FILE="${1:-$ROOT_DIR/apps/control-plane/build/experiment-results/wss-outbox-quorum.json}"
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
docker compose -f "$COMPOSE_FILE" exec -T broker11 \
  /opt/kafka/bin/kafka-topics.sh --bootstrap-server broker11:29092 \
  --create --topic charging.transaction.observed.v1 --partitions 1 \
  --replication-factor 3 --config min.insync.replicas=2
EVC_WSS_OUTBOX_QUORUM_TEST=true \
  EVC_WSS_OUTBOX_QUORUM_COMPOSE="$COMPOSE_FILE" \
  EVC_WSS_OUTBOX_QUORUM_OUTPUT="$OUTPUT_FILE" \
  ./gradlew :apps:control-plane:test \
  --tests 'com.evcharging.controlplane.transaction.OcppGatewayToInboxIntegrationTest.fixedWssTransactionKeepsOutboxIntentAcrossWriterCrash' \
  --rerun-tasks --offline --no-daemon --no-parallel
printf 'Result: %s\n' "$OUTPUT_FILE"
