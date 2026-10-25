# Three broker leader failure experiment

Run from the repository root with Java 21 and Docker Desktop/Engine:

```bash
load-tests/kafka-failover/run.sh
```

The script resets only the `evc-kafka-failover` Compose project, starts three Kafka 4.1.1 combined broker/controller nodes, runs the opt-in JUnit experiment, and removes that project's containers and volumes afterward. Pass an output JSON path as the first argument to retain each repetition separately. The default result is under `apps/telemetry-worker/build/experiment-results/`.

The test creates a one-partition, replication-factor-3 topic with `min.insync.replicas=2`. A producer with `acks=all`, idempotence and retries sends 100 synthetic IDs while the partition leader receives `SIGKILL`. It records ACK/exception and latency per send, observes the new leader and ISR size 2, retries IDs without an ACK, restarts the killed broker, observes ISR size 3, and consumes the entire topic to reconcile physical records and unique IDs. The result timestamps are local client observations; they do not isolate broker election time from client metadata refresh, timeout, Docker startup, or polling delay.

This is an isolated Kafka E4 test. It does not exercise the deployed application, database, outbox, multi-partition traffic, network partitions, or disk loss.
