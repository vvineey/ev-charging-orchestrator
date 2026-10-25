# WSS Outbox broker quorum failure experiment

Run `./load-tests/wss-outbox-quorum/run.sh [absolute-result-json-path]` with Docker and JDK 21. The script creates an isolated Kafka 4.1.1 cluster with three dedicated controllers and three brokers, then removes its containers and volumes on exit. The controllers remain available when two brokers are killed, so the test can observe the output topic's ISR falling from 3 to 1 while its `min.insync.replicas` stays at 2.

The opt-in JUnit sends a fixed 2,500 Wh transaction over WSS to the real OCPP Gateway and control-plane Inbox. The closeout writer, Outbox table, Polling Relay and independent effect consumer are existing **test-only** implementations; this experiment does not establish an operational BILLABLE or production Outbox path. It first records a committed Outbox row, then kills the output leader and another broker. The Relay must fail without marking that row published. After both brokers restart and ISR returns to 3, a fresh Relay publishes the same event ID and an independent consumer applies one effect. The JSON result records checkpoints and final source/Outbox/Kafka/effect reconciliation.

The experiment uses one transaction, one partition and one host. It does not test network partitions, retention loss, multi-host availability or a production Relay deployment.
