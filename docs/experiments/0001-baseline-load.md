# EXP-0001: 충전소 이벤트 기준 부하

- 상태: Planned
- 증거 수준: E0 목표 / 실행 후 E5

## 가설

2,000개 충전소와 10,000개 EVSE를 시뮬레이션해도 Kafka 소비 지연과 PostgreSQL 조회 모델 갱신이 합의한 목표 안에 들어온다.

## 부하 모델

- WebSocket: 2,000 connections
- 가동 포트: 20%
- MeterValue: 15초 주기
- Heartbeat: 60초 주기
- 평균 목표: 200 messages/sec
- 순간 목표: 1,000 messages/sec

## 기록할 값

p50/p95/p99 지연, 처리량, 오류율, Kafka consumer lag, DB CPU/IO, 재시작 후 복구 시간, 중복·유실 검증 결과를 동일한 환경에서 기록한다.

## 실행 도구

전용 OCPP Simulator, k6 REST 시나리오, Testcontainers 로컬 기준선, AWS Staging 반복 측정.
