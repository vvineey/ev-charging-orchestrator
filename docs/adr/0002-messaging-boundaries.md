# ADR-0002: Kafka와 RabbitMQ 역할 분리

- 상태: Accepted
- 결정일: 2026-09-13

## 결정 질문

충전기 상태 이벤트와 AI 스케줄 요청의 서로 다른 전달 의미를 하나의 브로커로 처리할 것인가?

## 결정

- 충전기 상태·계량 이벤트: Kafka
- AI 스케줄 계산 요청: RabbitMQ
- AI 결과: PostgreSQL 저장 후 `ScheduleGenerated` 이벤트를 Kafka에 발행
- 실제 장비 명령: 별도 Dispatcher가 명령 상태와 멱등성 검증 후 전송

## 이유

Kafka는 여러 소비자가 같은 이벤트를 독립적으로 읽고 장애 후 offset부터 재처리하기에 적합하다. RabbitMQ는 하나의 Worker가 작업을 맡아 완료 후 ACK하고, 실패 시 재시도·DLQ로 보내는 작업 큐 모델에 적합하다.

## 필수 안전장치

두 브로커 모두 중복 전달 가능성을 전제로 한다. `requestId` 고유 제약, idempotent consumer, 명령 상태 머신, 결과 저장 후 ACK 순서를 사용한다.

## 재검토 조건

실제 이벤트 소비자가 하나뿐이거나 AI 작업이 동기식으로 충분하다면 브로커를 통합한다. AWS 운영비 또는 관리 복잡도가 목표를 초과하면 Kinesis/SQS 대안을 동일 부하 조건에서 비교한다.
