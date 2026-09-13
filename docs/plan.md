# 개발 계획

## 목표

`E0` 1차 목표는 충전소 2,000개, 5포트 기준 10,000개 EVSE, 충전소당 WebSocket 1개입니다. 20% 가동률·계량 15초·Heartbeat 60초라는 초기 가정에서 평균 200 messages/sec와 순간 1,000 messages/sec를 검증 대상으로 둡니다.

## 단계

1. 도메인·이벤트 계약과 저장소 구조 확정
2. PostgreSQL 기반 충전소·EVSE·세션 상태 모델 구현
3. 기존 MQTT 기반 Raspberry Pi Adapter와 5포트 제어/계량 수직 슬라이스 구현
4. OCPP Gateway를 추가하고 MQTT/OCPP 양쪽을 동일한 도메인 이벤트로 변환
5. AI Worker와 RabbitMQ 작업 큐 구현
6. 결과 저장·멱등성·Transactional Outbox 구현
7. AWS 배포(Terraform, ECS/Fargate, RDS, MSK, Amazon MQ)
8. 100 → 2,000 충전소 부하·재접속·중복·장애 실험
9. 측정 결과를 바탕으로 병목 개선과 포트폴리오 정리

## 첫 Vertical Slice 완료 기준

- 한 대의 Raspberry Pi가 5개 포트 상태를 전송한다.
- 기존 MQTT payload의 필드명·timestamp 단위를 공식 계약으로 확정한다.
- 서버가 Boot/Heartbeat/StatusNotification을 수신한다.
- 상태 이벤트가 Kafka에 기록되고 PostgreSQL 조회 모델에 반영된다.
- AI 스케줄 요청이 RabbitMQ를 거쳐 Worker에서 처리된다.
- 결과 저장 전 Worker가 종료되어도 재전달 후 중복 제어된다.
- 명령 결과와 로그에 `stationId`, `evseId`, `requestId`, `correlationId`가 남는다.
