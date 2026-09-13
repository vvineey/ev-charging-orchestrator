# Decision Backlog

| ID | 질문 | 상태 | 필요한 증거 |
|---|---|---|---|
| DB-001 | Raspberry Pi 내부 모듈의 실제 통신 방식과 LOAD2/LOAD3 포트 매핑은 무엇인가? | Closed for mapping | LOAD3→1~3, LOAD2→4~5 확정; 내부 프레임은 추가 확인 |
| DB-002 | Kafka와 RabbitMQ의 AWS 운영 형태를 MSK/Amazon MQ로 할 것인가? | Open | 비용·운영·장애 실험 |
| DB-003 | PostgreSQL 단일 조회 모델로 충분한가? | Open | 지도 조회·상태 조회 부하 테스트 |
| DB-004 | AI 결과를 포트별 출력 제한으로 변환하는 규칙은 무엇인가? | Open | AI 계약서·안전 제약 |
| DB-005 | 실제 결제·로밍을 MVP에 포함할 것인가? | Open | 제품 범위 확인 |
| DB-006 | 기존 MQTT telemetry를 OCPP 2.0.1로 어떤 단계에 전환할 것인가? | Investigating | 기존 MQTT 코드와 OCPP 수직 슬라이스 비교 |
| DB-007 | JSON 계약은 `timestamp` ms인가 `timeStamp` sec인가, power/charging을 포함하는가? | Closed for contract | `timestamp`, `charging`, `power`, `voltage`, `current` 형식 확정; 실제 정밀도는 실행 캡처 필요 |
