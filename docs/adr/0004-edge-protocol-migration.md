# ADR-0004: MQTT 기반 물리 연동에서 OCPP로 단계적 전환

- 상태: Proposed
- 제안일: 2026-09-13

## 문맥

현재 Raspberry Pi 코드는 Mosquitto MQTT로 계측 JSON을 발행한다. 목표 백엔드는 상용 충전기 호환을 위해 OCPP 2.0.1을 사용한다. 물리 장비 검증을 OCPP 구현 완료까지 미루면 하드웨어와 서버를 동시에 검증할 수 없다.

## 결정 후보

1. 즉시 OCPP만 구현한다.
2. MQTT 연동을 폐기하고 시뮬레이터만 사용한다.
3. MQTT Adapter로 현재 장비를 먼저 연결하고, 동일 도메인 이벤트 계약 뒤에 OCPP Gateway를 추가한다.

## 제안

3번을 제안한다. MQTT는 하드웨어 수직 슬라이스와 계측 검증에 사용하고, OCPP는 별도 Gateway 수직 슬라이스로 구현한다. 두 경로 모두 `ChargerTelemetryReceived`, `ChargerStatusChanged`, `ChargingCommandRequested` 같은 프로토콜 중립 이벤트로 변환한다.

## 장점

- 실제 Raspberry Pi 검증과 상용 프로토콜 개발을 병렬화할 수 있다.
- 기존 MQTT payload의 불일치를 Adapter 경계에서 흡수할 수 있다.
- OCPP 도입 후에도 하드웨어 드라이버와 도메인 로직을 재사용할 수 있다.

## 남은 검증

- 최종 JSON 필드와 timestamp 단위
- LOAD2/LOAD3 실제 포트 매핑
- Raspberry Pi에서 출력 ON/OFF 명령을 수신·적용하는 경로
- MQTT 재연결 및 오프라인 데이터 처리 정책
