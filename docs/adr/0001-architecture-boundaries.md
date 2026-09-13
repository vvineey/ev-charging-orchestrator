# ADR-0001: Gateway와 Control Plane 분리

- 상태: Accepted
- 결정일: 2026-09-13

## 문맥

충전기 WebSocket 연결은 장시간 유지되며, 사용자·운영자 API와 다른 확장·장애 특성을 갖는다.

## 결정

`ocpp-gateway`, `control-plane`, `telemetry-worker`, `ai-worker`를 별도 실행 단위로 둔다. 업무 도메인은 모듈형 구조로 시작하고, 모든 기능을 무리하게 마이크로서비스로 쪼개지 않는다.

## 결과

Gateway는 연결 수 기준, Worker는 큐 적체 기준, Control Plane은 API 부하 기준으로 독립 확장할 수 있다. 대신 이벤트 계약과 추적 ID를 먼저 고정해야 한다.
