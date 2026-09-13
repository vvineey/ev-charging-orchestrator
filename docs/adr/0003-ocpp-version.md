# ADR-0003: OCPP 2.0.1 우선 구현

- 상태: Accepted
- 결정일: 2026-09-13

## 결정

충전소 상위 통신은 OCPP 2.0.1 JSON over WSS로 구현하고, Raspberry Pi 내부 모듈 통신은 Adapter 뒤에 숨긴다.

## 이유

표준 메시지와 Smart Charging 확장으로 실제 CPO 시스템과 유사한 검증이 가능하다. 하드웨어 특화 명령은 Gateway 외부로 새지 않도록 protocol-agnostic domain event로 변환한다.

## 제한

첨부 장비는 실제 고전력 EV 충전기가 아니라 5포트 제어·계량 테스트베드다. 포트폴리오에서는 이 범위를 명확히 표시한다.
