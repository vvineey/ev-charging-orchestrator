# 개발 흐름과 검토 게이트

## 기본 순서

```text
요청
  → 역할·범위 분해
  → Issue 작성
  → ADR/계약 검토(Proposed)
  → 사용자 승인
  → 구현 PR
  → E3 기능 검증
  → E4 장애·복구 검증
  → E5 부하 검증
  → 증거·다음 작업 정리
```

## ADR 검토 원칙

서비스 경계, 브로커, 프로토콜, 데이터 계약, 인증, 배포, 정합성, 비용, 확장성에 영향을 주는 결정은 구현 전에 ADR로 제안합니다. ADR은 처음부터 `Accepted`로 작성하지 않고 `Proposed` 상태로 사용자에게 제출합니다.

각 작업의 마지막 보고에는 다음을 포함합니다.

- 검토할 ADR 경로와 상태
- 후보별 차이와 현재 권장안
- 비용·장애·정합성·운영에 미치는 영향
- 사용자가 승인하거나 수정해야 하는 질문
- API·이벤트 계약 또는 테스트 목표 중 추가 검토가 필요한 문서

사용자가 이해하고 승인하기 전에는 해당 결정에 종속된 구현을 진행하지 않습니다. 승인 후 ADR을 `Accepted`로 변경하고 구현 PR과 연결합니다.

## 역할 라우팅

| 작업 | 우선 역할 | 검증 |
|---|---|---|
| 충전소·EVSE·세션 API | 도메인 Backend, Data Integrity | E3, 동시성·멱등성 |
| Raspberry Pi telemetry | Edge Hardware, Event Platform | E3, 재연결 E4 |
| OCPP Gateway | OCPP Protocol, Cloud Platform | WebSocket·인증 E3/E4 |
| AI 스케줄 | AI Integration, Data Integrity | ACK·재시도·DLQ E4 |
| AWS 배포 | Cloud Platform, Reliability | 롤백·복구 E4 |
| 대규모 부하 | Reliability·Performance | p95/p99·처리량 E5 |

## 기록 위치

- 문제·완료 기준: GitHub Issue
- 결정과 트레이드오프: `docs/adr/`
- 반복 실험과 원본 수치: `docs/experiments/`
- 실제 장애의 원인·복구·재발 방지: `docs/runbooks/`
- 포트폴리오 근거 연결: `docs/portfolio/evidence-index.md`

측정하지 않은 목표값은 실적처럼 기록하지 않습니다. 공개 저장소에는 비밀·실제 인증정보·내부 배선 자료를 추가하지 않습니다.
