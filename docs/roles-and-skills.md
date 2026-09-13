# 프로젝트 역할과 기술 스킬

이 문서는 기능 단위가 아니라 책임과 검증 증거 단위로 작업을 나누기 위한 기준입니다. 한 사람이 여러 역할을 맡을 수 있지만, 각 역할의 완료 조건은 분리해 기록합니다.

## 역할 분해

| 역할 | 핵심 스킬 | 책임 | 주요 산출물 |
|---|---|---|---|
| 도메인·CPO Backend | Java, Spring, DDD, 트랜잭션 | 충전소·EVSE·세션·예약·명령 상태 모델 | API, 상태 머신, DB migration |
| Edge Hardware | C, Raspberry Pi, Serial, USB, GPIO | 계측·USB 상태 감지·포트 ON/OFF | 장비 Adapter, 프레임 명세, 실물 테스트 |
| OCPP Protocol | WebSocket, OCPP 2.0.1, 인증 | 장비 연결·메시지 검증·프로토콜 변환 | Gateway, 시퀀스 테스트 |
| Event Platform | Kafka, Schema, Consumer Group | 상태·계량 이벤트 발행·재처리·조회 모델 | 토픽 정책, Consumer, Projection |
| AI Integration | Python, FastAPI, RabbitMQ | 스케줄 요청·ACK·재시도·결과 반영 | Worker, 작업 계약, DLQ 처리 |
| Data Integrity | PostgreSQL, Outbox, Idempotency | 중복·역순·동시 요청에도 정합성 유지 | 제약조건, Outbox, 멱등 테스트 |
| Cloud Platform | AWS, ECS/Fargate, Terraform, IAM | 환경 재현·배포·비밀·네트워크 | IaC, CI/CD, 운영 Runbook |
| Reliability·Performance | k6, OCPP Simulator, Toxiproxy | 부하·장애·복구 측정 | 실험계획, 원본 결과, 대시보드 |
| Evidence·Documentation | ADR, Issue, PR, 기술 문서 | 결정과 구현 근거의 추적성 유지 | ADR, 실험 보고서, 증거 인덱스 |

## 작업 분해 규칙

각 작업은 다음 순서로 생성합니다.

1. Issue에 문제·제약·완료 기준을 작성합니다.
2. 되돌리기 어렵거나 장애·정합성에 영향을 주는 결정이면 ADR을 `Proposed`로 작성합니다.
3. ADR 승인 후 구현 PR을 생성하고 ADR·Issue·테스트를 연결합니다.
4. 기능 테스트는 E3, 장애 복구는 E4, 성능 수치는 E5로 구분합니다.
5. 실제 결과가 없는 목표·예상치는 성과로 표현하지 않습니다.

## 첫 번째 수직 슬라이스 담당 경계

```text
Edge Hardware
  → MQTT Adapter
  → ChargerTelemetryReceived
  → Kafka
  → Control Plane Projection
  → PostgreSQL

Control Plane
  → RabbitMQ ai-schedule-request
  → AI Worker
  → ScheduleGenerated
  → Command Dispatcher
```

## 역할 병합 예시

- 1인 개발: 도메인·CPO Backend + Data Integrity + Evidence
- 2인 개발: Backend/Data와 Edge/OCPP로 분리
- 3인 개발: Backend, Edge/OCPP, Cloud/Performance
- 4인 이상: AI Integration과 Reliability를 별도 담당

역할을 합쳐도 하나의 사람이 작성한 결정과 테스트는 PR에서 명시적으로 구분합니다.
