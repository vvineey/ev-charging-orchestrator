# EV Charging Orchestrator

분산된 전기차 충전소의 상태·계량 데이터를 실시간 수집하고, 전력 스케줄과 원격 제어 명령을 안전하게 전달하는 충전 운영 플랫폼입니다.

## 해결하려는 문제

전기차 충전 서비스는 충전소 수가 증가할수록 다음 문제를 동시에 다뤄야 합니다.

- 충전소와 충전 포트의 상태를 실시간으로 파악해야 합니다.
- 계량 데이터가 중복되거나 순서가 바뀌어도 충전 세션과 요금이 틀어지지 않아야 합니다.
- AI가 계산한 전력 스케줄을 실제 장비에 안전하게 전달해야 합니다.
- 네트워크 단절, 프로세스 재시작, 소비자 장애 이후에도 데이터를 복구해야 합니다.
- 사용자 요청과 충전기 이벤트가 동시에 발생해도 세션 상태가 일관되어야 합니다.

## 핵심 기능

- 충전소·EVSE 등록과 운영 상태 관리
- 실시간 충전 상태와 계량 데이터 수집
- 충전 세션 시작·종료 및 이용 이력 관리
- 충전 포트별 ON/OFF 상태 반영
- AI 기반 충전 스케줄 계산 요청과 결과 적용
- 충전소별 전력 제약 검증
- 장애·재연결·중복 메시지 처리
- 운영자용 원격 제어와 상태 조회 API

## 사용자와 시스템 역할

| 역할 | 책임 |
|---|---|
| 사용자 | 충전소 조회, 충전 시작·종료, 진행 상태·이용 이력 확인 |
| 운영자 | 충전소·포트 등록, 장애 확인, 원격 제어, 요금·운영 정책 관리 |
| AI Worker | 충전 수요와 전력 제약을 입력받아 포트별 스케줄 계산 |
| Raspberry Pi | 센서 계측, USB 삽입·분리 감지, 포트 제어, 서버 통신 |
| OCPP/MQTT Adapter | 장비 프로토콜을 공통 도메인 이벤트로 변환 |

## 전체 아키텍처

```text
Raspberry Pi 5포트 테스트베드
        │ MQTT 현재 연동 / OCPP 전환 목표
        ▼
Device Adapter 또는 OCPP Gateway
        │
        ├── 충전기 상태·계량 이벤트 ──> Kafka
        │                                  ├── 상태 조회 모델
        │                                  ├── 요금·세션 처리
        │                                  ├── 장애 모니터링
        │                                  └── AI 입력 데이터
        │
사용자·운영자 API ──> Control Plane ──> PostgreSQL
                              │
                              └── AI 스케줄 요청 ──> RabbitMQ ──> AI Worker
                                                               │
                                      결과 저장·이벤트 발행 <───┘
                                                               │
                                                         Command Dispatcher
                                                               │
                                                               ▼
                                                        충전소 제어 명령
```

## 이벤트와 명령 흐름

### 충전기 이벤트

1. Raspberry Pi가 전압·전류·USB 상태를 읽습니다.
2. Adapter가 `ChargerTelemetryReceived` 또는 `ChargerStatusChanged` 이벤트로 변환합니다.
3. Kafka에 `stationId`를 메시지 키로 발행합니다.
4. 상태·세션·요금·모니터링 소비자가 각자 이벤트를 처리합니다.
5. 현재 상태는 조회 모델에 반영하고 원본 이벤트는 재처리할 수 있도록 보존합니다.

### AI 스케줄 요청

1. Control Plane이 스케줄 요청과 입력 데이터 버전을 저장합니다.
2. `requestId`를 포함해 RabbitMQ 작업 큐에 발행합니다.
3. AI Worker가 요청을 받아 계산합니다.
4. 결과를 저장한 뒤 `ScheduleGenerated` 이벤트를 발행합니다.
5. Command Dispatcher가 전력 제약과 현재 세션 상태를 재검증합니다.
6. 검증을 통과한 명령만 충전소에 전달합니다.

모든 소비자는 중복 전달을 전제로 하며, `requestId`·고유 제약·멱등 처리를 사용합니다. 브로커 사용만으로 end-to-end exactly-once를 가정하지 않습니다.

## 기술 스택과 선택 이유

| 영역 | 기술 | 선택 이유 |
|---|---|---|
| Backend | Java 21, Spring Boot | 트랜잭션·보안·운영 생태계와 장기 유지보수 |
| Gateway | Spring WebFlux, Netty | 장시간 유지되는 WebSocket 연결 처리 |
| Edge | C 및 Raspberry Pi 환경 | 기존 센서·Serial·USB 제어 코드와의 호환 |
| Device protocol | MQTT Adapter, OCPP 2.0.1 목표 | 현재 장비 검증과 상용 충전기 호환을 단계적으로 연결 |
| Event stream | Apache Kafka | 다수 소비자, offset 기반 재처리, 충전소별 순서 |
| Work queue | RabbitMQ | AI 작업의 ACK, 재시도, DLQ, Worker 확장 |
| Transaction DB | PostgreSQL | 세션·예약·명령 상태의 일관성 |
| Migration | Flyway | 스키마 변경 재현 |
| Cloud | AWS ECS/Fargate, ECR, RDS | 컨테이너 기반 배포와 관리형 운영 |
| Infrastructure | Terraform | AWS 환경 재현과 변경 추적 |
| Observability | Micrometer, OpenTelemetry, CloudWatch | API·브로커·DB·컨테이너 관측 |
| Test | JUnit 5, Testcontainers, Toxiproxy | 통합 환경과 장애 조건 재현 |
| Load test | k6 및 전용 OCPP Simulator | API·WebSocket·충전기 이벤트 부하 분리 |

## 개발 단계

1. 도메인 용어와 이벤트 계약 정의
2. 충전소·EVSE·충전 세션 모델 구현
3. Raspberry Pi MQTT Adapter 수직 슬라이스
4. Kafka 이벤트 발행과 조회 모델
5. OCPP Gateway 추가
6. RabbitMQ AI Worker와 결과 반영
7. 멱등성·Outbox·명령 상태 머신
8. AWS Staging 배포
9. 부하·장애·복구 검증
10. 측정 결과를 반영한 운영 구조 개선

## 하드웨어 테스트베드

Raspberry Pi 1대가 충전소 1곳을 재현하고, 5개의 포트를 EVSE 단위로 모델링합니다.

- LOAD3: 포트 1~3
- LOAD2: 포트 4~5
- 포트 상태 변화: USB 삽입·분리 이벤트로 인식
- 계측: 전압과 전류를 읽고 순간 전력(W)을 계산
- 기존 장비 연동: C, WiringPi, WiringSerial, Mosquitto 기반 MQTT
- 상용화 통신 목표: OCPP 2.0.1 over WebSocket Secure

첨부 하드웨어는 실제 고전력 EV 충전기가 아니라 계측·출력 제어·통신 흐름을 검증하는 축소형 테스트베드입니다.

## 부하와 장애 테스트 기준

초기 목표는 5포트 충전소 2,000곳, 총 10,000개 포트입니다. 아래 수치는 측정 전 가정이며 실제 성능 결과가 아닙니다.

- 충전소 WebSocket 2,000개
- 평균 200 messages/sec
- 순간 1,000 messages/sec
- 6시간 지속 부하
- 충전소 재접속 폭주
- 중복·역순 이벤트
- Kafka 소비자 중단과 offset 재처리
- RabbitMQ Worker 종료와 메시지 재전달
- DB 또는 외부 AI 서버 지연

기록 항목은 p50/p95/p99 지연, 처리량, 오류율, consumer lag, DB 자원, backlog 복구 시간, 중복·유실 검증 결과입니다. 측정 결과가 없는 수치는 성과로 표현하지 않습니다.
