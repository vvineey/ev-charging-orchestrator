# EV Charging Orchestrator

충전사업자(CPO)를 위한 통합 운영 플랫폼을 설계·구현하는 프로젝트입니다. 충전기 상태 관제부터 거래 세션, 요금 계산, 결제, AI 전력 스케줄과 원격 제어까지 하나의 업무 흐름으로 연결하는 것이 목표입니다. Raspberry Pi는 센서·통신 경로를 검증하는 축소형 장비이며, 상용 충전기 연동은 OCPP 2.0.1 Gateway와 시뮬레이터로 별도 검증합니다.

> **현재 구현 범위:** 공통 telemetry 이벤트 계약과 별도 `apps/mqtt-adapter`의 MQTT 검증·변환·Kafka 발행 결과 관찰을 구현했습니다. `telemetry-worker`는 Kafka 동기 소비·계약 검증·JDBC/Flyway 최신 상태 저장·실패 정지와 같은 그룹 재시작, 내부 HTTPS 목록·단건 조회를 제공합니다. `control-plane`은 내부 HTTPS를 호출해 운영자용 목록·단건 조회를 제공합니다. 충전 거래·결제, RabbitMQ AI 작업, OCPP Gateway의 실제 업무 흐름은 후속입니다. 아래의 그 밖의 흐름은 **목표 설계 또는 검토 중인 설계**입니다.

## 해결하려는 문제

전기차 충전 서비스는 충전소 수가 증가할수록 다음 문제를 동시에 다뤄야 합니다.

- 충전소와 충전 포트의 상태를 실시간으로 파악해야 합니다.
- 충전기가 통신 단절 후 복귀해 거래 이벤트를 늦게·중복·역순으로 보내더라도 세션·요금·결제 금액이 틀어지지 않아야 합니다.
- AI가 계산한 전력 스케줄을 실제 장비에 안전하게 전달해야 합니다.
- 네트워크 단절, 프로세스 재시작, 소비자 장애 이후에도 데이터를 복구해야 합니다.
- 사용자 요청과 충전기 이벤트가 동시에 발생해도 세션 상태가 일관되어야 합니다.
- 운영자에게 상태·고장·충전 거래 내역을 제공하고, 결제 실패나 정산 차이를 추적해야 합니다.

## 핵심 기능

- 충전소·EVSE 등록과 운영 상태 관리
- 실시간 충전 상태와 계량 데이터 수집
- 충전 세션 시작·종료 및 이용 이력 관리
- 확정된 세션의 요금 계산, 결제 요청·결과·대사
- 충전 포트별 ON/OFF 상태 반영
- AI 기반 충전 스케줄 계산 요청과 결과 적용
- 충전소별 전력 제약 검증
- 장애·재연결·중복 메시지 처리
- 운영자용 원격 제어와 상태 조회 API

## 사용자와 시스템 역할

| 역할 | 책임 |
|---|---|
| 사용자 | 충전소 조회, 충전 시작·종료, 진행 상태·이용 이력 확인 |
| 운영자 | 충전소·포트 등록, 장애·거래·결제 상태 확인, 원격 제어, 요금·운영 정책 관리 |
| 결제 서비스/PG | 확정된 청구 건에 대한 결제 승인·취소·결과 통지 및 대사 |
| AI Worker | 충전 수요와 전력 제약을 입력받아 포트별 스케줄 계산 |
| Raspberry Pi | 센서 계측, USB 삽입·분리 감지, 포트 제어, 서버 통신 |
| OCPP/MQTT Adapter | 장비 프로토콜을 공통 도메인 이벤트로 변환 |

## 목표 아키텍처

```text
상용 충전기/시뮬레이터 ── OCPP Gateway ──┐
                                        ├── Kafka: 충전기 도메인 이벤트
Raspberry Pi 테스트베드 ─ MQTT Adapter ─┘      ├── 현재 상태·고장 조회 모델 → 운영자 API
                                               ├── 거래 세션 복구·확정 → 세션 이력 API
                                               │                       └── 요금·청구 → 결제 서비스 → PG
                                               └── AI 입력 갱신

사용자·운영자 API ── Control Plane ── PostgreSQL
                           └── RabbitMQ: AI 계산 작업 → AI Worker
                                                         └── 결과 반영 경계 → PostgreSQL 결과 저장
                                                                            └── Kafka: ScheduleGenerated
                                                                                       └── Command Dispatcher
                                                                                              └── 충전기 제어
```

각 화살표는 업무 책임을 보여 주는 **목표 흐름**이며, 서비스·토픽·테이블의 최종 개수나 배포 경계를 확정한 도면은 아닙니다. 최신 상태 조회 모델의 소유권과 결제 기능의 앱 배치는 설계 검토 중입니다.

## 이벤트와 명령 흐름

### 충전기 이벤트

1. 현재 Raspberry Pi는 전압·전류·순간 전력·충전 여부를 MQTT로 보냅니다. Adapter는 이를 `ChargerTelemetryReceived`로 변환해 `stationId` 키로 Kafka에 발행합니다. 로컬 환경에서 합성 MQTT 입력 한 건의 Kafka 발행·소비·DB 고정 값/이벤트 ID 일치와 commit offset 2·lag 0을 확인했습니다. 실장비와 API 전달은 후속 검증입니다.
2. 목표 설계에서는 OCPP 충전기의 상태·계량·거래 이벤트도 Gateway가 검증하고 프로토콜 중립 이벤트로 발행합니다.
3. 각 소비자는 필요한 이벤트를 독립 처리합니다. 현재 상태·고장 조회, 거래 세션 복구, AI 입력은 처리 목적과 재처리 기준이 다릅니다.
4. Kafka의 보존 기간 안에서는 소비자가 offset부터 다시 읽어 조회 모델을 복구할 수 있습니다. Kafka 기록 순서가 장비에서 발생한 거래 순서와 같다는 보장은 없으므로 거래 순번과 누적 계량값을 따로 검증해야 합니다.

현재 telemetry 계약에는 `transactionId`, 거래 순번, 누적 전력량, 고장 코드가 없습니다. 따라서 지금 구현만으로 정확한 충전 세션·요금·고장 이력을 제공한다고 주장하지 않습니다. 세션 복구에는 OCPP 거래 식별자·순번·누적 계량값 또는 동등한 별도 거래 계약이 필요합니다. Raspberry Pi의 계측값을 상용 거래·과금 값으로 사용하지 않습니다.

### 충전 거래에서 결제까지의 목표 흐름

1. 거래 소비자가 `(충전기 식별자, transactionId, seqNo)`를 기준으로 재전달·역순·누락을 식별하고, 시작·종료와 누적 계량값으로 세션을 확정합니다. 누락이나 불가능한 계량값은 자동 청구 대신 보류·조사 대상으로 기록합니다.
2. 확정된 세션에 당시 요금 정책을 적용해 청구 금액과 계산 근거를 저장합니다. 원시 telemetry 이벤트를 받을 때마다 결제하지 않습니다.
3. 결제 서비스가 고유한 청구 ID로 PG 승인을 요청하고 응답·통지·조회 결과를 대사합니다. 재시도는 동일한 멱등 키와 상태 전이를 사용해 중복 청구를 막습니다.
4. 운영자와 사용자는 상태 조회 모델과 확정된 세션·청구·결제 기록을 각각 조회합니다. 조회 지연과 결제 완료 여부를 혼동하지 않습니다.

### AI 스케줄 요청

1. Control Plane이 스케줄 요청과 입력 데이터 버전을 저장합니다.
2. `requestId`를 포함해 RabbitMQ 작업 큐에 발행합니다. DB 저장과 발행 사이의 손실을 막을 방법은 Outbox 검토 대상입니다.
3. AI Worker가 요청을 받아 계산합니다.
4. AI Worker의 출력을 결과 반영 경계(Control Plane 또는 전용 컴포넌트)가 검증·저장한 뒤 `ScheduleGenerated` 이벤트를 발행합니다. 결과 저장과 Kafka 발행의 일관성도 Outbox 검토 대상입니다.
5. Command Dispatcher가 전력 제약과 현재 세션 상태를 재검증합니다.
6. 검증을 통과한 명령만 충전소에 전달합니다.

이 AI 흐름은 승인된 목표 설계이며 현재 동작하는 기능이 아닙니다. RabbitMQ는 계산 작업을 한 Worker에 할당하고 ACK·재전달·실패 격리를 다루는 후보입니다. Kafka도 작업 처리에 사용할 수 있으므로 두 브로커를 유지할지는 소비자 수, 재처리 필요성, 장애·운영 비용을 실험해 재검토합니다. 브로커 사용만으로 종단 간 exactly-once나 결제 멱등성이 보장되지는 않습니다.

## 기술 스택과 선택 이유

| 영역 | 기술 | 선택 이유 |
|---|---|---|
| Backend | Java 21, Spring Boot | 트랜잭션·보안·운영 생태계와 장기 유지보수 |
| Gateway | Spring WebFlux, Netty | 장시간 유지되는 WebSocket 연결 처리 |
| Edge | C 및 Raspberry Pi 환경 | 기존 센서·Serial·USB 제어 코드와의 호환 |
| Device protocol | MQTT Adapter, OCPP 2.0.1 목표 | 현재 장비 검증과 상용 충전기 호환을 단계적으로 연결 |
| Event stream | Apache Kafka | 독립 소비자와 보존 기간 내 재처리; 동일 키의 파티션 기록 순서 |
| Work queue | RabbitMQ 목표 | AI 작업 ACK, 재전달, 실패 작업 분리와 Worker 확장 |
| Transaction DB | PostgreSQL 목표 | 확정 세션·청구·결제 상태의 제약과 트랜잭션 |
| Payment | 외부 PG 연동 목표 | 승인·취소·통지·조회 결과와 내부 청구 상태 대사 |
| Migration | Flyway | 스키마 변경 재현 |
| Cloud | AWS ECS/Fargate, ECR, RDS | 컨테이너 기반 배포와 관리형 운영 |
| Infrastructure | Terraform | AWS 환경 재현과 변경 추적 |
| Observability | Micrometer, OpenTelemetry, CloudWatch | API·브로커·DB·컨테이너 관측 |
| Test | JUnit 5, Testcontainers, Toxiproxy | 통합 환경과 장애 조건 재현 |
| Load test | k6 및 전용 OCPP Simulator | API·WebSocket·충전기 이벤트 부하 분리 |

## 실행 앱과 공통 모듈

| 경로 | 현재 구현과 책임 |
|---|---|
| `apps/mqtt-adapter` | MQTT 연결·구독·재연결, 입력 검증·EVSE 매핑, 공통 이벤트 생성·Kafka 발행·Future 결과 관찰 |
| `apps/telemetry-worker` | Kafka envelope v1 검증·동기 소비·PostgreSQL 최신 상태 저장·조건부 갱신·실패 정지·내부 HTTPS 조회 |
| `apps/ocpp-gateway` | 실행 앱 골격. OCPP 2.0.1 연결·정규화·발행·명령 전송은 후속 구현 |
| `apps/control-plane` | 실행 앱 골격. 사용자·운영자 업무 API는 후속 구현 |
| `apps/ai-worker` | 실행 앱 골격. RabbitMQ 계산 작업 처리는 후속 구현 |
| `apps/charger-simulator` | 실행 앱 골격. 장비 입력·장애 시나리오 재현은 후속 구현 |
| `modules/messaging-contract` | envelope v1, telemetry payload, EVSE 식별자와 계약 검증. MQTT·Kafka 구현에 의존하지 않음 |
| `modules/charging-domain`, `modules/test-support` | 공통 도메인·테스트 지원 모듈 골격 |

실행 앱은 서로 직접 의존하지 않고 공통 모듈을 사용합니다. MQTT 설정과 Paho·Spring Integration MQTT·Kafka publisher 의존성은 `mqtt-adapter`가 소유합니다.

## 로컬 실행과 검증

Java 21과 실행 중인 Docker가 필요합니다. 프로젝트 루트에서 `.env.example`을 `.env`로 복사한 뒤 관리자·앱 비밀번호 두 개를 서로 다른 임의 값으로 바꿉니다. `.env`는 Git에서 제외합니다. Compose는 루트의 `.env`를 읽고, 두 앱은 `local` profile에서만 같은 파일을 properties 형식으로 읽습니다. 비밀번호는 따옴표·`#`·`$`가 없는 임의의 hex 문자열을 권장합니다.

```sh
cp .env.example .env
# .env의 두 비밀번호를 바꾼 후 실행
# JAVA_HOME을 Java 21로 지정하고 Python 3로 로컬 TLS 자료 생성
python3 scripts/create-local-telemetry-tls.py
docker compose config --quiet
docker compose up -d --wait postgres kafka mosquitto
docker compose run --rm kafka-init
./gradlew :apps:telemetry-worker:bootRun --args='--spring.profiles.active=local'
# worker를 실행한 채 별도 터미널에서 MQTT 입력을 받아 Kafka로 발행
./gradlew :apps:mqtt-adapter:bootRun --args='--spring.profiles.active=local'
# 두 앱을 실행한 채 다른 터미널에서 운영자 조회 진입점을 시작
./gradlew :apps:control-plane:bootRun --args='--spring.profiles.active=local'
```

Windows에서는 파일 복사에 `Copy-Item .env.example .env`, 앱 실행에 `.\gradlew.bat`을 사용합니다. `bootRun`의 작업 디렉터리는 루트로 고정되어 있습니다. JAR도 루트에서 실행하거나 `LOCAL_ENV_FILE`을 `.env`의 절대 경로로 지정합니다. local profile에서 파일이 없으면 시작하지 않습니다.

| 구성 | 호스트 앱의 접속 주소 | Compose 내부 접속 주소 |
|---|---|---|
| PostgreSQL 18.3 | `127.0.0.1:15432/telemetry_current` | `postgres:5432/telemetry_current` |
| Kafka 4.1.1 | `127.0.0.1:19092` | `kafka:29092` |
| MQTT Mosquitto 2.0 | `tcp://127.0.0.1:11883` | `tcp://mosquitto:1883` |
| control-plane 운영자 API | `127.0.0.1:18580` | 별도 Compose 서비스 없음 |

기본 Compose 프로젝트는 `evc-current-local`이며 DB·Kafka는 별도 named volume을 사용합니다. 다른 폴더의 기존 테스트 DB·Kafka와 공유하지 않습니다. 모든 공개 포트는 호스트 loopback에만 바인딩합니다. Kafka는 PLAINTEXT·단일 broker·복제/최소 ISR 1, MQTT는 익명 접속의 합성 데이터용 로컬 구성입니다. 원격 장비·운영 배포에는 별도 인증·암호화 설정이 필요합니다.

PostgreSQL 초기화 스크립트는 빈 volume에서 DB와 앱 계정 `telemetry_app`의 소유권을 준비합니다. 앱 계정은 superuser·DB 생성·역할 생성 권한이 없으며, 이 계정으로 Flyway가 업무 테이블을 생성합니다. DB 설정은 `SPRING_DATASOURCE_*`를 사용합니다. 과거 `DB_URL` 등의 별칭은 지원하지 않습니다. 초기화 스크립트는 데이터가 있는 volume에 재실행되지 않으므로 `.env`의 초기 계정·비밀번호 변경만으로 기존 DB 비밀번호가 바뀌지 않습니다. 기존 Flyway V1을 가진 다른 DB를 재사용하거나 자동 repair하지 않습니다.

`kafka-init`은 `charger.telemetry`를 partition 1·복제 1·최소 ISR 1·delete·7일 보존으로 명시 생성합니다. broker의 자동 토픽 생성은 끕니다. 재실행 시 기존 토픽을 보존하며 설정 변경을 자동 적용하지 않습니다. 앱 producer는 String key/value·`acks=all`·idempotence를 사용합니다. 단일 broker 구성에서 복제 장애 복구나 종단 간 exactly-once를 보장하지 않습니다. Kafka cluster ID는 같은 volume을 재사용하는 동안 유지합니다.

local profile의 worker는 migration 후 `telemetry-current-state-v1`로 계속 소비하고 `https://localhost:18443`에서 내부 조회를 제공합니다. Kafka 주소·topic이 없는 기본 실행은 소비자가 활성화되지 않습니다. `docker compose stop`은 DB·Kafka 데이터를 보존합니다. volume 삭제는 데이터 삭제이므로 일반 중지 절차에 포함하지 않습니다.

local profile을 사용하지 않을 때 Adapter는 브로커 설정 없이 시작할 수 있습니다. worker는 접근 가능한 PostgreSQL과 `SPRING_DATASOURCE_URL`, `SPRING_DATASOURCE_USERNAME`, `SPRING_DATASOURCE_PASSWORD`, `TELEMETRY_TLS_KEY_STORE`, `TELEMETRY_TLS_KEY_STORE_PASSWORD`가 필요하며 설정 누락·접속·migration·TLS 실패 시 시작하지 않습니다. `mqtt.url`, `mqtt.topic`, `spring.kafka.bootstrap-servers`, `telemetry.kafka-topic`를 직접 전달하는 실행 방식도 유지합니다. 기본 `mqtt.client-id=telemetry-worker`와 자동 재연결·clean session은 기존대로이며 local profile은 client ID를 `mqtt-adapter-local`로 지정합니다. 이전 worker와 Adapter를 같은 MQTT 구독으로 동시에 실행하지 않습니다.

### worker 내부 HTTPS 조회

- 목록: `GET /internal/v1/stations/{stationId}/telemetry/latest` → `{ "stationId": "...", "evses": [...] }`. 저장된 EVSE만 오름차순으로 반환하며 관측이 없으면 빈 목록입니다.
- 단건: `GET /internal/v1/stations/{stationId}/evses/{evseId}/telemetry/latest`. `stationId`, `evseId`, `charging`, `power`(W), `voltage`(V), `current`(A), `occurredAt`, `receivedAt`, `updatedAt`, `lastEventId`를 반환합니다. 시각은 UTC ISO-8601과 DB microsecond 정밀도입니다.
- 오류: 잘못된 입력은 400/`INVALID_TELEMETRY_QUERY`, 단건 없음은 404/`TELEMETRY_NOT_FOUND`, DB 조회 불가는 503/`TELEMETRY_QUERY_UNAVAILABLE`. 오류 JSON은 `code`, `message`만 포함합니다.

조회는 commit된 마지막 관측을 읽습니다. Kafka 발행 직후 반영이나 장비 연결 상태를 보장하지 않고, 보고 중단을 충전 종료로 해석하지 않습니다. 데이터 갱신·발행·캐시·재시도를 추가하지 않습니다. control-plane의 운영자 API 중계는 다음 Feature입니다.

TLS 생성 스크립트는 기존 DB·Kafka 설정을 보존하면서 `.env`의 TLS 항목을 채우고, `secrets/`에 30일 유효한 localhost/127.0.0.1 개발용 인증서·PKCS12 키/신뢰 저장소를 만듭니다. 기존 TLS 자료가 있으면 덮어쓰지 않습니다. `.env`는 0600이며 인증 자료는 Git에서 제외합니다. 루트에서 JAR를 실행하면 상대 키 저장소 경로도 같은 루트를 기준으로 합니다. 다른 디렉터리에서는 `TELEMETRY_TLS_KEY_STORE`도 절대 file 경로로 지정합니다.

```sh
curl --cacert secrets/telemetry-worker.crt https://localhost:18443/internal/v1/stations/SIM001/telemetry/latest
```

worker에는 평문 우회 connector가 없으며 TLS 자료 없이 HTTPS 서버가 시작하지 않습니다. 테스트는 실행 중 인증서를 생성·정리하고 실제 PostgreSQL·HTTPS로 정렬/정밀도·400/404/503·commit 가시성·신뢰/호스트명 거부·평문 거부를 확인합니다. 로컬 서버는 loopback에만 바인딩합니다. 서비스/운영자 인증·권한·외부 접근 통제·운영 인증서 발급/갱신 정책은 외부 배포 전에 별도로 결정합니다.

### 운영자 최신 상태 조회

- 목록: `GET http://127.0.0.1:18580/api/v1/stations/{stationId}/telemetry/latest`.
- 단건: `GET http://127.0.0.1:18580/api/v1/stations/{stationId}/evses/{evseId}/telemetry/latest`.

`control-plane`은 `telemetry-worker`의 DB에 접속하지 않고 두 내부 경로를 HTTPS로 호출합니다. local profile은 `.env`의 기존 신뢰 저장소로 서버 인증서의 체인과 호스트명을 검증하며 전체 호출 deadline은 **로컬 시험용 1초**입니다. 자동 재시도·응답 캐시·평문 우회는 없습니다. 저장된 관측과 숫자·UTC 시각·eventId를 그대로 반환하고, 잘못된 입력과 worker의 정의된 400/404는 전달합니다. worker 연결/TLS/deadline/해석 불가 응답은 503/`TELEMETRY_QUERY_UNAVAILABLE`로 구분합니다. 두 API의 오류 JSON은 `code`, `message`만 포함하며 내부 오류 정보는 포함하지 않습니다. worker가 중단돼도 오래된 캐시를 정상 최신 상태로 반환하지 않습니다.

두 HTTP 서버는 현재 loopback 전용 개발용이며 운영자 인증·권한 정책이 없습니다. 외부 접근을 열기 전에 인증/접근 통제·운영 인증서 발급/갱신 정책을 별도로 결정해야 합니다. 기본 실행도 HTTPS origin·신뢰 자료·deadline 설정이 필요하고 누락·평문 origin은 시작 시 거부합니다. 다른 디렉터리에서 실행할 때는 `LOCAL_ENV_FILE`의 절대 경로와 `TELEMETRY_TLS_TRUST_STORE`의 절대 경로를 사용합니다.

```powershell
.\gradlew.bat :apps:mqtt-adapter:test :apps:telemetry-worker:test
.\gradlew.bat test
```

worker 테스트는 Docker가 실행 중이어야 하며 Testcontainers가 `postgres:18.3-alpine`·`apache/kafka:4.1.1`을 시작·종료합니다. Docker를 사용할 수 없을 때 실제 DB·broker 검증을 자동으로 건너뛰지 않습니다. macOS·Linux에서는 `./gradlew :apps:telemetry-worker:test`로 실행합니다.

최신 상태는 `(station_id, evse_id)`별 한 행입니다. 더 최신인 `occurredAt`, 이어 `receivedAt`만 반영하고 두 시각이 모두 같으면 기존 행을 유지합니다. 저장·비교 시각은 UTC microsecond로 절삭하며 원본 이벤트는 변경하지 않습니다. 재전달은 eventId·두 시각·payload를 보존한다는 전제이며, 이때 계측값·`last_event_id`·`updated_at`을 바꾸지 않습니다. 과거 입력도 기존 행을 유지합니다. 계측값은 BigDecimal과 PostgreSQL NUMERIC으로 저장합니다. 같은 이벤트의 재전달 처리와 MQTT 원천 중복 식별은 다르며, 이 저장 방식은 결제·누적 계산의 중복 실행 방지를 제공하지 않습니다.

`updated_at`은 반영 시 PostgreSQL의 `clock_timestamp()`로 기록합니다. 같은 트랜잭션의 연속 갱신도 각각의 처리 시각을 기록하며 무시한 입력은 변경하지 않습니다. 같은 eventId에 변경된 시각·내용을 넣은 사건의 거절 정책은 별도 계약 검토 대상으로 남깁니다.

저장 서비스는 Kafka listener와 연결됐으며 HTTP API는 후속입니다. 실제 PostgreSQL에서 신규·최신·중복·과거·동률·정밀도 왕복·동시 입력과 저장 직후 SQL 실패 rollback을 검증합니다. 저장소 작업은 [Issue #12](https://github.com/vvineey/ev-charging-orchestrator/issues/12)입니다.

[Issue #17](https://github.com/vvineey/ev-charging-orchestrator/issues/17)의 소비자는 group `telemetry-current-state-v1`, String key/value, auto commit false, earliest, RECORD, concurrency 1을 사용합니다. 신규 그룹·유효 offset이 없는 경우 보존된 처음 기록부터 읽고, 같은 그룹은 commit 위치부터 재시작합니다. 동기 listener가 별도 DB transaction 서비스의 commit 완료 뒤 반환하면 container가 offset을 기록합니다. 두 commit은 원자적이지 않습니다.

실제 Boot JSON mapper로 필수 필드 누락/null·JSON 자료형·타입/버전·key/시각을 검증합니다. 계약·DB 오류는 원본 JSON·key·SQL 예외 메시지를 보존하지 않는 안전한 예외와 위치·가능한 eventId·실패 시각으로 기록하고 `CommonContainerStoppingErrorHandler`가 container를 정지합니다. 실패 record는 skip/DLT 처리하지 않습니다. 잘못된 입력이 계속 남으면 소비도 계속 중단되며 수동 원인 조치가 필요합니다. 앞선 성공 record의 offset 기록은 허용합니다.

실제 Kafka/PostgreSQL 테스트는 실패 offset 유지·뒤 record 미처리·DB rollback·같은 그룹 재시작을 확인합니다. 테스트 전용 별도 JVM은 실제 listener의 DB commit 뒤 `halt(137)`로 종료하고, offset 미기록·재전달 후 값/last_event_id/updated_at 불변을 확인합니다. 이 강제 종료 코드는 테스트에만 있습니다. Kafka retention 밖의 원본 복원·새 조회 모델 재구축·원천 MQTT 중복 식별·이벤트별 처리 이력·결제 멱등성은 제공하지 않습니다.

Adapter는 Future 성공 완료 뒤 `eventId`, topic, partition, offset을 기록합니다. 오류는 `json_serialization`, `send`, `async` 단계와 예외·원인 타입으로 기록하고 원본 payload·record key·예외 메시지·stack trace를 발행 결과 로그에 넣지 않습니다. Kafka 또는 Java timeout을 포함한 원인 체인은 `outcome=unknown`으로 표시합니다. JSON 직렬화 예외의 기존 wrapping과 동기 send 예외 전파는 유지합니다.

기본 LoggingProducerListener는 Adapter의 no-op listener로 대체해 중복 오류·내용 출력을 막고 KafkaTemplate 로그 수준은 INFO로 둡니다. 명시적으로 KafkaTemplate DEBUG/TRACE를 켜면 라이브러리가 record 내용을 출력할 수 있습니다. 결과를 기다리는 `get`·`join`·`flush`와 애플리케이션 재발행은 추가하지 않았습니다. Kafka `send()` 자체의 metadata·buffer 대기는 기존 producer 설정에 따릅니다.

`outcome=completed`는 producer의 설정에 따른 Future 완료이며 consumer·DB 반영을 뜻하지 않습니다. 브로커 설정만 준 테스트 컨텍스트에서는 Kafka client의 기본 `acks=all`(정규화된 값 `-1`)을 확인했습니다. local profile은 `acks=all`과 idempotence를 명시하며 실제 producer 설정·partition 0/offset 0 완료와 Kafka 수신을 확인했습니다. 운영 환경의 override·ISR·topic 설정은 별도 검증 대상입니다. `acks=0`이면 완료해도 offset은 `-1`이고 브로커 수신 확인이 없습니다.

Adapter 테스트는 입력 계약·Future 완료 전/성공/실패·timeout·예외 전파·안전한 결과 로그·앱 설정을 검증합니다. 실제 KafkaTemplate과 MockProducer의 callback도 확인합니다. [Issue #14](https://github.com/vvineey/ev-charging-orchestrator/issues/14)의 로컬 환경에서는 MQTT→Kafka 한 건·앱 계정 Flyway·정상 재시작 후 스키마/기록 유지를 확인했습니다. #17에서는 두 앱의 local profile로 실제 MQTT→Kafka→DB 연결을 확인했습니다. broker 단절·MQTT 재연결·실장비·API·장시간 부하는 후속입니다. 발행 관찰 구현은 [Issue #8](https://github.com/vvineey/ev-charging-orchestrator/issues/8)입니다.

## 개발 단계

1. **구현:** 공통 telemetry 계약, 별도 MQTT 입력·Kafka 발행 결과 관찰, Kafka 소비·PostgreSQL 최신 상태 저장·내부 HTTPS 조회
2. **다음 구현:** 최소 OCPP 입력 계약과 상태·계측 매핑, 재구축 실험의 고정 입력·정답 정의
3. **후속 설계:** OCPP 거래 계약, 안정적인 원천 식별자, 세션·요금·결제 경계 결정
4. **핵심 실험:** 통신 단절 뒤 지연·중복·역순 거래 이벤트의 세션 복구와 중복 청구 방지
5. Kafka 독립 소비자, RabbitMQ AI 작업과 결과 반영 구현
6. Outbox·멱등 처리·명령 상태 머신, PG 대사와 장애 복구 검증
7. AWS Staging 부하·복구 측정 후 설계와 목표 재조정

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

국내 공공 통계에서 전체 충전기는 2026년 8월 말 기준 약 54만 기로 표시됩니다([무공해차 통합누리집 통계](https://www.ev.or.kr/nportal/stats/statsDashboard.do)). **2,000 충전기**는 전국 규모가 아니라 한 사업자급 검증 시나리오로 잡습니다. 실제 해당 사업자의 이용률·발행 주기는 확보하지 못했으므로 다음 비율은 모두 실험 가정입니다.

- 기준: 충전기 2,000기, 동시 충전 30%, 활성 포트 60초 계량 주기 → 약 10건/초. 20,000건은 이 기준에서 약 33분 분량이며 성능 목표 자체가 아닙니다.
- 확장 스트레스: **2,000개 충전소 × 5포트 = 10,000포트**라는 별도 가정. 20% 활성 포트를 15초마다 계량하면 약 133건/초이고, 충전소당 60초 Heartbeat를 더하면 약 167건/초입니다. 200건/초는 여유를 둔 시험 부하입니다.
- 순간 1,000건/초, WebSocket 2,000개, 6시간 지속 부하는 E0 목표이며 측정 결과가 아닙니다.
- 충전소 재접속 폭주
- 중복·역순 이벤트
- Kafka 소비자 중단과 offset 재처리
- RabbitMQ Worker 종료와 메시지 재전달
- DB 또는 외부 AI 서버 지연

기록 항목은 p50/p95/p99 지연, 처리량, 오류율, consumer lag, DB 자원, backlog 복구 시간, 중복·유실 검증 결과입니다. 측정 결과가 없는 수치는 성과로 표현하지 않습니다.
