# Raspberry Pi Edge 코드 분석

## 분석 범위

`IoT ) 라즈베리파이 코드 .docx`에 포함된 C 코드 블록을 정적 분석하고 렌더링 결과를 대조했다. 실행 로그나 실제 배선도는 제공되지 않았으므로 아래 구현 사실과 하드웨어 사실을 구분한다.

## 확인된 구현 사실 E1

- C 언어, WiringPi, WiringSerial, Mosquitto C client를 사용한다.
- `/dev/ttyUSB0`, `/dev/ttyUSB1`을 각각 2400 baud로 연다.
- `getLoad2()`와 `getLoad3()`가 각 버스의 전압·전류 배열을 채운다.
- 최종 하드웨어 매핑은 LOAD3→포트 1·2·3, LOAD2→포트 4·5다.
- `STATION_ID`는 `EV001`로 고정되어 있다.
- MQTT keepalive는 30초, QoS는 1, retain은 false다.
- 토픽 형식은 `iot.data/{stationId}/{chargerNo}/telemetry`다.
- 메인 루프는 측정·발행 후 3초 대기한다.
- JSON은 포트별로 한 건씩 발행한다.
- 별도 `p4-c.c`는 파일에서 10회 읽고 인자로 받은 종료 코드로 끝나는 프로세스 종료 테스트 코드다.

## 확정된 JSON 계약

문서에는 과거 형식과 수정 형식이 함께 존재하지만, 프로젝트의 공식 계약은 다음 형식으로 확정한다.

```json
{
  "stationId": "EV001",
  "chargerNo": 1,
  "charging": true,
  "power": 120,
  "timestamp": 1720000000000,
  "voltage": 24.00,
  "current": 5.00
}
```

필드명과 의미는 확정했지만, 최종 실행 파일의 실제 stdout/MQTT payload 캡처로 단위와 정밀도를 검증한다. 기존 구현의 `time(NULL) * 1000`은 필드명은 밀리초지만 실제 정밀도는 초 단위라는 점을 별도 검증 대상으로 둔다.

## 포트 상태 변화

사용자 확인에 따라 포트 ON/OFF 상태 변화는 USB 삽입·분리 이벤트로 인식한다. 현재 제공 코드에는 서버 원격 명령이 릴레이를 직접 구동하는 경로가 보이지 않으므로, 원격 제어 가능 여부와 명령 프레임은 별도 확인이 필요하다.

## 보안 관찰 E1

- 과거 테스트 코드에 MQTT 호스트, 포트, 사용자명, 비밀번호가 하드코딩되어 있다.
- 사용자 확인에 따라 해당 계정과 비밀번호는 현재 사용하지 않는 과거 자격 증명이며, 이 프로젝트에서 발생한 활성 보안 사고로 분류하지 않는다.
- 그래도 자격 증명은 저장소에 복사하지 않고, 새 구현에서는 환경 변수 또는 AWS Secrets Manager와 TLS를 사용한다.

## 복구 관찰 E1

MQTT 연결 실패와 Serial open 실패 시 즉시 프로세스를 종료한다. 자동 재연결, 지수 백오프, 오프라인 버퍼, 마지막 정상 측정값 정책은 코드에서 확인되지 않았다. 이는 아직 장애가 아니라 다음 E4 실험 대상이다.

## 백엔드 전환 계획

현재 물리 장치 연동은 MQTT adapter로 보존하고, 도메인 이벤트 계약을 분리한다.

```text
Raspberry Pi MQTT
    → Edge/MQTT Adapter
    → protocol-agnostic ChargerTelemetryReceived
    → Kafka

후속 단계
Raspberry Pi OCPP 2.0.1/WSS
    → OCPP Gateway
    → 동일한 도메인 이벤트 계약
```

이렇게 하면 하드웨어 검증을 중단하지 않고 OCPP 전환을 단계적으로 검증할 수 있다.
