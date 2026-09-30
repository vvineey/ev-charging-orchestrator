package com.evcharging.ocppgateway;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TransactionEventMapperTest {
    private final ObjectMapper json = new ObjectMapper();
    private final TransactionEventMapper mapper = new TransactionEventMapper(json,
            Clock.fixed(Instant.parse("2026-09-30T00:00:01Z"), ZoneOffset.UTC));
    private final StationRegistration station = new StationRegistration("CP-1", "ST-1", Set.of(1, 2), "test-credential-only");

    @Test
    void preservesAllMeterGroupsAndMissingEvseWithoutChoosingBillableEnergy() {
        var source = json.readTree("""
                {
                  "eventType":"Ended","timestamp":"2026-09-30T00:00:00Z","triggerReason":"EVDisconnected",
                  "seqNo":2,"transactionInfo":{"transactionId":"TX-1","stoppedReason":"EVDisconnected"},
                  "meterValue":[
                    {"timestamp":"2026-09-30T00:00:00Z","sampledValue":[
                      {"value":2500,"measurand":"Energy.Active.Import.Register","context":"Transaction.End","unitOfMeasure":{"unit":"Wh","multiplier":0}},
                      {"value":2.5,"measurand":"Energy.Active.Import.Register","context":"Sample.Periodic","unitOfMeasure":{"unit":"kWh"}}]},
                    {"timestamp":"2026-09-30T00:00:00Z","sampledValue":[{"value":2500,"measurand":"Energy.Active.Import.Register"}]}
                  ],"idToken":{"idToken":"private-token","type":"Central"}
                }
                """);

        var record = mapper.map(source, station);
        var result = record.json();
        assertThat(record.stationId()).isEqualTo("ST-1");
        assertThat(result.path("eventType").textValue()).isEqualTo("OcppTransactionEventObserved");
        assertThat(result.path("schemaVersion").intValue()).isEqualTo(1);
        assertThat(result.path("occurredAt").textValue()).isEqualTo("2026-09-30T00:00:00Z");
        assertThat(result.path("receivedAt").textValue()).isEqualTo("2026-09-30T00:00:01Z");
        assertThat(result.path("payload").path("seqNo").intValue()).isEqualTo(2);
        assertThat(result.path("payload").path("transactionId").textValue()).isEqualTo("TX-1");
        assertThat(result.path("payload").has("evse")).isFalse();
        assertThat(result.path("payload").path("meterValue")).isEqualTo(source.path("meterValue"));
        assertThat(json.writeValueAsString(result)).doesNotContain("private-token", "idToken", "billableEnergy");
    }

    @Test
    void acceptsShortTransactionWithoutMeterValuesAndRejectsUnregisteredEvse() {
        var ended = json.readTree("""
                {"eventType":"Ended","timestamp":"2026-09-30T00:00:00Z","triggerReason":"EVDisconnected",
                "seqNo":1,"transactionInfo":{"transactionId":"TX-2"}}
                """);
        assertThat(mapper.map(ended, station).json().path("payload").has("meterValue")).isFalse();
        var invalid = json.readTree("""
                {"eventType":"Started","timestamp":"2026-09-30T00:00:00Z","triggerReason":"CablePluggedIn",
                "seqNo":0,"transactionInfo":{"transactionId":"TX-3"},"evse":{"id":3}}
                """);
        assertThatThrownBy(() -> mapper.map(invalid, station))
                .isInstanceOf(TransactionEventMapper.InvalidTransactionEvent.class);
    }

    @Test
    void refusesUnknownSourceFieldsRatherThanSilentlyDroppingThem() {
        var source = json.readTree("""
                {"eventType":"Started","timestamp":"2026-09-30T00:00:00Z","triggerReason":"CablePluggedIn",
                "seqNo":0,"transactionInfo":{"transactionId":"TX-4"},"customData":{"vendorId":"x"}}
                """);
        assertThatThrownBy(() -> mapper.map(source, station))
                .isInstanceOf(TransactionEventMapper.InvalidTransactionEvent.class);
    }
}
