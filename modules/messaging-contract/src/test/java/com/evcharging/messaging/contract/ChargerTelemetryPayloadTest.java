package com.evcharging.messaging.contract;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;

class ChargerTelemetryPayloadTest {

    @Test
    void rejectsNegativeTimestamp() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new ChargerTelemetryPayload(
                        -1L,
                        false,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO
                ))
                .withMessage("timestamp must not be negative");
    }

    @Test
    void rejectsMissingMeasurement() {
        assertThatNullPointerException()
                .isThrownBy(() -> new ChargerTelemetryPayload(
                        1L,
                        false,
                        null,
                        BigDecimal.ZERO,
                        BigDecimal.ZERO
                ))
                .withMessage("power must not be null");
    }
}
