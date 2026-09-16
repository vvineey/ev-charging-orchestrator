package com.evcharging.messaging.contract;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

class EvseIdTest {

    @Test
    void acceptsPositiveValue() {
        assertThat(new EvseId(1).value()).isEqualTo(1);
    }

    @Test
    void rejectsZero() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvseId(0))
                .withMessage("evseId must be positive");
    }

    @Test
    void rejectsNegativeValue() {
        assertThatIllegalArgumentException()
                .isThrownBy(() -> new EvseId(-1))
                .withMessage("evseId must be positive");
    }
}
