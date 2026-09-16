package com.evcharging.messaging.contract;

public record EvseId(int value) {

    public EvseId {
        if (value <= 0) {
            throw new IllegalArgumentException("evseId must be positive");
        }
    }
}
