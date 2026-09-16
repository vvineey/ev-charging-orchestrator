package com.evcharging.telemetryworker.adapter.mqtt;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "telemetry")
public record TelemetryWorkerProperties(String kafkaTopic) {
}
