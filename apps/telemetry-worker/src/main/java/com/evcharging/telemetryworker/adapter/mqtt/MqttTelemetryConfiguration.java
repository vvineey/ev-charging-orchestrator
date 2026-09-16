package com.evcharging.telemetryworker.adapter.mqtt;

import com.evcharging.messaging.contract.ChargerTelemetryPayload;
import com.evcharging.messaging.contract.DomainEventEnvelope;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageHeaders;

@Configuration
@EnableConfigurationProperties(TelemetryWorkerProperties.class)
@ConditionalOnProperty(
        name = {"mqtt.url", "mqtt.topic", "spring.kafka.bootstrap-servers", "telemetry.kafka-topic"}
)
public class MqttTelemetryConfiguration {

    @Bean
    MqttPahoClientFactory mqttClientFactory() {
        MqttConnectOptions options = new MqttConnectOptions();
        options.setAutomaticReconnect(true);
        options.setCleanSession(true);

        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    IntegrationFlow mqttTelemetryFlow(
            MqttPahoClientFactory clientFactory,
            MqttTelemetryMapper mapper,
            TelemetryEventPublisher publisher,
            @Value("${mqtt.url}") String url,
            @Value("${mqtt.client-id:telemetry-worker}") String clientId,
            @Value("${mqtt.topic}") String topic
    ) {
        MqttPahoMessageDrivenChannelAdapter inbound = new MqttPahoMessageDrivenChannelAdapter(
                url,
                clientId,
                clientFactory,
                topic
        );
        DefaultPahoMessageConverter messageConverter = new DefaultPahoMessageConverter();
        messageConverter.setPayloadAsBytes(false);
        inbound.setConverter(messageConverter);

        return IntegrationFlow.from(inbound)
                .transform(String.class, mapper::toEvent)
                .handle((event, headers) -> {
                    publish(publisher, event, headers);
                    return null;
                })
                .get();
    }

    private static void publish(
            TelemetryEventPublisher publisher,
            Object event,
            MessageHeaders headers
    ) {
        if (!(event instanceof DomainEventEnvelope<?> envelope)
                || !(envelope.payload() instanceof ChargerTelemetryPayload)) {
            throw new IllegalArgumentException("unexpected MQTT telemetry event type");
        }

        @SuppressWarnings("unchecked")
        DomainEventEnvelope<ChargerTelemetryPayload> telemetryEvent =
                (DomainEventEnvelope<ChargerTelemetryPayload>) envelope;
        publisher.publish(telemetryEvent);
    }
}
