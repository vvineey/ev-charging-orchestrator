package com.evcharging.mqttadapter;

import org.eclipse.paho.client.mqttv3.IMqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.util.ReflectionTestUtils;
import tools.jackson.databind.ObjectMapper;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@SpringBootTest(classes = MqttAdapterApplication.class, properties = {
        "mqtt.url=tcp://localhost:1883",
        "mqtt.topic=charger/telemetry",
        "mqtt.client-id=explicit-adapter-client",
        "spring.kafka.bootstrap-servers=localhost:9092",
        "telemetry.kafka-topic=charger.telemetry"
})
@Import(MqttTelemetryFlowTest.BrokerlessMqttInput.class)
class MqttTelemetryFlowTest {

    @Autowired
    private MqttPahoMessageDrivenChannelAdapter inbound;

    @Autowired
    private MqttPahoClientFactory clientFactory;

    @Autowired
    private ObjectMapper objectMapper;

    @MockitoBean
    private KafkaTemplate<String, String> kafkaTemplate;

    @Test
    void startsWithExplicitClientIdAndExistingConnectionOptions() {
        assertThat(inbound.isAutoStartup()).isFalse();
        assertThat(inbound.isRunning()).isFalse();
        assertThat(inbound.getTopic()).containsExactly("charger/telemetry");
        assertThat(ReflectionTestUtils.getField(inbound, "clientId"))
                .isEqualTo("explicit-adapter-client");
        assertThat(clientFactory.getConnectionOptions().isAutomaticReconnect()).isTrue();
        assertThat(clientFactory.getConnectionOptions().isCleanSession()).isTrue();
    }

    @Test
    void routesInboundPayloadThroughMapperAndKafkaPublisher() throws Exception {
        when(kafkaTemplate.send(eq("charger.telemetry"), eq("test-station"), org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(new CompletableFuture<>());
        String payload = """
                {"stationId":"test-station","chargerNo":5,"charging":true,
                 "power":120.0,"timestamp":1704067200000,"voltage":24.0,"current":5.0}
                """;

        inbound.messageArrived("charger/telemetry", message(payload));

        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        verify(kafkaTemplate).send(eq("charger.telemetry"), eq("test-station"), json.capture());
        var event = objectMapper.readTree(json.getValue());
        assertThat(event.get("eventType").asString()).isEqualTo("ChargerTelemetryReceived");
        assertThat(event.get("schemaVersion").asInt()).isEqualTo(1);
        assertThat(event.get("stationId").asString()).isEqualTo("test-station");
        assertThat(event.get("evseId").asInt()).isEqualTo(5);
        assertThat(event.get("payload").get("timestamp").asLong()).isEqualTo(1704067200000L);
        assertThat(event.get("payload").get("charging").asBoolean()).isTrue();
        assertThat(event.get("payload").get("power").decimalValue()).isEqualByComparingTo("120.0");
        assertThat(event.get("payload").get("voltage").decimalValue()).isEqualByComparingTo("24.0");
        assertThat(event.get("payload").get("current").decimalValue()).isEqualByComparingTo("5.0");
    }

    @Test
    void rejectsInvalidInboundPayloadBeforeKafkaPublication() {
        assertThatThrownBy(() -> inbound.messageArrived("charger/telemetry", message("not-json")))
                .hasCauseInstanceOf(IllegalArgumentException.class);
        verifyNoInteractions(kafkaTemplate);
    }

    private static MqttMessage message(String payload) {
        return new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
    }

    @TestConfiguration(proxyBeanMethods = false)
    static class BrokerlessMqttInput {

        @Bean
        static BeanPostProcessor disableMqttConnection() {
            return new BeanPostProcessor() {
                @Override
                public Object postProcessBeforeInitialization(Object bean, String beanName) {
                    if (bean instanceof MqttPahoMessageDrivenChannelAdapter adapter) {
                        adapter.setAutoStartup(false);
                        // No client is created without startup; provide one for brokerless cleanup.
                        ReflectionTestUtils.setField(adapter, "client", mock(IMqttAsyncClient.class));
                    }
                    return bean;
                }
            };
        }
    }
}
