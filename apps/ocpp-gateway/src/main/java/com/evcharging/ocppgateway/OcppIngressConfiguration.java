package com.evcharging.ocppgateway;

import jakarta.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.config.annotation.EnableWebSocket;
import org.springframework.web.socket.config.annotation.WebSocketConfigurer;
import org.springframework.web.socket.config.annotation.WebSocketHandlerRegistry;
import org.springframework.web.socket.server.HandshakeInterceptor;
import org.springframework.web.socket.server.support.DefaultHandshakeHandler;
import org.springframework.web.socket.server.standard.ServletServerContainerFactoryBean;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Configuration(proxyBeanMethods = false)
@EnableWebSocket
@ConditionalOnProperty(name = "ocpp.ingress.enabled", havingValue = "true")
class OcppIngressConfiguration implements WebSocketConfigurer {

    private final WebSocketHandler handler;
    private final StationRegistration station;

    OcppIngressConfiguration(WebSocketHandler handler, StationRegistration station, Environment environment) {
        this.handler = handler;
        this.station = station;
        if (!environment.getProperty("server.ssl.enabled", Boolean.class, false)
                || environment.getProperty("server.port", Integer.class, -1) < 0) {
            throw new IllegalStateException("OCPP ingress requires a TLS listener port");
        }
    }

    @Bean
    static StationRegistration stationRegistration(
            @Value("${ocpp.station.charging-station-id}") String chargingStationId,
            @Value("${ocpp.station.station-id}") String stationId,
            @Value("${ocpp.station.allowed-evse-ids}") String allowedEvseIds,
            @Value("${ocpp.station.password}") String password
    ) {
        Set<Integer> evseIds = Arrays.stream(allowedEvseIds.split(","))
                .map(String::trim).map(Integer::parseInt).collect(Collectors.toUnmodifiableSet());
        return new StationRegistration(chargingStationId, stationId, evseIds, password);
    }

    @Bean
    ServletServerContainerFactoryBean ocppWebSocketContainer() {
        ServletServerContainerFactoryBean container = new ServletServerContainerFactoryBean();
        container.setMaxTextMessageBufferSize(1024 * 1024);
        return container;
    }

    @Override
    public void registerWebSocketHandlers(WebSocketHandlerRegistry registry) {
        DefaultHandshakeHandler handshakeHandler = new DefaultHandshakeHandler();
        handshakeHandler.setSupportedProtocols("ocpp2.0.1");
        registry.addHandler(handler, "/ocpp/*")
                .addInterceptors(new StationHandshakeInterceptor(station))
                .setHandshakeHandler(handshakeHandler);
    }

    private static final class StationHandshakeInterceptor implements HandshakeInterceptor {
        private static final Logger log = LoggerFactory.getLogger(StationHandshakeInterceptor.class);
        private final StationRegistration station;

        private StationHandshakeInterceptor(StationRegistration station) {
            this.station = station;
        }

        @Override
        public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response,
                WebSocketHandler wsHandler, Map<String, Object> attributes) {
            if (!(request instanceof ServletServerHttpRequest servletRequest)
                    || !servletRequest.getServletRequest().isSecure()) {
                return reject(response, HttpStatus.FORBIDDEN, "insecure_transport");
            }
            HttpServletRequest servlet = servletRequest.getServletRequest();
            String path = servlet.getRequestURI();
            String expectedPath = "/ocpp/" + station.chargingStationId();
            String authorization = request.getHeaders().getFirst("Authorization");
            String protocols = request.getHeaders().getFirst("Sec-WebSocket-Protocol");
            boolean ocppProtocolRequested = protocols != null && Arrays.stream(protocols.split(","))
                    .map(String::trim).anyMatch("ocpp2.0.1"::equals);
            if (!expectedPath.equals(path) || !ocppProtocolRequested
                    || authorization == null || !authorization.startsWith("Basic ")) {
                return reject(response, HttpStatus.UNAUTHORIZED, "identity_or_protocol");
            }
            String credential;
            try {
                credential = new String(Base64.getDecoder().decode(authorization.substring(6)),
                        StandardCharsets.UTF_8);
            } catch (IllegalArgumentException exception) {
                return reject(response, HttpStatus.UNAUTHORIZED, "invalid_credentials");
            }
            int separator = credential.indexOf(':');
            if (separator <= 0 || !station.chargingStationId().equals(credential.substring(0, separator))
                    || !MessageDigest.isEqual(
                            station.password().getBytes(StandardCharsets.UTF_8),
                            credential.substring(separator + 1).getBytes(StandardCharsets.UTF_8))) {
                return reject(response, HttpStatus.UNAUTHORIZED, "invalid_credentials");
            }
            attributes.put("station", station);
            return true;
        }

        private boolean reject(ServerHttpResponse response, HttpStatus status, String reason) {
            log.warn("ocpp_handshake_rejected reason={}", reason);
            response.setStatusCode(status);
            return false;
        }

        @Override
        public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response,
                WebSocketHandler wsHandler, Exception exception) {
        }
    }
}
