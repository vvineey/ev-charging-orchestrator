package com.evcharging.reconciliationtest;

import com.evcharging.controlplane.ControlPlaneApplication;
import com.evcharging.controlplane.reconciliation.ReconciliationAccessPolicy;
import com.evcharging.controlplane.reconciliation.ReconciliationCertificateFilter;
import jakarta.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ReconciliationMtlsIntegrationTest {
    private static final String PATH = "/internal/v1/reconciliation/authorization-probe";
    private static final String GOOD = "{\"issuerId\":\"fixture-issuer\",\"stationId\":\"ST-1\",\"action\":\"SUBMIT_EVIDENCE\"}";
    private static TestCertificates certificates;
    private static ConfigurableApplicationContext app;
    private static int port;
    private static HttpClient allowed;
    private static HttpClient rotated;
    private static HttpClient otherTrusted;
    private static HttpClient untrusted;
    private static HttpClient ambiguous;
    private static HttpClient noCertificate;

    @BeforeAll
    static void start() throws Exception {
        certificates = new TestCertificates();
        app = startApp(true, ProbeController.class);
        port = ((ServletWebServerApplicationContext) app).getWebServer().getPort();
        allowed = certificates.client(certificates.allowedStore);
        rotated = certificates.client(certificates.rotatedStore);
        otherTrusted = certificates.client(certificates.otherStore);
        untrusted = certificates.client(certificates.untrustedStore);
        ambiguous = certificates.client(certificates.ambiguousStore);
        noCertificate = certificates.client(null);
    }

    @AfterAll
    static void stop() throws Exception {
        for (HttpClient client : new HttpClient[] {allowed, rotated, otherTrusted, untrusted, ambiguous, noCertificate}) {
            if (client != null) client.close();
        }
        if (app != null) app.close();
        if (certificates != null) certificates.close();
    }

    @Test
    void permitsOnlyTrustedServiceWithinExplicitIssuerStationAndAction() throws Exception {
        assertThat(post(allowed, GOOD).statusCode()).isEqualTo(204);
        assertThat(post(allowed, GOOD.replace("fixture-issuer", "another-issuer")).statusCode()).isEqualTo(403);
        assertThat(post(allowed, GOOD.replace("ST-1", "ST-2")).statusCode()).isEqualTo(403);
        assertThat(post(allowed, GOOD.replace("SUBMIT_EVIDENCE", "REQUEST_CLOSEOUT")).statusCode()).isEqualTo(403);
    }

    @Test
    void rejectsMissingAndUnregisteredClientCertificates() throws Exception {
        assertThat(post(noCertificate, GOOD).statusCode()).isEqualTo(401);
        assertThat(post(otherTrusted, GOOD).statusCode()).isEqualTo(401);
        assertThat(post(ambiguous, GOOD).statusCode()).isEqualTo(401);
        // With client-auth=want, a certificate outside the server trust store may be omitted by the client.
        // The request must still be rejected by the protected route.
        assertThat(post(untrusted, GOOD).statusCode()).isEqualTo(401);
    }

    @Test
    void replacingTrustStoreAcceptsRotatedCredentialAndRejectsOldCredential() throws Exception {
        // Both credentials are valid while the trust store contains both certificates.
        assertThat(post(allowed, GOOD).statusCode()).isEqualTo(204);
        assertThat(post(rotated, GOOD).statusCode()).isEqualTo(204);

        // A restarted server that trusts only the replacement must not accept the old certificate.
        try (ConfigurableApplicationContext replacement = startApp(
                true, ProbeController.class, true, certificates.rotatedOnlyTrustStore)) {
            int replacementPort = ((ServletWebServerApplicationContext) replacement).getWebServer().getPort();
            assertThat(post(rotated, GOOD, replacementPort).statusCode()).isEqualTo(204);
            assertThat(post(allowed, GOOD, replacementPort).statusCode()).isEqualTo(401);
        }
    }

    @Test
    void disabledConfigurationDeniesTheReservedPath() {
        try (ConfigurableApplicationContext disabled = startApp(false, ControlPlaneApplication.class, false)) {
            int disabledPort = ((ServletWebServerApplicationContext) disabled).getWebServer().getPort();
            try (HttpClient client = HttpClient.newHttpClient()) {
                var request = HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + disabledPort + PATH))
                        .POST(HttpRequest.BodyPublishers.ofString(GOOD)).build();
                assertThat(client.send(request, HttpResponse.BodyHandlers.discarding()).statusCode()).isEqualTo(403);
            }
        } catch (Exception exception) {
            throw new AssertionError(exception);
        }
    }

    @Test
    void enabledWithoutTlsCannotStart() {
        assertThatThrownBy(() -> startApp(true, ControlPlaneApplication.class, false))
                .hasRootCauseInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Reconciliation authentication requires HTTPS");
    }

    private static HttpResponse<Void> post(HttpClient client, String body) throws Exception {
        return post(client, body, port);
    }

    private static HttpResponse<Void> post(HttpClient client, String body, int targetPort) throws Exception {
        var request = HttpRequest.newBuilder(URI.create("https://localhost:" + targetPort + PATH))
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(5))
                .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        return client.send(request, HttpResponse.BodyHandlers.discarding());
    }

    private static ConfigurableApplicationContext startApp(boolean enabled, Class<?> source) {
        return startApp(enabled, source, true);
    }

    private static ConfigurableApplicationContext startApp(boolean enabled, Class<?> source, boolean tls) {
        return startApp(enabled, source, tls, certificates.clientTrustStore);
    }

    private static ConfigurableApplicationContext startApp(boolean enabled, Class<?> source, boolean tls,
                                                            Path clientTrustStore) {
        var application = new SpringApplication(ControlPlaneApplication.class, source);
        Map<String, Object> properties = new java.util.HashMap<>(Map.of(
                "server.port", "0",
                "telemetry.worker.base-url", "https://localhost:9",
                "telemetry.worker.trust-store", certificates.serverTrustStore.toString(),
                "telemetry.worker.trust-store-password", certificates.password,
                "telemetry.worker.deadline", "1s",
                "reconciliation.auth.enabled", Boolean.toString(enabled),
                "reconciliation.auth.client-uri", "urn:ev-charging:reconciliation:fixture",
                "reconciliation.auth.issuer-id", "fixture-issuer",
                "reconciliation.auth.station-ids[0]", "ST-1",
                "reconciliation.auth.actions[0]", "SUBMIT_EVIDENCE"));
        if (tls) {
            properties.putAll(Map.of(
                    "server.ssl.enabled", "true",
                    "server.ssl.client-auth", "want",
                    "server.ssl.key-store", certificates.serverStore.toString(),
                    "server.ssl.key-store-password", certificates.password,
                    "server.ssl.key-store-type", "PKCS12",
                    "server.ssl.trust-store", clientTrustStore.toString(),
                    "server.ssl.trust-store-password", certificates.password,
                    "server.ssl.trust-store-type", "PKCS12"));
        }
        return application.run(properties.entrySet().stream()
                .map(entry -> "--" + entry.getKey() + "=" + entry.getValue())
                .toArray(String[]::new));
    }

    @RestController
    @RequestMapping("/internal/v1/reconciliation")
    public static class ProbeController {
        private final ReconciliationAccessPolicy policy;

        public ProbeController(ReconciliationAccessPolicy policy) {
            this.policy = policy;
        }

        @PostMapping("/authorization-probe")
        public org.springframework.http.ResponseEntity<Void> authorize(@RequestBody ProbeRequest request, HttpServletRequest httpRequest) {
            ReconciliationAccessPolicy.Action action;
            try {
                action = ReconciliationAccessPolicy.Action.valueOf(request.action());
            } catch (IllegalArgumentException exception) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN);
            }
            String clientUri = (String) httpRequest.getAttribute(ReconciliationCertificateFilter.CLIENT_URI_ATTRIBUTE);
            if (!policy.allows(clientUri, request.issuerId(), request.stationId(), action)) {
                throw new ResponseStatusException(HttpStatus.FORBIDDEN);
            }
            return org.springframework.http.ResponseEntity.noContent().build();
        }
    }

    public record ProbeRequest(String issuerId, String stationId, String action) {}

    private static final class TestCertificates implements AutoCloseable {
        private final Path directory;
        private final String password = UUID.randomUUID().toString();
        private final Path serverStore;
        private final Path allowedStore;
        private final Path rotatedStore;
        private final Path otherStore;
        private final Path untrustedStore;
        private final Path ambiguousStore;
        private final Path serverTrustStore;
        private final Path clientTrustStore;
        private final Path rotatedOnlyTrustStore;

        private TestCertificates() throws Exception {
            directory = Files.createTempDirectory("reconciliation-mtls-");
            serverStore = keypair("server", "CN=localhost", "SAN=dns:localhost");
            allowedStore = keypair("allowed", "CN=fixture", "SAN=uri:urn:ev-charging:reconciliation:fixture");
            rotatedStore = keypair("rotated", "CN=fixture-rotated", "SAN=uri:urn:ev-charging:reconciliation:fixture");
            otherStore = keypair("other", "CN=other", "SAN=uri:urn:ev-charging:reconciliation:other");
            untrustedStore = keypair("untrusted", "CN=untrusted", "SAN=uri:urn:ev-charging:reconciliation:fixture");
            ambiguousStore = keypair("ambiguous", "CN=ambiguous", "SAN=uri:urn:ev-charging:reconciliation:fixture,uri:urn:ev-charging:reconciliation:other");
            serverTrustStore = trustStore("server-trust", serverStore);
            clientTrustStore = trustStore("client-trust", allowedStore, rotatedStore, otherStore, ambiguousStore);
            rotatedOnlyTrustStore = trustStore("rotated-only-trust", rotatedStore);
        }

        private Path keypair(String alias, String dname, String extension) throws Exception {
            Path target = directory.resolve(alias + ".p12");
            var command = new java.util.ArrayList<>(List.of(
                    "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048",
                    "-sigalg", "SHA256withRSA", "-dname", dname, "-ext", extension,
                    "-validity", "1", "-storetype", "PKCS12", "-keystore", target.toString(),
                    "-storepass:env", "EVC_RECON_TEST_PASSWORD", "-keypass:env", "EVC_RECON_TEST_PASSWORD"));
            command.addFirst(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
            var builder = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(directory.resolve(alias + "-keytool.log").toFile());
            builder.environment().put("EVC_RECON_TEST_PASSWORD", password);
            var process = builder.start();
            if (!process.waitFor(20, TimeUnit.SECONDS)) {
                process.destroyForcibly();
                throw new IllegalStateException("Test keytool timed out");
            }
            if (process.exitValue() != 0) throw new IllegalStateException("Test keytool failed for " + alias);
            return target;
        }

        private Path trustStore(String name, Path... sources) throws Exception {
            var trust = KeyStore.getInstance("PKCS12");
            trust.load(null, password.toCharArray());
            for (Path source : sources) {
                var store = load(source);
                String alias = store.aliases().nextElement();
                trust.setCertificateEntry(alias, store.getCertificate(alias));
            }
            Path target = directory.resolve(name + ".p12");
            try (var output = Files.newOutputStream(target)) {
                trust.store(output, password.toCharArray());
            }
            return target;
        }

        private KeyStore load(Path path) throws Exception {
            var store = KeyStore.getInstance("PKCS12");
            try (var input = Files.newInputStream(path)) {
                store.load(input, password.toCharArray());
            }
            return store;
        }

        private HttpClient client(Path keyStore) throws Exception {
            var keys = keyStore == null ? null : KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            if (keys != null) keys.init(load(keyStore), password.toCharArray());
            var trust = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(load(serverTrustStore));
            var tls = SSLContext.getInstance("TLS");
            tls.init(keys == null ? null : keys.getKeyManagers(), trust.getTrustManagers(), null);
            return HttpClient.newBuilder().sslContext(tls).connectTimeout(Duration.ofSeconds(5)).build();
        }

        @Override
        public void close() throws Exception {
            try (var paths = Files.walk(directory)) {
                for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) Files.deleteIfExists(path);
            }
        }
    }
}
