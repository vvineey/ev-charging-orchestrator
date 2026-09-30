package com.evcharging.ocppgateway;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/** Ephemeral localhost certificate with an explicit client trust root. */
final class TestTlsMaterial implements AutoCloseable {
    final Path directory;
    final Path keyStore;
    final Path certificate;
    final String password = UUID.randomUUID().toString();
    final SSLContext trustedContext;

    TestTlsMaterial() {
        try {
            directory = Files.createTempDirectory("ocpp-gateway-tls-");
            keyStore = directory.resolve("server.p12");
            certificate = directory.resolve("server.crt");
            var command = new java.util.ArrayList<>(List.of("-genkeypair", "-alias", "ocpp-gateway",
                    "-keyalg", "RSA", "-keysize", "2048", "-sigalg", "SHA256withRSA",
                    "-dname", "CN=localhost", "-ext", "SAN=dns:localhost", "-validity", "1",
                    "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                    "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-keypass:env", "EVC_TEST_TLS_PASSWORD"));
            command.addFirst(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
            var builder = new ProcessBuilder(command).redirectErrorStream(true)
                    .redirectOutput(directory.resolve("keytool.log").toFile());
            builder.environment().put("EVC_TEST_TLS_PASSWORD", password);
            var process = builder.start();
            if (!process.waitFor(20, TimeUnit.SECONDS) || process.exitValue() != 0) {
                process.destroyForcibly();
                throw new IllegalStateException("TLS test keytool failed");
            }
            var export = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "keytool")
                    .toString(), "-exportcert", "-rfc", "-alias", "ocpp-gateway", "-keystore",
                    keyStore.toString(), "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-file",
                    certificate.toString()).redirectErrorStream(true)
                    .redirectOutput(directory.resolve("keytool-export.log").toFile());
            export.environment().put("EVC_TEST_TLS_PASSWORD", password);
            var exportProcess = export.start();
            if (!exportProcess.waitFor(20, TimeUnit.SECONDS) || exportProcess.exitValue() != 0) {
                exportProcess.destroyForcibly();
                throw new IllegalStateException("TLS test certificate export failed");
            }
            var serverStore = KeyStore.getInstance("PKCS12");
            try (var input = Files.newInputStream(keyStore)) {
                serverStore.load(input, password.toCharArray());
            }
            var trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("ocpp-gateway", serverStore.getCertificate("ocpp-gateway"));
            var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trustStore);
            trustedContext = SSLContext.getInstance("TLS");
            trustedContext.init(null, trustManagers.getTrustManagers(), null);
        } catch (Exception exception) {
            throw new IllegalStateException("Could not create temporary TLS test material", exception);
        }
    }

    @Override
    public void close() throws IOException {
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
