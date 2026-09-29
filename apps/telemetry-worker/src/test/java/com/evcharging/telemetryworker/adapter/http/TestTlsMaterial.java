package com.evcharging.telemetryworker.adapter.http;

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

/** Never checked in: a localhost-only certificate and an explicit client trust store. */
final class TestTlsMaterial implements AutoCloseable {
    final Path directory;
    final Path keyStore;
    final String password = UUID.randomUUID().toString();
    final SSLContext trustedContext;

    TestTlsMaterial() {
        Path generated = null;
        try {
            generated = Files.createTempDirectory("telemetry-query-tls-");
            directory = generated;
            keyStore = directory.resolve("server.p12");
            keytool("-genkeypair", "-alias", "telemetry-worker", "-keyalg", "RSA", "-keysize", "2048",
                    "-sigalg", "SHA256withRSA", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost",
                    "-validity", "1", "-storetype", "PKCS12", "-keystore", keyStore.toString(),
                    "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-keypass:env", "EVC_TEST_TLS_PASSWORD");
            var serverStore = KeyStore.getInstance("PKCS12");
            try (var input = Files.newInputStream(keyStore)) {
                serverStore.load(input, password.toCharArray());
            }
            var trustStore = KeyStore.getInstance("PKCS12");
            trustStore.load(null, null);
            trustStore.setCertificateEntry("telemetry-worker", serverStore.getCertificate("telemetry-worker"));
            var trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trustManagers.init(trustStore);
            trustedContext = SSLContext.getInstance("TLS");
            trustedContext.init(null, trustManagers.getTrustManagers(), null);
        } catch (Exception exception) {
            if (generated != null) {
                try {
                    deleteDirectory(generated);
                } catch (IOException cleanupFailure) {
                    exception.addSuppressed(cleanupFailure);
                }
            }
            throw new IllegalStateException("Could not create temporary TLS test material", exception);
        }
    }

    private void keytool(String... arguments) throws IOException, InterruptedException {
        var command = new java.util.ArrayList<>(List.of(arguments));
        command.addFirst(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve("keytool.log").toFile());
        builder.environment().put("EVC_TEST_TLS_PASSWORD", password);
        var process = builder.start();
        if (!process.waitFor(20, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("TLS test keytool timed out");
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("TLS test keytool failed");
        }
    }

    @Override
    public void close() throws IOException {
        deleteDirectory(directory);
    }

    private static void deleteDirectory(Path directory) throws IOException {
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
