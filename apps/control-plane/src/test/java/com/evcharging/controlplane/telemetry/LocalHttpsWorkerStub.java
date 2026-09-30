package com.evcharging.controlplane.telemetry;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/** Real HTTPS socket with short-lived, test-generated certificates. */
final class LocalHttpsWorkerStub implements AutoCloseable {
    record Reply(int status, String body, long beforeHeadersMs, long beforeBodyMs) {
        static Reply json(int status, String body) {
            return new Reply(status, body, 0, 0);
        }
    }

    final Path directory;
    final Path trustStore;
    final Path untrustedStore;
    final String password = UUID.randomUUID().toString();
    final AtomicInteger calls = new AtomicInteger();
    final AtomicReference<String> lastPath = new AtomicReference<>();
    final AtomicReference<Function<String, Reply>> respond = new AtomicReference<>();

    private final HttpsServer server;
    private final ExecutorService executor = Executors.newCachedThreadPool();

    LocalHttpsWorkerStub() {
        try {
            directory = Files.createTempDirectory("operator-query-tls-");
            Path serverStore = directory.resolve("server.p12");
            Path otherStore = directory.resolve("other.p12");
            trustStore = directory.resolve("trusted.p12");
            untrustedStore = directory.resolve("untrusted.p12");
            keytool(serverStore, "telemetry-worker");
            keytool(otherStore, "untrusted-worker");
            var keyStore = load(serverStore);
            saveTrust(keyStore, trustStore);
            saveTrust(load(otherStore), untrustedStore);
            var keys = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keys.init(keyStore, password.toCharArray());
            var tls = SSLContext.getInstance("TLS");
            tls.init(keys.getKeyManagers(), null, null);
            server = HttpsServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            server.setHttpsConfigurator(new HttpsConfigurator(tls));
            server.setExecutor(executor);
            server.createContext("/", this::handle);
            server.start();
        } catch (Exception exception) {
            executor.shutdownNow();
            throw new IllegalStateException("Could not start temporary HTTPS worker stub", exception);
        }
    }

    int port() {
        return server.getAddress().getPort();
    }

    private void handle(HttpExchange exchange) throws IOException {
        calls.incrementAndGet();
        String path = exchange.getRequestURI().getRawPath();
        lastPath.set(path);
        Reply reply = respond.get().apply(path);
        try {
            Thread.sleep(reply.beforeHeadersMs());
            byte[] bytes = reply.body().getBytes(StandardCharsets.UTF_8);
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(reply.status(), bytes.length);
            Thread.sleep(reply.beforeBodyMs());
            exchange.getResponseBody().write(bytes);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
            // A timed-out client may have already closed its socket.
        } finally {
            exchange.close();
        }
    }

    private void keytool(Path target, String alias) throws Exception {
        var command = new java.util.ArrayList<>(List.of(
                "-genkeypair", "-alias", alias, "-keyalg", "RSA", "-keysize", "2048",
                "-sigalg", "SHA256withRSA", "-dname", "CN=localhost", "-ext", "SAN=dns:localhost",
                "-validity", "1", "-storetype", "PKCS12", "-keystore", target.toString(),
                "-storepass:env", "EVC_TEST_TLS_PASSWORD", "-keypass:env", "EVC_TEST_TLS_PASSWORD"));
        command.addFirst(Path.of(System.getProperty("java.home"), "bin", "keytool").toString());
        var builder = new ProcessBuilder(command).redirectErrorStream(true)
                .redirectOutput(directory.resolve("keytool.log").toFile());
        builder.environment().put("EVC_TEST_TLS_PASSWORD", password);
        var process = builder.start();
        if (!process.waitFor(20, TimeUnit.SECONDS)) {
            process.destroyForcibly();
            throw new IllegalStateException("keytool timed out");
        }
        if (process.exitValue() != 0) {
            throw new IllegalStateException("keytool failed");
        }
    }

    private KeyStore load(Path path) throws Exception {
        var store = KeyStore.getInstance("PKCS12");
        try (var input = Files.newInputStream(path)) {
            store.load(input, password.toCharArray());
        }
        return store;
    }

    private void saveTrust(KeyStore source, Path target) throws Exception {
        var store = KeyStore.getInstance("PKCS12");
        store.load(null, null);
        store.setCertificateEntry("trusted", source.getCertificate(source.aliases().nextElement()));
        try (var output = Files.newOutputStream(target)) {
            store.store(output, password.toCharArray());
        }
    }

    @Override
    public void close() throws IOException {
        server.stop(0);
        executor.shutdownNow();
        try (var paths = Files.walk(directory)) {
            for (var path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
