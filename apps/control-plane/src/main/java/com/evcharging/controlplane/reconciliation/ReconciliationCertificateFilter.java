package com.evcharging.controlplane.reconciliation;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Objects;

/** Reads only the certificate verified by the servlet TLS connector. */
@Order(Ordered.HIGHEST_PRECEDENCE)
public final class ReconciliationCertificateFilter extends OncePerRequestFilter {
    public static final String CLIENT_URI_ATTRIBUTE = ReconciliationCertificateFilter.class.getName() + ".clientUri";
    private static final String CERTIFICATES_ATTRIBUTE = "jakarta.servlet.request.X509Certificate";
    private static final String PATH = "/internal/v1/reconciliation";

    private final boolean enabled;
    private final ObjectProvider<ReconciliationAccessPolicy> policies;

    ReconciliationCertificateFilter(boolean enabled, ObjectProvider<ReconciliationAccessPolicy> policies) {
        this.enabled = enabled;
        this.policies = policies;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        String path = request.getServletPath();
        if (!PATH.equals(path) && !path.startsWith(PATH + "/")) {
            chain.doFilter(request, response);
            return;
        }
        if (!enabled) {
            response.sendError(HttpServletResponse.SC_FORBIDDEN);
            return;
        }
        Object attribute = request.getAttribute(CERTIFICATES_ATTRIBUTE);
        if (!request.isSecure() || !(attribute instanceof X509Certificate[] certificates)
                || certificates.length == 0) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        String uri;
        try {
            certificates[0].checkValidity();
            uri = clientUri(certificates[0]);
        } catch (CertificateException exception) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        if (!policies.getObject().recognizes(uri)) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED);
            return;
        }
        request.setAttribute(CLIENT_URI_ATTRIBUTE, uri);
        chain.doFilter(request, response);
    }

    private static String clientUri(X509Certificate certificate) throws CertificateException {
        var alternatives = certificate.getSubjectAlternativeNames();
        if (alternatives == null) throw new CertificateException("Missing service URI SAN");
        List<String> uris = alternatives.stream()
                .filter(value -> value.size() >= 2 && Objects.equals(value.getFirst(), 6))
                .map(value -> String.valueOf(value.get(1)))
                .toList();
        if (uris.size() != 1 || !uris.getFirst().startsWith("urn:ev-charging:reconciliation:")) {
            throw new CertificateException("Expected one reconciliation URI SAN");
        }
        return uris.getFirst();
    }
}
