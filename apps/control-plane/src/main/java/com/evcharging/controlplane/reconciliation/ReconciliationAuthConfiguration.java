package com.evcharging.controlplane.reconciliation;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration(proxyBeanMethods = false)
@ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
@EnableConfigurationProperties(ReconciliationAuthProperties.class)
public class ReconciliationAuthConfiguration {
    @Bean
    @ConditionalOnProperty(name = "reconciliation.auth.enabled", havingValue = "true")
    ReconciliationAccessPolicy reconciliationAccessPolicy(ReconciliationAuthProperties properties, Environment environment) {
        requireTls(environment);
        return new ReconciliationAccessPolicy(properties);
    }

    @Bean
    ReconciliationCertificateFilter reconciliationCertificateFilter(ReconciliationAuthProperties properties,
                                                                     org.springframework.beans.factory.ObjectProvider<ReconciliationAccessPolicy> policies) {
        return new ReconciliationCertificateFilter(properties.enabled(), policies);
    }

    private static void requireTls(Environment environment) {
        String clientAuth = environment.getProperty("server.ssl.client-auth", "");
        if (!environment.getProperty("server.ssl.enabled", Boolean.class, false)
                || !("want".equalsIgnoreCase(clientAuth) || "need".equalsIgnoreCase(clientAuth))
                || blank(environment.getProperty("server.ssl.key-store"))
                || blank(environment.getProperty("server.ssl.trust-store"))) {
            throw new IllegalStateException("Reconciliation authentication requires HTTPS, client certificate verification, key-store and trust-store");
        }
    }

    private static boolean blank(String value) {
        return value == null || value.isBlank();
    }
}
