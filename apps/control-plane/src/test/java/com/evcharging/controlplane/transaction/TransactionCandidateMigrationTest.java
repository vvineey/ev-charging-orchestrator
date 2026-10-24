package com.evcharging.controlplane.transaction;

import io.zonky.test.db.postgres.embedded.EmbeddedPostgres;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class TransactionCandidateMigrationTest {
    @Test
    void existingFinalizedAndHeldSessionsBecomeProvisionalAndInvalidatedOnUpgrade() throws Exception {
        try (EmbeddedPostgres postgres = EmbeddedPostgres.builder().start()) {
            var dataSource = postgres.getPostgresDatabase();
            Flyway.configure().dataSource(dataSource).locations("classpath:db/migration")
                    .target(MigrationVersion.fromVersion("1")).load().migrate();
            var jdbc = new JdbcTemplate(dataSource);
            jdbc.update("""
                    INSERT INTO transaction_inbox_state
                      (charging_station_id, transaction_id, station_id, status, hold_reason)
                    VALUES ('CS-TEST', 'TX-EARLY', 'ST-TEST', 'FINALIZED', NULL),
                           ('CS-TEST', 'TX-LATE', 'ST-TEST', 'HOLD_AFTER_FINALIZATION', 'SOURCE_CONFLICT')
                    """);
            jdbc.update("""
                    INSERT INTO recovered_transaction_session
                      (charging_station_id, transaction_id, station_id, evse_id,
                       started_at, ended_at, energy_wh)
                    VALUES ('CS-TEST', 'TX-EARLY', 'ST-TEST', 1,
                            '2026-01-01 10:00:00+00', '2026-01-01 10:20:00+00', 2500),
                           ('CS-TEST', 'TX-LATE', 'ST-TEST', 1,
                            '2026-01-01 10:00:00+00', '2026-01-01 10:20:00+00', 2500)
                    """);

            Flyway.configure().dataSource(dataSource).locations("classpath:db/migration").load().migrate();

            assertThat(jdbc.queryForObject("""
                    SELECT status FROM transaction_inbox_state WHERE transaction_id = 'TX-EARLY'
                    """, String.class)).isEqualTo("PROVISIONAL");
            assertThat(jdbc.queryForObject("""
                    SELECT status FROM transaction_inbox_state WHERE transaction_id = 'TX-LATE'
                    """, String.class)).isEqualTo("HOLD_AFTER_PROVISIONAL");
            assertThat(jdbc.queryForObject("""
                    SELECT candidate_status FROM transaction_session_candidate
                    WHERE transaction_id = 'TX-EARLY'
                    """, String.class)).isEqualTo("PROVISIONAL");
            assertThat(jdbc.queryForObject("""
                    SELECT candidate_status FROM transaction_session_candidate
                    WHERE transaction_id = 'TX-LATE'
                    """, String.class)).isEqualTo("INVALIDATED");
            assertThat(jdbc.queryForObject("""
                    SELECT invalidation_reason FROM transaction_session_candidate
                    WHERE transaction_id = 'TX-LATE'
                    """, String.class)).isEqualTo("SOURCE_CONFLICT");
            assertThat(jdbc.queryForObject("""
                    SELECT count(*) FROM transaction_session_candidate
                    WHERE calculated_at IS NOT NULL AND energy_wh = 2500
                    """, Long.class)).isEqualTo(2L);
        }
    }
}
