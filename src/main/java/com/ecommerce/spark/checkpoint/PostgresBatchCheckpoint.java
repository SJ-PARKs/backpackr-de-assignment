package com.ecommerce.spark.checkpoint;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Instant;
import java.util.Properties;

public class PostgresBatchCheckpoint implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresBatchCheckpoint.class);

    public static final String IN_PROGRESS = "IN_PROGRESS";
    public static final String SUCCESS     = "SUCCESS";
    public static final String FAILED      = "FAILED";

    private final String jdbcUrl;
    private final String user;
    private final String password;

    public PostgresBatchCheckpoint(String jdbcUrl, String user, String password) {
        this.jdbcUrl  = jdbcUrl;
        this.user     = user;
        this.password = password;
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC driver not found", e);
        }
        initSchema();
    }

    // 첫 실행 시 테이블 자동 생성 (RDS에 직접 접속이 어려운 환경 대응)
    private void initSchema() {
        try (Connection conn = connect();
             Statement st = conn.createStatement()) {
            st.execute(
                "CREATE TABLE IF NOT EXISTS batch_processing_log (" +
                "  partition_date  VARCHAR(10)  PRIMARY KEY," +
                "  status          VARCHAR(20)  NOT NULL," +
                "  run_id          VARCHAR(36)," +
                "  start_time      TIMESTAMP," +
                "  end_time        TIMESTAMP," +
                "  record_count    BIGINT," +
                "  error_message   TEXT" +
                ")"
            );
            LOG.info("batch_processing_log table ready");
        } catch (SQLException e) {
            throw new RuntimeException("Failed to initialize batch_processing_log schema", e);
        }
    }

    // 현재 상태 반환. 레코드 없으면 null.
    public String getStatus(String partitionDate) throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT status FROM batch_processing_log WHERE partition_date = ?")) {
            ps.setString(1, partitionDate);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("status") : null;
            }
        }
    }

    // IN_PROGRESS 상태의 run_id 반환 (stale tmp 경로 정리용)
    public String getRunId(String partitionDate) throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                 "SELECT run_id FROM batch_processing_log WHERE partition_date = ?")) {
            ps.setString(1, partitionDate);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? rs.getString("run_id") : null;
            }
        }
    }

    // INSERT or UPDATE → IN_PROGRESS
    // 기존 상태 row가 있으면 SELECT FOR UPDATE로 잠근 뒤 갱신
    public void upsertInProgress(String partitionDate, String runId) throws SQLException {
        try (Connection conn = connect()) {
            conn.setAutoCommit(false);
            try {
                // 레코드 존재 여부 확인 및 row lock 획득
                boolean exists;
                try (PreparedStatement ps = conn.prepareStatement(
                        "SELECT 1 FROM batch_processing_log WHERE partition_date = ? FOR UPDATE")) {
                    ps.setString(1, partitionDate);
                    try (ResultSet rs = ps.executeQuery()) {
                        exists = rs.next();
                    }
                }

                if (exists) {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "UPDATE batch_processing_log " +
                            "SET status=?, run_id=?, start_time=?, end_time=NULL, " +
                            "    record_count=NULL, error_message=NULL " +
                            "WHERE partition_date=?")) {
                        ps.setString(1, IN_PROGRESS);
                        ps.setString(2, runId);
                        ps.setTimestamp(3, Timestamp.from(Instant.now()));
                        ps.setString(4, partitionDate);
                        ps.executeUpdate();
                    }
                } else {
                    try (PreparedStatement ps = conn.prepareStatement(
                            "INSERT INTO batch_processing_log " +
                            "(partition_date, status, run_id, start_time) VALUES (?,?,?,?)")) {
                        ps.setString(1, partitionDate);
                        ps.setString(2, IN_PROGRESS);
                        ps.setString(3, runId);
                        ps.setTimestamp(4, Timestamp.from(Instant.now()));
                        ps.executeUpdate();
                    }
                }
                conn.commit();
                LOG.debug("upsertInProgress: dt={} runId={}", partitionDate, runId);
            } catch (SQLException e) {
                conn.rollback();
                throw e;
            }
        }
    }

    public void updateSuccess(String partitionDate, long recordCount) throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE batch_processing_log " +
                 "SET status=?, end_time=?, record_count=?, error_message=NULL " +
                 "WHERE partition_date=?")) {
            ps.setString(1, SUCCESS);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            ps.setLong(3, recordCount);
            ps.setString(4, partitionDate);
            ps.executeUpdate();
            LOG.info("updateSuccess: dt={} rows={}", partitionDate, recordCount);
        }
    }

    public void updateFailed(String partitionDate, String errorMessage) throws SQLException {
        try (Connection conn = connect();
             PreparedStatement ps = conn.prepareStatement(
                 "UPDATE batch_processing_log " +
                 "SET status=?, end_time=?, error_message=? " +
                 "WHERE partition_date=?")) {
            ps.setString(1, FAILED);
            ps.setTimestamp(2, Timestamp.from(Instant.now()));
            String truncatedMessage = errorMessage == null
                ? null
                : errorMessage.substring(0, Math.min(errorMessage.length(), 1000));
            ps.setString(3, truncatedMessage);
            ps.setString(4, partitionDate);
            int updated = ps.executeUpdate();
            // IN_PROGRESS → FAILED 전환 시 레코드가 없을 수 있음 (upsert 방식)
            if (updated == 0) {
                LOG.warn("updateFailed: no row found for dt={}, skipping", partitionDate);
            }
        }
    }

    private Connection connect() throws SQLException {
        // DriverManager 우회 - EMR에 Redshift JDBC 드라이버가 jdbc:postgresql:// URL을 가로채는 문제 방지
        try {
            Driver driver = (Driver) Class.forName("org.postgresql.Driver").newInstance();
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("ssl", "false");
            return driver.connect(jdbcUrl, props);
        } catch (SQLException e) {
            throw e;
        } catch (Exception e) {
            throw new SQLException("Failed to load org.postgresql.Driver", e);
        }
    }

    @Override
    public void close() {
        // 연결은 각 메서드에서 try-with-resources로 닫음 - 별도 정리 불필요
    }
}
