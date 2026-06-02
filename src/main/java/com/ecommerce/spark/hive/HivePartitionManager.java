package com.ecommerce.spark.hive;

import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HivePartitionManager {

    private static final Logger LOG = LoggerFactory.getLogger(HivePartitionManager.class);

    private static final String DB_TABLE = "ecommerce.events";

    private final SparkSession spark;

    public HivePartitionManager(SparkSession spark) {
        this.spark = spark;
    }

    // Hive Metastore에 파티션 등록 (이미 있으면 무시)
    public void addPartition(String dt, String location) {
        String sql = String.format(
            "ALTER TABLE %s ADD IF NOT EXISTS PARTITION (dt='%s') LOCATION '%s'",
            DB_TABLE, dt, location);
        LOG.info("Adding partition: dt={} location={}", dt, location);
        spark.sql(sql);
    }

    // 파티션 통계 수집 - Glue / 구형 Hive에서 미지원일 수 있으므로 best-effort
    public void analyzePartition(String dt) {
        String sql = String.format(
            "ANALYZE TABLE %s PARTITION (dt='%s') " +
            "COMPUTE STATISTICS FOR COLUMNS user_id, event_type, generated_session_id",
            DB_TABLE, dt);
        LOG.info("Analyzing partition: dt={}", dt);
        try {
            spark.sql(sql);
        } catch (Exception e) {
            LOG.warn("ANALYZE TABLE skipped for dt={}: {}", dt, e.getMessage());
        }
    }

    // Hive DB + External Table 초기화 (없으면 생성)
    public void initTable(String location) {
        spark.sql("CREATE DATABASE IF NOT EXISTS ecommerce " +
                  "COMMENT 'eCommerce behavior event data'");
        spark.sql(
            "CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.events (" +
            "  event_time           TIMESTAMP," +
            "  event_type           STRING," +
            "  product_id           INT," +
            "  category_id          BIGINT," +
            "  category_code        STRING," +
            "  brand                STRING," +
            "  price                DOUBLE," +
            "  user_id              BIGINT," +
            "  user_session         STRING," +
            "  generated_session_id STRING" +
            ") PARTITIONED BY (dt STRING)" +
            "  STORED AS PARQUET" +
            "  LOCATION '" + location + "'" +
            "  TBLPROPERTIES ('parquet.compression'='SNAPPY')"
        );
        LOG.info("Hive table ecommerce.events initialized at {}", location);
    }
}
