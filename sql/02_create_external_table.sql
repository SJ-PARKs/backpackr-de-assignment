-- Hive: ecommerce.events External Table 생성
--
-- dt 파티션: KST 기준 YYYY-MM-DD
-- LOCATION: Spark Application이 Parquet을 쓰는 HDFS 경로
-- 파티션 등록은 Spark Application(HivePartitionManager)이 직접 수행

CREATE EXTERNAL TABLE IF NOT EXISTS ecommerce.events (
    event_time           TIMESTAMP,
    event_type           STRING,
    product_id           INT,
    category_id          BIGINT,
    category_code        STRING,
    brand                STRING,
    price                DOUBLE,
    user_id              BIGINT,
    user_session         STRING,
    generated_session_id STRING
)
COMMENT 'eCommerce user behavior events, sessionized, KST daily partitioned'
PARTITIONED BY (dt STRING)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/data/ecommerce'
TBLPROPERTIES (
    'parquet.compression'='SNAPPY',
    'parquet.metadata.read.parallelism'='16'
);
