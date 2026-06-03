# Hive External Table

## External Table 선택 이유

Spark가 S3 또는 HDFS에 Parquet 파일을 직접 쓰고, Hive는 해당 위치를 참조만 하도록 External Table을 사용했습니다.

이 구조에서는 데이터의 소유권이 Spark 파이프라인에 있고, Hive는 메타데이터와 조회 인터페이스 역할을 합니다. 따라서 실수로 Hive 테이블을 삭제하더라도 실제 Parquet 파일은 보존됩니다. 또한 Spark, Hive, Athena 같은 여러 엔진이 동일한 파일을 참조할 수 있습니다.

## 파티션 등록 방식

파티션 등록은 `MSCK REPAIR TABLE` 대신 `ALTER TABLE ADD IF NOT EXISTS PARTITION` 방식을 사용했습니다.

`MSCK REPAIR TABLE`은 전체 경로를 스캔해 파티션을 한 번에 등록할 수 있어 간단하지만, 파티션 수가 많아질수록 비용이 커질 수 있습니다. 현재 파이프라인은 Spark Job이 특정 `dt` 파티션을 쓴 직후 해당 파티션만 명시적으로 등록하므로, 불필요한 전체 스캔을 피할 수 있습니다.

## DDL

```sql
CREATE DATABASE IF NOT EXISTS ecommerce
COMMENT 'eCommerce behavior event data';

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
```

Spark 애플리케이션에서도 동일한 테이블 구조를 기준으로 `ecommerce.events` External Table을 초기화합니다.

## 파티션 등록 및 통계 수집

Spark 앱은 파티션 쓰기 완료 직후 다음과 같은 방식으로 Hive 파티션을 등록합니다.

```sql
ALTER TABLE ecommerce.events
ADD IF NOT EXISTS PARTITION (dt='2019-10-01')
LOCATION 'hdfs://namenode:9000/data/ecommerce/dt=2019-10-01';
```

이후 `ANALYZE TABLE`을 실행해 옵티마이저가 사용할 수 있는 통계를 수집합니다. 다만 Glue 또는 Hive 환경에 따라 지원 여부가 다를 수 있어, 코드에서는 실패해도 전체 Job을 중단하지 않고 warning 로그만 남기도록 처리했습니다.

```sql
ANALYZE TABLE ecommerce.events
PARTITION (dt='2019-10-01')
COMPUTE STATISTICS FOR COLUMNS user_id, event_type, generated_session_id;
```

## 추가 기간 처리

애플리케이션 파라미터로 처리할 날짜 범위를 제한할 수 있습니다.

```bash
spark-submit ecommerce-processor.jar \
  --input-dir      hdfs://namenode:9000/input/ecommerce/ \
  --output-dir     hdfs://namenode:9000/data/ecommerce/ \
  --checkpoint-dir hdfs://namenode:9000/checkpoint/ \
  --start-date     2019-12-01 \
  --end-date       2019-12-31
```

`--start-date`, `--end-date`가 없으면 전체 기간을 처리합니다. 새 CSV를 input 경로에 추가한 뒤 날짜 범위를 지정하면 추가 기간만 적재할 수 있습니다.

다만 세션 경계의 정확성을 위해 읽기는 전체 입력을 대상으로 수행하고, 실제 쓰기 대상 파티션만 날짜 범위로 제한합니다.
