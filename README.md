# eCommerce Spark Pipeline

Kaggle eCommerce 로그 데이터를 Spark Java로 처리하여 Hive External Table로 서빙하는 배치 ETL 파이프라인입니다.

원본 UTC 이벤트 시간은 그대로 보존하고, KST 기준 날짜 파티션(`dt`)을 별도로 생성합니다. 또한 사용자 행동 흐름을 기준으로 세션 ID를 재생성한 뒤, Hive 또는 Athena에서 조회할 수 있는 분석용 Parquet 테이블로 적재합니다.

## 1. 실행 결과 요약

AWS EMR에서 Spark Application을 실행한 뒤, Athena에서 `make athena-wau`로 WAU 쿼리를 확인했습니다. 실행 결과 2019년 11월 11일 주의 WAU가 가장 높았습니다.

| week_start | wau_by_user | wau_by_session | week_type |
| --- | ---: | ---: | --- |
| 2019-09-30 | 818,388 | 1,570,536 | PARTIAL |
| 2019-10-07 | 1,057,958 | 2,154,180 | FULL |
| 2019-10-14 | 1,090,898 | 2,257,214 | FULL |
| 2019-10-21 | 1,093,146 | 2,153,837 | FULL |
| 2019-10-28 | 1,054,722 | 2,115,233 | FULL |
| 2019-11-04 | 1,321,141 | 2,751,842 | FULL |
| 2019-11-11 | 1,543,309 | 4,754,423 | FULL |
| 2019-11-18 | 1,376,755 | 2,876,494 | FULL |
| 2019-11-25 | 1,176,254 | 2,376,156 | FULL |

해당 주에는 약 154만 명의 고유 사용자가 활동했고, 세션 기준으로는 약 475만 개의 고유 세션이 집계되었습니다.

![Athena WAU query result](docs/images/athena-wau-result.png)

## 2. 실행 방법

### AWS 환경

1. S3에 원본 로그와 빌드된 JAR 파일을 업로드합니다.

```text
s3://ecommerce-spark-pipeline-ap-northeast-2/
├── input/ecommerce/
│   ├── 2019-Oct.csv
│   └── 2019-Nov.csv
└── jars/
    └── ecommerce-spark-1.0.jar
```

`data/` 폴더에 CSV 파일을 넣은 뒤 아래 명령으로 버킷 생성과 업로드를 한 번에 처리할 수 있습니다.

```bash
make docker-up    # Maven 빌드 환경(Docker) 시작
make docker-build # JAR 빌드
make s3-setup     # 버킷 생성, CSV 업로드, JAR 업로드
```

2. `.env` 파일에 환경 변수를 설정한 뒤 RDS와 EMR 클러스터를 생성하고 Step을 제출합니다.

```bash
make create-rds      # RDS PostgreSQL 생성 
make rds-endpoint    # 엔드포인트 확인 및 .env 자동 업데이트
make create-cluster  # EMR 클러스터 생성 및 CLUSTER_ID 자동 업데이트 
make cluster-status  # WAITING 상태 확인 후
make submit          # EMR Step 제출
make status          # Step 진행 상태 확인
make check-output    # S3 output 파일 확인
```

EMR은 Glue Data Catalog를 Hive Metastore로 사용하고, 처리 결과는 S3에 Parquet 형식으로 저장됩니다.

3. Athena에서 WAU 쿼리를 실행해 결과를 확인합니다.

```bash
make athena-wau
```

### 로컬 환경

1. CSV 파일을 프로젝트 루트의 `data/` 폴더에 넣습니다.

```text
data/
├── 2019-Oct.csv
└── 2019-Nov.csv
```

2. Docker 기반 로컬 클러스터를 실행하고, HDFS에 CSV를 업로드한 뒤 Spark Job을 실행합니다.

```bash
make docker-up      # 클러스터 시작

# HDFS 초기화 (최초 1회)
docker exec namenode hdfs dfs -mkdir -p /input/ecommerce /data/ecommerce /tmp/ecommerce
docker exec namenode hdfs dfs -setrep -w 1 /
docker exec namenode hdfs dfs -put /mnt/csv/2019-Oct.csv /input/ecommerce/
docker exec namenode hdfs dfs -put /mnt/csv/2019-Nov.csv /input/ecommerce/

make docker-build   # JAR 빌드 (/app/pom.xml 기준)
make docker-submit  # Spark 실행
make wau            # HiveServer2에서 WAU 쿼리 실행
```

처리 결과는 HDFS `/data/ecommerce/dt=YYYY-MM-DD/` 경로에 Parquet으로 저장되고, Hive External Table로 서빙됩니다.

## 3. 아키텍처

현재 저장소에는 Airflow DAG 구현이 포함되어 있지 않습니다. 다만 전체 아키텍처 그림에서는 일별 배치 스케줄링과 작업 오케스트레이션을 Airflow가 담당한다고 가정했습니다. 실제 실행 진입점은 `Makefile`의 EMR Step 제출 또는 로컬 Docker 실행 명령입니다.

![High-level architecture](docs/images/architecture-overview.png)

![Spark ETL detailed flow](docs/images/spark-etl-detailed-flow.png)

## 4. 설계 결정 상세

### Java 선택 이유

<details>
<summary><strong>Java와 Scala 중 Java를 선택한 이유</strong></summary>

Spark는 Scala 기반으로 만들어졌고 Scala API가 더 간결하다는 장점이 있습니다. 다만 이번 과제에서는 새로운 언어 문법을 익히는 데 시간을 쓰기보다, 데이터 처리 흐름과 파이프라인 안정성에 더 집중하는 것이 중요하다고 판단했습니다.

저는 이전 웹 개발 경험을 통해 Java에 익숙했기 때문에, Java를 사용하면 타입, 예외 처리, 빌드 구조를 더 안정적으로 다룰 수 있었습니다. 또한 Maven 기반으로 의존성을 관리하고, EMR 또는 Docker 환경에서 실행 가능한 JAR로 패키징하기에도 명확했습니다.

따라서 Scala의 간결함보다는 Java의 익숙함과 구현 안정성을 선택했습니다. 그리고 Spark SQL, Window Function, DataFrame API를 사용해 데이터 처리 로직을 구성했습니다.

</details>

### KST 기준 Daily Partition

<details>
<summary><strong>파티션 레벨 구조</strong></summary>

파티션 구조는 `dt=YYYY-MM-DD` 단일 레벨과 `year=/month=/day=` 멀티 레벨 중에서 고민했습니다. 이 과제에서는 파티션 조회 조건이 날짜 단위로 단순하고, Hive 파티션 등록 로직도 간단하게 유지할 수 있어 `dt=YYYY-MM-DD` 단일 레벨을 선택했습니다.

</details>

<details>
<summary><strong>UTC → KST 변환</strong></summary>

원본 `event_time`은 UTC 기준 값으로 그대로 보존합니다. KST 기준 날짜는 파생 컬럼인 `dt`에만 반영했습니다.

단순히 `+9` 시간을 더하는 방식 대신 Spark의 `from_utc_timestamp(event_time, "Asia/Seoul")`을 사용했습니다. 타임존 처리를 Spark에 위임하면 타임존 정책이나 예외 처리를 하드코딩하지 않아도 됩니다.

</details>

<details>
<summary><strong>Cross-day 세션 처리</strong></summary>

세션 ID는 `dt`를 기준으로 데이터를 나누기 전에 먼저 생성합니다.

예를 들어 23:58부터 00:03까지 이어지는 세션이 있을 때, 날짜 파티션을 먼저 나눈 뒤 세션을 계산하면 같은 세션이 서로 다른 ID를 받을 수 있습니다. 이를 방지하기 위해 전체 입력 데이터에서 먼저 세션 ID를 생성하고, 그 다음 KST 기준 `dt` 컬럼을 추가했습니다.

</details>

### 세션 ID 재생성

<details>
<summary><strong>세션 ID 포맷</strong></summary>

세션 ID 포맷은 `user_id_seq`와 `user_id_yyyyMMddHHmmss` 중에서 후자를 선택했습니다.

`user_id_seq` 방식은 앞선 기간의 데이터가 추가되면 기존 세션들의 순번이 밀릴 수 있습니다. 예를 들어 나중에 2019년 9월 데이터가 추가되면 이미 적재된 10월, 11월 세션 ID가 바뀔 수 있습니다.

반면 `user_id_yyyyMMddHHmmss` 방식은 세션 시작 시각이 바뀌지 않는 한 ID가 안정적으로 유지됩니다. 또한 ID만 보아도 어떤 사용자에게서 언제 시작된 세션인지 파악하기 쉽습니다.

</details>

<details>
<summary><strong>구현 방식</strong></summary>

세션 분리 방식은 크게 두 가지를 고려했습니다.

첫 번째는 `user_id`로 그룹핑한 뒤 커스텀 함수로 세션을 분리하고 결과를 다시 조인하는 방식입니다. 이 경우 groupBy와 join 과정에서 셔플이 여러 번 발생합니다.

두 번째는 Window Function을 사용하는 방식입니다. 현재 코드는 `PARTITION BY user_id` 기준으로 `LAG`를 사용해 이전 이벤트와의 시간 차이를 계산하고, `LAST(..., true)`로 가장 최근 세션 시작 시각을 전파합니다. 이 방식은 Spark SQL 연산으로 세션 ID를 생성할 수 있어 더 단순하고, 별도 UDF 조인 구조도 필요하지 않습니다.

</details>

<details>
<summary><strong>Data Skew</strong></summary>

Window 연산은 `user_id` 단위로 데이터를 묶어야 하므로, 특정 사용자에게 이벤트가 과도하게 몰려 있으면 한 파티션이 오래 걸릴 수 있습니다.

일반적인 skew 완화 방법인 salting은 이 경우 조심해야 합니다. 같은 사용자의 이벤트를 여러 salt로 쪼개면 세션 경계를 정확히 계산하기 어려워질 수 있기 때문입니다.

현재 코드는 처리 전에 사용자별 이벤트 수 분포를 로그로 출력하고, `skew.threshold` 기본값인 10,000건을 초과하는 사용자를 감지합니다. Spark AQE 설정도 켜두었지만, Window skew 자체를 완전히 해결하는 장치라기보다는 실행 환경의 기본 최적화를 활성화하는 성격에 가깝습니다. 따라서 이 부분은 skew를 사전에 확인하고 운영 시 병목 후보를 빠르게 파악하기 위한 방어 장치로 보았습니다.

</details>

### Hive External Table

<details>
<summary><strong>External Table 선택 이유</strong></summary>

Spark가 S3 또는 HDFS에 Parquet 파일을 직접 쓰고, Hive는 해당 위치를 참조만 하도록 External Table을 사용했습니다.

이 구조에서는 데이터의 소유권이 Spark 파이프라인에 있고, Hive는 메타데이터와 조회 인터페이스 역할을 합니다. 따라서 실수로 Hive 테이블을 삭제하더라도 실제 Parquet 파일은 보존됩니다. 또한 Spark, Hive, Athena 같은 여러 엔진이 동일한 파일을 참조할 수 있습니다.

</details>

<details>
<summary><strong>파티션 등록 방식</strong></summary>

파티션 등록은 `MSCK REPAIR TABLE` 대신 `ALTER TABLE ADD IF NOT EXISTS PARTITION` 방식을 사용했습니다.

`MSCK REPAIR TABLE`은 전체 경로를 스캔해 파티션을 한 번에 등록할 수 있어 간단하지만, 파티션 수가 많아질수록 비용이 커질 수 있습니다. 현재 파이프라인은 Spark Job이 특정 `dt` 파티션을 쓴 직후 해당 파티션만 명시적으로 등록하므로, 불필요한 전체 스캔을 피할 수 있습니다.

</details>

<details>
<summary><strong>DDL</strong></summary>

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

</details>

<details>
<summary><strong>파티션 등록 및 통계 수집</strong></summary>

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

</details>

<details>
<summary><strong>추가 기간 처리</strong></summary>

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

</details>

### 장애 복구

<details>
<summary><strong>상태 저장 위치</strong></summary>

파티션별 처리 상태는 PostgreSQL의 `batch_processing_log` 테이블에 저장합니다.

PostgreSQL은 로컬 환경에서는 Hive Metastore 백엔드로 이미 사용하고 있고, AWS 환경에서는 RDS를 사용할 수 있으므로 별도의 상태 저장 인프라를 추가하지 않아도 됩니다. 상태 값은 `IN_PROGRESS`, `SUCCESS`, `FAILED`이며, `run_id`, 처리 시작/종료 시각, 처리 건수, 에러 메시지도 함께 저장합니다.

</details>

<details>
<summary><strong>멱등성</strong></summary>

재실행해도 결과가 달라지지 않도록 파티션 단위로 처리합니다. 현재 코드의 처리 순서는 다음과 같습니다.

1. PostgreSQL에서 현재 파티션 상태를 확인합니다.
2. `SUCCESS`이고 실제 파일이 존재하면 건너뜁니다.
3. `SUCCESS`인데 파일이 없으면 `FAILED`로 변경한 뒤 재처리합니다.
4. `IN_PROGRESS`이면 이전 `run_id`의 임시 경로를 정리하고 `FAILED`로 변경한 뒤 재처리합니다.
5. 새 `run_id`를 생성하고 `IN_PROGRESS`로 기록합니다.
6. Parquet 파일을 씁니다.
7. Hive 파티션을 등록합니다.
8. 파티션 통계를 수집합니다.
9. 마지막으로 PostgreSQL 상태를 `SUCCESS`로 변경합니다.

핵심은 `SUCCESS` 기록을 가장 마지막에 수행하는 것입니다. 쓰기 또는 파티션 등록 도중 실패하면 상태가 `IN_PROGRESS` 또는 `FAILED`로 남기 때문에 재실행 시 다시 처리할 수 있습니다.

</details>

<details>
<summary><strong>쓰기 방식</strong></summary>

S3와 HDFS는 쓰기 방식이 다릅니다.

S3 출력 경로에서는 Spark의 overwrite 쓰기 방식을 사용합니다. S3는 HDFS처럼 원자적 rename을 기대하기 어렵기 때문에 Spark의 S3 쓰기 동작에 맡기는 구조입니다.

HDFS 출력 경로에서는 `checkpointDir/runId/dt=YYYY-MM-DD` 임시 경로에 먼저 쓰고, 쓰기가 끝난 뒤 최종 경로인 `outputDir/dt=YYYY-MM-DD`로 rename합니다. 기존 최종 경로가 있으면 `_old_` 경로로 옮긴 뒤 교체하고, 성공하면 old 경로를 삭제합니다.

</details>

<details>
<summary><strong>동시 실행에 대한 운영 가정</strong></summary>

`batch_processing_log` 갱신 시 `SELECT ... FOR UPDATE`를 사용해 상태 레코드 변경을 트랜잭션 안에서 처리합니다.

다만 현재 애플리케이션은 동일 파티션에 대해 여러 Spark Job을 동시에 실행하는 상황까지 완전히 큐잉하거나 skip하도록 만든 구조는 아닙니다. 따라서 운영에서는 같은 입력 기간에 대한 중복 Job 제출을 피하는 것을 전제로 두고, 실패 후 재실행 또는 날짜 범위 기반 추가 적재를 주 사용 시나리오로 보았습니다.

</details>

<details>
<summary><strong>실패 시나리오별 복구</strong></summary>

| 실패 지점 | 재실행 시 동작 |
| --- | --- |
| Parquet 쓰기 중 실패 | `IN_PROGRESS` 감지 후 이전 `run_id`의 tmp 경로를 정리하고 재처리합니다. |
| HDFS rename 중 실패 | 남아 있는 tmp 또는 old 경로를 기준으로 정리 후 재처리합니다. |
| Hive 파티션 등록 중 실패 | `ADD IF NOT EXISTS`를 사용하므로 재실행 시 중복 등록을 피하면서 다시 처리합니다. |
| `SUCCESS` 기록 직후 | `SUCCESS` 상태와 실제 파일 존재를 확인한 뒤 skip합니다. |
| `SUCCESS`인데 파일 없음 | `FAILED`로 변경한 뒤 재처리합니다. |

</details>

### WAU 쿼리

<details>
<summary><strong>user 기준 WAU와 session 기준 WAU를 함께 계산</strong></summary>

user 기준 WAU와 session 기준 WAU를 따로 계산하면 같은 기간의 데이터를 두 번 스캔하게 됩니다.

```sql
-- 쿼리 1
SELECT week_start, COUNT(DISTINCT user_id) FROM ...

-- 쿼리 2
SELECT week_start, COUNT(DISTINCT generated_session_id) FROM ...
```

이 과제의 입력 데이터는 1억 건 이상이므로, 같은 테이블을 반복 스캔하지 않는 것이 중요합니다. 그래서 하나의 쿼리에서 두 지표를 함께 계산했습니다.

```sql
SELECT
    week_start,
    COUNT(DISTINCT user_id)              AS wau_by_user,
    COUNT(DISTINCT generated_session_id) AS wau_by_session
FROM ecommerce.events
GROUP BY week_start;
```

한 사용자가 여러 세션을 가질 수 있으므로 일반적으로 `wau_by_session >= wau_by_user` 관계가 성립합니다.

</details>

<details>
<summary><strong>Hive와 Athena의 주 시작일 처리</strong></summary>

Athena 쿼리에서는 `date_trunc('WEEK', CAST(dt AS DATE))`를 사용해 주 시작일을 계산합니다.

```sql
SELECT
    date_trunc('WEEK', CAST(dt AS DATE))              AS week_start,
    COUNT(DISTINCT user_id)                           AS wau_by_user,
    COUNT(DISTINCT generated_session_id)              AS wau_by_session,
    CASE
        WHEN date_trunc('WEEK', CAST(dt AS DATE)) < DATE '2019-10-01'
          OR date_add('day', 6, date_trunc('WEEK', CAST(dt AS DATE))) > DATE '2019-12-01'
        THEN 'PARTIAL'
        ELSE 'FULL'
    END                                               AS week_type
FROM ecommerce.events
GROUP BY date_trunc('WEEK', CAST(dt AS DATE))
ORDER BY week_start;
```

로컬 Hive 2.3.2 환경에서는 `date_trunc('WEEK')` 지원이 제한적이어서, `sql/04_wau_query.sql`에서는 다음 표현으로 월요일 기준 주 시작일을 계산했습니다.

```sql
NEXT_DAY(DATE_SUB(TO_DATE(dt), 7), 'MO')
```

이렇게 환경별 함수 차이를 반영해 로컬 Hive와 Athena 양쪽에서 WAU를 확인할 수 있도록 했습니다.

</details>

<details>
<summary><strong>데이터 경계 처리</strong></summary>

원본 데이터는 2019년 10월과 11월 로그입니다. 첫 주와 마지막 주는 전체 7일 데이터가 모두 들어 있지 않을 수 있습니다.

이런 경계 주를 단순히 제거하면 분석 결과가 왜곡될 수 있습니다. 예를 들어 특정 주의 WAU가 낮게 나왔을 때, 실제 유저가 줄어든 것인지 데이터가 일부 날짜만 포함된 것인지 구분하기 어렵습니다.

그래서 제거 대신 `week_type` 플래그를 제공합니다.

```sql
CASE
    WHEN week_start < '2019-10-01'
      OR DATE_ADD(week_start, 6) > '2019-12-01'
    THEN 'PARTIAL'
    ELSE 'FULL'
END AS week_type
```

분석 사용자는 `PARTIAL` 주를 제외할지, 참고용으로만 볼지 직접 판단할 수 있습니다.

</details>

## 5. AI 활용 범위

### 코드와 설계 검토

전반적인 코드 작성과 설계 검토 과정에서 Claude와 GPT를 함께 활용했습니다. 특히 세션 경계, 파티션 처리, 멱등성, 장애 복구처럼 실무 관점에서 놓칠 수 있는 부분을 점검하는 용도로 사용했습니다.

### Hive 관련 문법 확인

Hive 사용 경험이 많지 않아 External Table, 파티션 등록, `ANALYZE TABLE`, Hive 함수 차이 등을 확인할 때 AI와 문서를 함께 참고했습니다.

### 오류 원인 확인

AWS 환경에서 실행하면서 발생한 오류 원인을 확인하는 데도 AI를 활용했습니다. 예를 들어 RDS JDBC URL 뒤에 `?ssl=false`를 붙이지 않으면 SSL 인증서 관련 오류가 발생할 수 있다는 점, PostgreSQL 비밀번호 정책처럼 환경 설정에서 놓치기 쉬운 부분을 확인했습니다.

### 프롬프트 전략

AI에는 단순히 코드를 생성해달라고 요청하기보다, 먼저 요구사항을 기준으로 설계 누락 가능성을 점검하도록 요청했습니다. 예를 들어 KST 파티션 처리, cross-day 세션, 재처리 멱등성, Hive External Table 파티션 등록 방식처럼 데이터 파이프라인에서 문제가 생길 수 있는 지점을 질문했습니다.

구현 단계에서는 제가 작성한 코드와 README를 기준으로, 코드와 설명이 서로 맞지 않는 부분이 있는지 검토하는 방식으로 활용했습니다. 최종 판단과 AWS/로컬 실행 검증은 직접 수행했고, 설명할 수 없는 코드는 제출하지 않는 것을 기준으로 삼았습니다.
