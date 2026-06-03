# 장애 복구

## 상태 저장 위치

파티션별 처리 상태는 PostgreSQL의 `batch_processing_log` 테이블에 저장합니다.

PostgreSQL은 로컬 환경에서는 Hive Metastore 백엔드로 이미 사용하고 있고, AWS 환경에서는 RDS를 사용할 수 있으므로 별도의 상태 저장 인프라를 추가하지 않아도 됩니다. 상태 값은 `IN_PROGRESS`, `SUCCESS`, `FAILED`이며, `run_id`, 처리 시작/종료 시각, 처리 건수, 에러 메시지도 함께 저장합니다.

## 멱등성

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

## 쓰기 방식

S3와 HDFS는 쓰기 방식이 다릅니다.

S3 출력 경로에서는 Spark의 overwrite 쓰기 방식을 사용합니다. S3는 HDFS처럼 원자적 rename을 기대하기 어렵기 때문에 Spark의 S3 쓰기 동작에 맡기는 구조입니다.

HDFS 출력 경로에서는 `checkpointDir/runId/dt=YYYY-MM-DD` 임시 경로에 먼저 쓰고, 쓰기가 끝난 뒤 최종 경로인 `outputDir/dt=YYYY-MM-DD`로 rename합니다. 기존 최종 경로가 있으면 `_old_` 경로로 옮긴 뒤 교체하고, 성공하면 old 경로를 삭제합니다.

## 동시 실행에 대한 운영 가정

`batch_processing_log` 갱신 시 `SELECT ... FOR UPDATE`를 사용해 상태 레코드 변경을 트랜잭션 안에서 처리합니다.

다만 현재 애플리케이션은 동일 파티션에 대해 여러 Spark Job을 동시에 실행하는 상황까지 완전히 큐잉하거나 skip하도록 만든 구조는 아닙니다. 따라서 운영에서는 같은 입력 기간에 대한 중복 Job 제출을 피하는 것을 전제로 두고, 실패 후 재실행 또는 날짜 범위 기반 추가 적재를 주 사용 시나리오로 보았습니다.

## 실패 시나리오별 복구

| 실패 지점 | 재실행 시 동작 |
| --- | --- |
| Parquet 쓰기 중 실패 | `IN_PROGRESS` 감지 후 이전 `run_id`의 tmp 경로를 정리하고 재처리합니다. |
| HDFS rename 중 실패 | 남아 있는 tmp 또는 old 경로를 기준으로 정리 후 재처리합니다. |
| Hive 파티션 등록 중 실패 | `ADD IF NOT EXISTS`를 사용하므로 재실행 시 중복 등록을 피하면서 다시 처리합니다. |
| `SUCCESS` 기록 직후 | `SUCCESS` 상태와 실제 파일 존재를 확인한 뒤 skip합니다. |
| `SUCCESS`인데 파일 없음 | `FAILED`로 변경한 뒤 재처리합니다. |
