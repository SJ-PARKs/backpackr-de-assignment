-- PostgreSQL: 배치 처리 상태 테이블
-- Hive Metastore 백엔드와 동일 인스턴스(metastore DB)에 생성
--
-- status 값:
--   IN_PROGRESS : 처리 중 (장애 시 이 상태로 남음)
--   SUCCESS     : 처리 완료 + HDFS 파일 확인됨
--   FAILED      : 명시적 실패 또는 HDFS 파일 없는 SUCCESS → FAILED 강등

CREATE TABLE IF NOT EXISTS batch_processing_log (
    partition_date  VARCHAR(10)   PRIMARY KEY,   -- KST YYYY-MM-DD
    status          VARCHAR(20)   NOT NULL,
    run_id          VARCHAR(36),                  -- UUID, staging 경로 구분용
    start_time      TIMESTAMP,
    end_time        TIMESTAMP,
    record_count    BIGINT,
    error_message   TEXT
);
