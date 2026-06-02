package com.ecommerce.spark;

import com.ecommerce.spark.checkpoint.PostgresBatchCheckpoint;
import com.ecommerce.spark.config.AppConfig;
import com.ecommerce.spark.hive.HivePartitionManager;
import com.ecommerce.spark.schema.EventSchema;
import com.ecommerce.spark.transform.KstDateTransformer;
import com.ecommerce.spark.transform.SessionIdGenerator;
import com.ecommerce.spark.writer.PartitionWriter;
import com.ecommerce.spark.writer.SkewDetector;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.col;

public class EcommerceProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(EcommerceProcessor.class);

    public static void main(String[] args) throws Exception {
        AppConfig config = AppConfig.parse(args);
        SparkSession spark = config.buildSparkSession();

        // ── 1. CSV 읽기 (명시적 스키마) ──────────────────────────────────
        Dataset<Row> raw = spark.read()
            .option("header", "true")
            .option("timestampFormat", "yyyy-MM-dd HH:mm:ss z")
            .schema(EventSchema.SCHEMA)
            .csv(config.getInputDir());

        // ── 2. 세션 ID 생성 (dt 분리 이전에 수행 - cross-day 세션 정확성) ─
        Dataset<Row> withSession = SessionIdGenerator.generate(raw);

        // ── 3. KST dt 파티션 컬럼 추가 ───────────────────────────────────
        Dataset<Row> withDt = KstDateTransformer.transform(withSession);

        // ── 4. 캐시 (파티션 루프에서 재사용) ─────────────────────────────
        withDt.persist(StorageLevel.MEMORY_AND_DISK_SER());

        // ── 5. Skew 감지 (캐시를 채우는 첫 액션이기도 함) ─────────────────
        SkewDetector.detect(withDt, config.getSkewThreshold());

        // ── 6. 처리 대상 dt 목록 수집 ─────────────────────────────────────
        List<String> targetDates = withDt.select("dt")
            .distinct()
            .orderBy("dt")
            .as(Encoders.STRING())
            .collectAsList()
            .stream()
            .filter(config::isInRange)
            .collect(Collectors.toList());

        if (targetDates.isEmpty()) {
            LOG.warn("No partitions to process for inputDir={}", config.getInputDir());
            spark.stop();
            return;
        }
        LOG.info("Processing {} partitions: {} ~ {}",
            targetDates.size(), targetDates.get(0), targetDates.get(targetDates.size() - 1));

        // ── 7. 컴포넌트 초기화 ────────────────────────────────────────────
        PostgresBatchCheckpoint checkpoint = new PostgresBatchCheckpoint(
            config.getPgJdbcUrl(), config.getPgUser(), config.getPgPassword());
        HivePartitionManager hiveManager = new HivePartitionManager(spark);

        // Hive DB/Table 없으면 자동 생성 (EMR 첫 실행 시)
        hiveManager.initTable(config.getOutputDir());
        PartitionWriter writer = new PartitionWriter(
            spark, config.getOutputDir(), config.getCheckpointDir());
        FileSystem fs = FileSystem.get(
            URI.create(config.getOutputDir()), spark.sparkContext().hadoopConfiguration());

        // ── 8. 파티션 루프 ────────────────────────────────────────────────
        for (String dt : targetDates) {
            processPartition(dt, config, checkpoint, hiveManager, writer, fs, withDt);
        }

        withDt.unpersist();
        checkpoint.close();
        spark.stop();
        LOG.info("EcommerceProcessor completed successfully.");
    }

    private static void processPartition(
            String dt,
            AppConfig config,
            PostgresBatchCheckpoint checkpoint,
            HivePartitionManager hiveManager,
            PartitionWriter writer,
            FileSystem fs,
            Dataset<Row> withDt) throws Exception {

        String finalPath = config.getOutputDir().replaceAll("/$", "") + "/dt=" + dt;

        // ── 상태 확인 및 멱등 처리 ────────────────────────────────────────
        String status = checkpoint.getStatus(dt);

        if (PostgresBatchCheckpoint.SUCCESS.equals(status)) {
            boolean outputOk = fs.exists(new Path(finalPath))
                && fs.listStatus(new Path(finalPath)).length > 0;
            if (outputOk) {
                LOG.info("Skipping dt={} (SUCCESS + output files verified)", dt);
                return;
            }
            LOG.warn("dt={} SUCCESS in PG but output files are missing - resetting to FAILED", dt);
            checkpoint.updateFailed(dt, "Output files missing after SUCCESS");
        }

        if (PostgresBatchCheckpoint.IN_PROGRESS.equals(status)) {
            String oldRunId = checkpoint.getRunId(dt);
            if (oldRunId != null) {
                Path staleTmp = new Path(
                    config.getCheckpointDir().replaceAll("/$", "") + "/" + oldRunId + "/dt=" + dt);
                try {
                    if (fs.exists(staleTmp)) {
                        fs.delete(staleTmp, true);
                        LOG.info("Cleaned stale tmp: {}", staleTmp);
                    }
                } catch (Exception e) {
                    LOG.warn("Could not clean stale tmp {}: {}", staleTmp, e.getMessage());
                }
            }
            checkpoint.updateFailed(dt, "Restarted after IN_PROGRESS");
        }

        // ── 처리 시작 ─────────────────────────────────────────────────────
        String runId = UUID.randomUUID().toString();
        checkpoint.upsertInProgress(dt, runId);

        try {
            // dt 컬럼은 파티션 경로에 포함되므로 Parquet 파일에서 제외
            Dataset<Row> partitionDf = withDt
                .filter(col("dt").equalTo(dt))
                .drop("dt");

            long count = writer.write(partitionDf, dt, runId);
            hiveManager.addPartition(dt, finalPath);
            hiveManager.analyzePartition(dt);
            checkpoint.updateSuccess(dt, count);

            LOG.info("Completed dt={} ({} records)", dt, count);

        } catch (Exception e) {
            LOG.error("Failed dt={}: {}", dt, e.getMessage(), e);
            try {
                checkpoint.updateFailed(dt, e.getMessage());
            } catch (Exception ce) {
                LOG.error("Could not update FAILED status for dt={}: {}", dt, ce.getMessage());
            }
            throw e;
        }
    }
}
