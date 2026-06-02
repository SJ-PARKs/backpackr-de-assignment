package com.ecommerce.spark.writer;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

public class PartitionWriter {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionWriter.class);

    private final SparkSession spark;
    private final String outputDir;
    private final String checkpointDir;

    public PartitionWriter(SparkSession spark, String outputDir, String checkpointDir) {
        this.spark         = spark;
        this.outputDir     = outputDir.replaceAll("/$", "");
        this.checkpointDir = checkpointDir.replaceAll("/$", "");
    }

    // dt 컬럼은 호출 전에 이미 drop된 상태여야 함
    public long write(Dataset<Row> df, String dt, String runId) throws IOException {
        String finalPath = outputDir + "/dt=" + dt;
        long count = df.count();
        LOG.info("dt={}: {} rows → writing", dt, count);

        if (isS3(outputDir)) {
            // S3: Spark가 내부적으로 _temporary 스테이징 처리 → 직접 쓰기
            df.repartition(2)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(finalPath);
        } else {
            // HDFS: tmp→rename으로 쓰기 중에도 기존 파티션 쿼리 가용성 유지
            String tmpPath = checkpointDir + "/" + runId + "/dt=" + dt;
            String oldPath = outputDir + "/dt=" + dt + "_old_" + runId;
            df.repartition(2)
                .write()
                .mode(SaveMode.Overwrite)
                .parquet(tmpPath);
            LOG.info("dt={}: tmp write done, renaming to final", dt);
            atomicRenameHdfs(tmpPath, finalPath, oldPath);
        }

        return count;
    }

    private void atomicRenameHdfs(String tmpPath, String finalPath, String oldPath) throws IOException {
        FileSystem fs = FileSystem.get(
            URI.create(outputDir), spark.sparkContext().hadoopConfiguration());

        Path tmp = new Path(tmpPath);
        Path fin = new Path(finalPath);
        Path old = new Path(oldPath);

        if (fs.exists(fin)) {
            if (!fs.rename(fin, old)) {
                throw new IOException("Failed to rename final→old: " + finalPath);
            }
        }
        if (!fs.rename(tmp, fin)) {
            if (fs.exists(old)) fs.rename(old, fin);
            throw new IOException("Failed to rename tmp→final: " + tmpPath);
        }
        if (fs.exists(old)) {
            fs.delete(old, true);
        }
    }

    private static boolean isS3(String path) {
        return path.startsWith("s3://") || path.startsWith("s3a://");
    }
}
