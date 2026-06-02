package com.ecommerce.spark.writer;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.spark.sql.functions.*;

public final class SkewDetector {

    private static final Logger LOG = LoggerFactory.getLogger(SkewDetector.class);
    private static final int TOP_N = 10;

    // user별 이벤트 수 분포 로깅 + threshold 초과 user 경고
    // salting은 Window partitionBy를 깨므로 적용하지 않고, 병목 후보를 로그로 남김
    // 이 호출이 persist() 이후 첫 번째 액션이 되어 캐시를 채움
    public static void detect(Dataset<Row> df, int threshold) {
        LOG.info("Running skew detection (threshold={})", threshold);

        Dataset<Row> userCounts = df
            .groupBy("user_id")
            .agg(count("*").alias("event_count"));

        Row[] topUsers = (Row[]) userCounts
            .orderBy(col("event_count").desc())
            .limit(TOP_N)
            .collect();

        LOG.info("Top {} users by event count:", TOP_N);
        for (Row r : topUsers) {
            LOG.info("  user_id={}, events={}", r.getLong(0), r.getLong(1));
        }

        long skewedUsers = userCounts.filter(col("event_count").geq(threshold)).count();
        if (skewedUsers > 0) {
            LOG.warn("{} users exceed skew threshold of {} events. Review window partition skew if this job is slow.",
                skewedUsers, threshold);
        } else {
            LOG.info("No users exceed skew threshold of {}", threshold);
        }
    }

    private SkewDetector() {}
}
