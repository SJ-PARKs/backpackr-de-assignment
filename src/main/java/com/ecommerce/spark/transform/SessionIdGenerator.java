package com.ecommerce.spark.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;

import static org.apache.spark.sql.functions.*;

public final class SessionIdGenerator {

    private static final long SESSION_TIMEOUT_SECONDS = 300L;

    // Window 기반 세션 ID 생성
    // 동일 user_id 파티션 안에서 LAG + LAST로 세션 시작 시각을 전파
    // 세션 포맷: {user_id}_{yyyyMMddHHmmss}
    public static Dataset<Row> generate(Dataset<Row> df) {
        // lag는 offset 기반으로 동작하므로 frame 없는 WindowSpec에 적용
        WindowSpec wOrder = Window
            .partitionBy("user_id")
            .orderBy(col("event_time").asc(),
                     col("event_type").asc(),
                     col("product_id").asc());

        // last(ignoreNulls)는 집계 함수 - 명시적 frame 필요
        WindowSpec w = wOrder
            .rowsBetween(Window.unboundedPreceding(), Window.currentRow());

        // 이전 이벤트의 epoch (seconds) - lag은 wOrder(frame 없음)에 적용
        org.apache.spark.sql.Column prevEpoch =
            lag(unix_timestamp(col("event_time")), 1).over(wOrder);

        // 새 세션 조건: 첫 이벤트이거나 gap >= 300s
        org.apache.spark.sql.Column isNewSession = prevEpoch.isNull().or(
            unix_timestamp(col("event_time")).minus(prevEpoch).geq(SESSION_TIMEOUT_SECONDS));

        // 세션 시작 시각 (새 세션이 아니면 null)
        org.apache.spark.sql.Column sessionStartFlag =
            when(isNewSession, col("event_time")).otherwise(null);

        // LAST(ignoreNulls=true): 현재 행까지 가장 최근 세션 시작 시각 전파
        org.apache.spark.sql.Column lastSessionStart = last(sessionStartFlag, true).over(w);

        // {user_id}_{yyyyMMddHHmmss}
        org.apache.spark.sql.Column sessionId = concat(
            col("user_id").cast(DataTypes.StringType),
            lit("_"),
            date_format(lastSessionStart, "yyyyMMddHHmmss")
        );

        return df.withColumn("generated_session_id", sessionId);
    }

    private SessionIdGenerator() {}
}
