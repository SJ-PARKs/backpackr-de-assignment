package com.ecommerce.spark.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import static org.apache.spark.sql.functions.*;

public final class KstDateTransformer {

    // UTC event_time을 KST 날짜로 변환해 dt 파티션 컬럼으로 추가
    // 원본 event_time은 변경하지 않음
    public static Dataset<Row> transform(Dataset<Row> df) {
        return df.withColumn("dt",
            date_format(from_utc_timestamp(col("event_time"), "Asia/Seoul"), "yyyy-MM-dd"));
    }

    private KstDateTransformer() {}
}
