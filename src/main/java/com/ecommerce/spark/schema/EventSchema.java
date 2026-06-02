package com.ecommerce.spark.schema;

import org.apache.spark.sql.types.*;

public final class EventSchema {

    // CSV 원본 스키마 (dt, generated_session_id 제외 - 파이프라인에서 추가됨)
    public static final StructType SCHEMA = new StructType()
        .add("event_time",    DataTypes.TimestampType, true)
        .add("event_type",    DataTypes.StringType,    false)
        .add("product_id",    DataTypes.IntegerType,   true)
        .add("category_id",   DataTypes.LongType,      true)   // INT 범위 초과 값 존재
        .add("category_code", DataTypes.StringType,    true)   // nullable ~32.6%
        .add("brand",         DataTypes.StringType,    true)   // nullable ~14.4%
        .add("price",         DataTypes.DoubleType,    true)
        .add("user_id",       DataTypes.LongType,      false)
        .add("user_session",  DataTypes.StringType,    true);

    private EventSchema() {}
}
