package com.ecommerce.spark.config;

import org.apache.spark.sql.SparkSession;

public class AppConfig {

    private String inputDir;
    private String outputDir;
    private String checkpointDir;
    private String startDate;   // nullable - KST YYYY-MM-DD
    private String endDate;     // nullable - KST YYYY-MM-DD
    private int skewThreshold = 10_000;

    private String pgJdbcUrl  = "jdbc:postgresql://postgres:5432/metastore";
    private String pgUser     = "hive";
    private String pgPassword = "hive";

    public static AppConfig parse(String[] args) {
        AppConfig cfg = new AppConfig();
        for (int i = 0; i < args.length - 1; i++) {
            switch (args[i]) {
                case "--input-dir":
                    cfg.inputDir = args[++i];
                    break;
                case "--output-dir":
                    cfg.outputDir = args[++i];
                    break;
                case "--checkpoint-dir":
                    cfg.checkpointDir = args[++i];
                    break;
                case "--start-date":
                    cfg.startDate = args[++i];
                    break;
                case "--end-date":
                    cfg.endDate = args[++i];
                    break;
                case "--skew-threshold":
                    cfg.skewThreshold = Integer.parseInt(args[++i]);
                    break;
                case "--pg-url":
                    cfg.pgJdbcUrl = args[++i];
                    break;
                case "--pg-user":
                    cfg.pgUser = args[++i];
                    break;
                case "--pg-password":
                    cfg.pgPassword = args[++i];
                    break;
                default:
                    break;
            }
        }
        if (cfg.inputDir == null || cfg.outputDir == null || cfg.checkpointDir == null) {
            throw new IllegalArgumentException(
                "Required: --input-dir, --output-dir, --checkpoint-dir");
        }
        return cfg;
    }

    public SparkSession buildSparkSession() {
        return SparkSession.builder()
            .appName("EcommerceProcessor")
            // Adaptive Query Execution
            .config("spark.sql.adaptive.enabled",              "true")
            .config("spark.sql.adaptive.skewJoin.enabled",     "true")
            // Parquet 설정
            .config("spark.sql.parquet.compression.codec",          "snappy")
            .config("spark.sql.sources.partitionOverwriteMode",     "dynamic")
            .config("spark.sql.parquet.mergeSchema",                "false")
            .config("spark.sql.parquet.filterPushdown",             "true")
            .config("spark.sql.hive.convertMetastoreParquet",       "true")
            .config("spark.hadoop.parquet.enable.summary-metadata", "false")
            // "2019-10-01 00:00:00 UTC" 형식 파싱을 위한 레거시 파서
            .config("spark.sql.legacy.timeParserPolicy",            "LEGACY")
            // S3A - EMR 환경에서는 IAM Role로 인증 (로컬 Docker에서는 무시됨)
            .config("spark.hadoop.fs.s3a.impl",
                    "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "com.amazonaws.auth.InstanceProfileCredentialsProvider")
            .config("spark.hadoop.fs.s3a.fast.upload",      "true")
            .config("spark.hadoop.fs.s3a.multipart.size",   "134217728")  // 128MB
            .enableHiveSupport()
            .getOrCreate();
    }

    public boolean isInRange(String dt) {
        if (startDate != null && dt.compareTo(startDate) < 0) return false;
        if (endDate   != null && dt.compareTo(endDate)   > 0) return false;
        return true;
    }

    public String getInputDir() { return inputDir; }
    public String getOutputDir() { return outputDir; }
    public String getCheckpointDir() { return checkpointDir; }
    public int getSkewThreshold() { return skewThreshold; }
    public String getPgJdbcUrl() { return pgJdbcUrl; }
    public String getPgUser() { return pgUser; }
    public String getPgPassword() { return pgPassword; }
}
