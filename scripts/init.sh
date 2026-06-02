#!/usr/bin/env bash
# 클러스터 초기화 스크립트
# 실행 전: docker-compose up -d 완료 확인
set -e

COMPOSE="docker-compose -f $(dirname "$0")/../docker-compose.yml"

echo "=== 1. HDFS 디렉토리 생성 ==="
docker exec namenode hdfs dfs -mkdir -p /input/ecommerce
docker exec namenode hdfs dfs -mkdir -p /data/ecommerce
docker exec namenode hdfs dfs -mkdir -p /tmp/ecommerce
docker exec namenode hdfs dfs -mkdir -p /user/hive/warehouse

echo "=== 2. HDFS 복제 계수 설정 (단일 DataNode) ==="
docker exec namenode hdfs dfs -setrep -w 1 /

echo "=== 3. Hive DDL 실행 ==="
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000" \
  -f /sql/01_create_database.sql
docker exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000" \
  -f /sql/02_create_external_table.sql

echo "=== 4. CSV 업로드 (13.7GB, 수 분 소요) ==="
echo "    Oct 파일 업로드 중..."
docker exec namenode hdfs dfs -put /mnt/csv/2019-Oct.csv /input/ecommerce/
echo "    Nov 파일 업로드 중..."
docker exec namenode hdfs dfs -put /mnt/csv/2019-Nov.csv /input/ecommerce/

echo ""
echo "=== 초기화 완료 ==="
echo "    Spark UI   : http://localhost:8080"
echo "    HDFS UI    : http://localhost:9870"
echo "    HiveServer2: jdbc:hive2://localhost:10000"
echo ""
echo "다음 명령으로 Spark 잡 실행:"
echo "  docker exec spark-master spark-submit \\"
echo "    --master spark://spark-master:7077 \\"
echo "    --class com.ecommerce.spark.EcommerceProcessor \\"
echo "    /app/ecommerce-spark-1.0.jar \\"
echo "    --input-dir hdfs://namenode:9000/input/ecommerce/ \\"
echo "    --output-dir hdfs://namenode:9000/data/ecommerce/ \\"
echo "    --checkpoint-dir hdfs://namenode:9000/tmp/ecommerce/"
