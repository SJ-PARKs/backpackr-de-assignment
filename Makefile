include .env

JAR = s3://$(S3_BUCKET)/jars/ecommerce-spark-1.0.jar
INPUT  = s3://$(S3_BUCKET)/input/ecommerce/
OUTPUT = s3://$(S3_BUCKET)/data/ecommerce/
TMP    = s3://$(S3_BUCKET)/tmp/ecommerce/
PG_URL = jdbc:postgresql://$(RDS_ENDPOINT):5432/metastore?ssl=false

# ── 빌드 ──────────────────────────────────────────────────────────────
build:
	mvn clean package -DskipTests

upload-jar:
	aws s3 cp target/ecommerce-spark-1.0.jar s3://$(S3_BUCKET)/jars/ --region $(REGION)

# ── S3 초기 설정 ──────────────────────────────────────────────────────
create-bucket:
	aws s3 mb s3://$(S3_BUCKET) --region $(REGION)

upload-data:
	aws s3 sync data/ s3://$(S3_BUCKET)/input/ecommerce/ \
	  --region $(REGION) --exclude "*" --include "*.csv"

s3-setup: create-bucket upload-data upload-jar

# ── EMR ───────────────────────────────────────────────────────────────
STEP_ARGS = --master,yarn,--deploy-mode,cluster,--class,com.ecommerce.spark.EcommerceProcessor,$(JAR),--input-dir,$(INPUT),--output-dir,$(OUTPUT),--checkpoint-dir,$(TMP),--pg-url,$(PG_URL),--pg-user,$(PG_USER),--pg-password,$(PG_PASSWORD)

submit:
	aws emr add-steps \
	  --cluster-id $(CLUSTER_ID) \
	  --region $(REGION) \
	  --steps "Type=SPARK,Name=EcommerceProcessor,ActionOnFailure=CONTINUE,Args=[$(STEP_ARGS)]"

status:
	@aws emr list-steps \
	  --cluster-id $(CLUSTER_ID) \
	  --region $(REGION) \
	  --query 'Steps[*].[Name,Status.State,Status.Timeline.StartDateTime,Status.Timeline.EndDateTime]' \
	  --output table

cluster-status:
	@aws emr describe-cluster \
	  --cluster-id $(CLUSTER_ID) \
	  --region $(REGION) \
	  --query 'Cluster.[Name,Status.State,Status.StateChangeReason.Message]' \
	  --output table

# ── S3 결과 확인 ──────────────────────────────────────────────────────
check-output:
	aws s3 ls s3://$(S3_BUCKET)/data/ecommerce/ --region $(REGION)

DOCKER_JAR = /app/target/ecommerce-spark-1.0.jar

# ── 로컬 Docker ───────────────────────────────────────────────────────
docker-up:
	docker-compose up -d

docker-down:
	docker-compose down

docker-build:
	docker run --rm -v "$$PWD":/app -w /app maven:3.8-openjdk-8 mvn clean package -DskipTests

docker-submit:
	docker exec spark-master /spark/bin/spark-submit \
	  --master spark://spark-master:7077 \
	  --class com.ecommerce.spark.EcommerceProcessor \
	  $(DOCKER_JAR) \
	  --input-dir  hdfs://namenode:9000/input/ecommerce/ \
	  --output-dir hdfs://namenode:9000/data/ecommerce/ \
	  --checkpoint-dir hdfs://namenode:9000/tmp/ecommerce/ \
	  --pg-url jdbc:postgresql://postgres:5432/metastore \
	  --pg-user $(PG_USER) --pg-password $(PG_PASSWORD)

docker-status:
	@docker exec spark-master curl -s http://spark-master:8080/json/ | \
	  python3 -c "import sys,json; d=json.load(sys.stdin); [print(a['name'],a['status']) for a in d.get('activejobs',[])+d.get('completedjobs',[])]" 2>/dev/null || \
	  echo "Spark UI: http://localhost:8080"

wau:
	docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000 \
	  -f /sql/04_wau_query.sql

# ── Athena ────────────────────────────────────────────────────────────
WAU_QUERY = SELECT date_trunc('WEEK', CAST(dt AS DATE)) AS week_start, COUNT(DISTINCT user_id) AS wau_by_user, COUNT(DISTINCT generated_session_id) AS wau_by_session, CASE WHEN date_trunc('WEEK', CAST(dt AS DATE)) < DATE '2019-10-01' OR date_add('day', 6, date_trunc('WEEK', CAST(dt AS DATE))) > DATE '2019-12-01' THEN 'PARTIAL' ELSE 'FULL' END AS week_type FROM ecommerce.events GROUP BY date_trunc('WEEK', CAST(dt AS DATE)) ORDER BY week_start

athena-wau:
	$(eval QID := $(shell aws athena start-query-execution \
	  --query-string "$(WAU_QUERY)" \
	  --query-execution-context Database=ecommerce \
	  --result-configuration OutputLocation=$(ATHENA_RESULTS) \
	  --region $(REGION) \
	  --query 'QueryExecutionId' --output text))
	@echo "Query ID: $(QID)"
	@until [ "$$(aws athena get-query-execution --query-execution-id $(QID) --region $(REGION) --query 'QueryExecution.Status.State' --output text)" != "RUNNING" ]; do sleep 3; done
	@aws athena get-query-results \
	  --query-execution-id $(QID) \
	  --region $(REGION) \
	  --query 'ResultSet.Rows[*].Data[*].VarCharValue' \
	  --output table

.PHONY: build create-bucket upload-data upload-jar s3-setup \
        submit status cluster-status check-output \
        docker-up docker-down docker-build docker-submit docker-status \
        wau athena-wau
