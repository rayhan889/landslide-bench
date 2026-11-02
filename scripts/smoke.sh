#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
set -a; source .env; set +a
DC="docker compose"

echo "[1/4] HDFS has a live datanode"
$DC exec -T namenode hdfs dfsadmin -report | grep 'Live datanodes (1)' >/dev/null

echo "[2/4] Spark master sees a worker"
curl -fs http://localhost:8080/json/ | grep -E '"aliveworkers"[[:space:]]*:[[:space:]]*[1-9]' >/dev/null

echo "[3/4] Spark job: driver/worker versions match, h3 UDF, Parquet round-trip on HDFS"
$DC exec -T spark-master /opt/spark/bin/spark-submit /workspace/scripts/smoke_spark.py

echo "[4/4] ClickHouse reads the same Parquet through hdfs()"
CH="$DC exec -T clickhouse clickhouse-client --user $CLICKHOUSE_USER --password $CLICKHOUSE_PASSWORD"
[ "$($CH -q "SELECT value FROM system.build_options WHERE name = 'USE_HDFS'")" = "1" ] \
  || { echo "ClickHouse build has no HDFS support (USE_HDFS != 1)"; exit 1; }
n=$($CH -q "SELECT count() FROM hdfs('hdfs://namenode:8020/tmp/smoke/*.parquet', 'Parquet')")
[ "$n" = "1000" ] || { echo "expected 1000 rows, ClickHouse read $n"; exit 1; }

echo "OK: stack is ready"
