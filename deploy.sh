#!/usr/bin/env bash
set -euo pipefail

USE_SAVEPOINT=false
if [[ "${1:-}" == "--savepoint" ]]; then
  USE_SAVEPOINT=true
fi

SAVEPOINT_PATH=""

echo "==> 1. Checking for existing running Flink jobs..."
RUNNING_JOB=$(docker exec poc-flink-jobmanager flink list -r 2>/dev/null | grep -E "[0-9a-f]{32}" | awk '{print $4}' | head -n 1 || true)

if [ -n "${RUNNING_JOB}" ]; then
  if [ "$USE_SAVEPOINT" = true ]; then
    echo "Found active job [${RUNNING_JOB}]. Taking Savepoint to S3 and stopping job..."
    STOP_OUTPUT=$(docker exec poc-flink-jobmanager flink stop "${RUNNING_JOB}" -p s3://poc-apache-flink-storage-dev/savepoints 2>&1)
    echo "${STOP_OUTPUT}"
    SAVEPOINT_PATH=$(echo "${STOP_OUTPUT}" | grep -oE "s3://[^[:space:]]+" | head -n 1 || true)
    if [ -n "${SAVEPOINT_PATH}" ]; then
      echo "==> State successfully frozen in: ${SAVEPOINT_PATH}"
    else
      echo "==> WARN: Could not extract savepoint path. Continuing deployment without state."
    fi
  else
    echo "Found active job [${RUNNING_JOB}]. Cancelling cleanly (Cold start)..."
    docker exec poc-flink-jobmanager flink cancel "${RUNNING_JOB}" || true
  fi
else
  echo "No active jobs found."
fi

echo "==> 2. Ensuring required Kafka topics exist..."
for topic in movements.incoming movements.currencies config.rules movements.alerts; do
  docker exec poc-flink-kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions 1 \
    --replication-factor 1 >/dev/null 2>&1 || true
done
echo "Kafka topics verified."

echo "==> 3. Packaging latest JAR..."
./mvnw clean package -DskipTests

echo "==> 4. Deploying to Flink cluster..."
docker cp target/poc-apache-flink-1.0.0-SNAPSHOT.jar poc-flink-jobmanager:/opt/flink/job.jar

if [ -n "${SAVEPOINT_PATH}" ]; then
  echo "==> Restoring state from Savepoint [${SAVEPOINT_PATH}]..."
  docker exec poc-flink-jobmanager flink run -d -s "${SAVEPOINT_PATH}" /opt/flink/job.jar
else
  docker exec poc-flink-jobmanager flink run -d /opt/flink/job.jar
fi

echo "==> Done! Job is now running in cluster. Monitor at http://localhost:8081"
