#!/usr/bin/env bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
LIB_DIR="${SCRIPT_DIR}/../lib"

mkdir -p "${LIB_DIR}"

echo "Downloading PyFlink Connector JARs to ${LIB_DIR}..."

KAFKA_JAR_URL="https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.0.1-1.18/flink-sql-connector-kafka-3.0.1-1.18.jar"
curl -s -L -o "${LIB_DIR}/flink-sql-connector-kafka-3.0.1-1.18.jar" "${KAFKA_JAR_URL}"
echo "✓ Kafka connector downloaded."

POSTGRES_JAR_URL="https://jdbc.postgresql.org/download/postgresql-42.6.0.jar"
curl -s -L -o "${LIB_DIR}/postgresql-42.6.0.jar" "${POSTGRES_JAR_URL}"
echo "✓ PostgreSQL JDBC Driver downloaded."

echo "All dependency JARs successfully retrieved."
