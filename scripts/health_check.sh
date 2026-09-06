#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────
# RTIS Infrastructure Health Check
# Verifies Kafka, TimescaleDB, and the telemetry topic are ready.
# ──────────────────────────────────────────────────────────────
set -euo pipefail

# Load .env if present
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
ENV_FILE="${SCRIPT_DIR}/../.env"
if [[ -f "${ENV_FILE}" ]]; then
    set -a; source "${ENV_FILE}"; set +a
fi

KAFKA_HOST="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
KAFKA_HOST_ONLY="${KAFKA_HOST%%:*}"
KAFKA_PORT="${KAFKA_HOST##*:}"
TOPIC="${KAFKA_TOPIC:-rtis-telemetry}"

PG_HOST="${POSTGRES_HOST:-localhost}"
PG_PORT="${POSTGRES_PORT:-5432}"
PG_DB="${POSTGRES_DB:-ir_eta_db}"
PG_USER="${POSTGRES_USER:-postgres}"

PASS=0
FAIL=0

check() {
    local label="$1"; shift
    if "$@" >/dev/null 2>&1; then
        echo "  ✓  ${label}"
        ((PASS++))
    else
        echo "  ✗  ${label}"
        ((FAIL++))
    fi
}

echo ""
echo "═══════════════════════════════════════"
echo "  RTIS Infrastructure Health Check"
echo "═══════════════════════════════════════"
echo ""

# ── Kafka ──────────────────────────────────
echo "▸ Kafka (${KAFKA_HOST})"
check "Broker reachable" nc -z -w 3 "${KAFKA_HOST_ONLY}" "${KAFKA_PORT}"

# Try to verify topic exists (requires kafka CLI or kcat)
if command -v kcat &>/dev/null; then
    check "Topic '${TOPIC}' exists" kcat -b "${KAFKA_HOST}" -L -t "${TOPIC}" -J
elif command -v kafka-topics &>/dev/null; then
    check "Topic '${TOPIC}' exists" kafka-topics --bootstrap-server "${KAFKA_HOST}" --describe --topic "${TOPIC}"
else
    echo "  ⊘  Topic check skipped (install kcat or kafka-topics CLI)"
fi

echo ""

# ── TimescaleDB ────────────────────────────
echo "▸ TimescaleDB (${PG_HOST}:${PG_PORT}/${PG_DB})"

if command -v pg_isready &>/dev/null; then
    check "PostgreSQL accepting connections" pg_isready -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}"
else
    check "Port reachable" nc -z -w 3 "${PG_HOST}" "${PG_PORT}"
fi

# Verify train_telemetry table exists
if command -v psql &>/dev/null; then
    check "Table 'train_telemetry' exists" \
        psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
             -c "SELECT 1 FROM train_telemetry LIMIT 0;"
    check "TimescaleDB extension loaded" \
        psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
             -c "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';"
    check "PostGIS extension loaded" \
        psql -h "${PG_HOST}" -p "${PG_PORT}" -U "${PG_USER}" -d "${PG_DB}" \
             -c "SELECT extversion FROM pg_extension WHERE extname = 'postgis';"
else
    echo "  ⊘  Table/extension checks skipped (psql not found)"
fi

echo ""
echo "───────────────────────────────────────"
echo "  Results: ${PASS} passed, ${FAIL} failed"
echo "───────────────────────────────────────"
echo ""

[[ ${FAIL} -eq 0 ]]
