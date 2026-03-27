#!/bin/bash
# ============================================================================
# JDBC Driver Comparator — Standalone Runner
#
# This script pulls the latest code, merges main into comparator-v2,
# runs the comparator, and outputs logs + report.
#
# Usage:
#   chmod +x run-comparator.sh
#   ./run-comparator.sh
#
# Output:
#   comparator-logs-<timestamp>.txt
#   comparator-report-<timestamp>.txt
# ============================================================================

# ============================================================================
# CONFIGURATION — edit these values
# ============================================================================

# Workspace
COMPARATOR_HOST="adb-7405613695221181.1.azuredatabricks.net"
COMPARATOR_WAREHOUSE="6feab30b476abfa4"
DATABRICKS_COMPARATOR_TOKEN="dapi..."  # Replace with your token

# Pro warehouse (leave empty to skip)
PRO_WAREHOUSE_ID=""

# Connection configs to run (comma-separated, empty = all)
#
# Config                     | Suites it runs
# ---------------------------+------------------------------------------------
# DEFAULT_PARAMS             | SELECT, SELECT_TRUNCATED, DDL, DML, OTHER,
#                            | PREPARED_TYPES, PREPARED_METADATA, NULL_HANDLING,
#                            | DATABASE_METADATA
# COMPRESSION_DISABLED       | SELECT
# DIRECT_RESULTS_DISABLED    | SELECT
# COMPLEX_TYPES_ENABLED      | COMPLEX_TYPES
# COMPLEX_TYPES_DISABLED     | COMPLEX_TYPES
# GEOSPATIAL_ENABLED         | GEOSPATIAL
# GEOSPATIAL_DISABLED        | GEOSPATIAL
# USE_QUERY_FOR_METADATA     | DATABASE_METADATA
# VOLUME_OPERATIONS          | VOLUME_OPERATIONS
# PRO_WAREHOUSE              | SELECT, COMPLEX_TYPES, GEOSPATIAL (requires PRO_WAREHOUSE_ID)
#
CONNECTION_CONFIG="DEFAULT_PARAMS"

# Test suites to run (comma-separated, empty = all suites for the config)
# Options: STATEMENT_SELECT, STATEMENT_SELECT_TRUNCATED, STATEMENT_DDL, STATEMENT_DML,
#          STATEMENT_OTHER, PREPARED_STATEMENT_TYPES, PREPARED_STATEMENT_METADATA,
#          COMPLEX_TYPES, GEOSPATIAL, NULL_HANDLING, VOLUME_OPERATIONS, DATABASE_METADATA
SUITES_RUN_ONLY="DATABASE_METADATA"

# DatabaseMetaData options
# Available methods: getCatalogs, getSchemas, getTables, getColumns,
#   getPrimaryKeys, getImportedKeys, getExportedKeys, getCrossReference, getFunctions
METADATA_RUN_ONLY_METHODS="getCatalogs,getSchemas"  # empty = all methods
METADATA_SKIP_SCHEMAS="information_schema,global_temp"
METADATA_PARALLEL_THREADS="40"

# Workspace setup (empty = skip, "recreate" = drop + create all test data)
WORKSPACE_SETUP=""

# Git repo
REPO_URL="https://github.com/databricks/databricks-jdbc.git"
WORK_DIR="/tmp/jdbc-comparator-$$"

# ============================================================================
# FILTER CONFIG — edit to skip known noisy argument combinations
# ============================================================================

FILTER_JSON='{
  "metadataSkipFilters": {
    "getTables": [
      {"schemaPattern": ""},
      {"types": "[]"},
      {"catalog": "comp%", "schemaPattern": "nonexistent"}
    ]
  }
}'

# ============================================================================
# DO NOT EDIT BELOW THIS LINE
# ============================================================================

set -e
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
LOG_FILE="comparator-logs-${TIMESTAMP}.txt"
REPORT_FILE="comparator-report-${TIMESTAMP}.txt"
FILTER_FILE="${WORK_DIR}/metadata-filters.json"

echo "=== JDBC Driver Comparator ==="
echo "Timestamp: ${TIMESTAMP}"
echo "Workspace: ${COMPARATOR_HOST}"
echo "Warehouse: ${COMPARATOR_WAREHOUSE}"
echo "Config: ${CONNECTION_CONFIG:-all}"
echo "Output: ${LOG_FILE}, ${REPORT_FILE}"
echo ""

# Capture current directory before cd
ORIGINAL_DIR="$(pwd)"

# Clone and prepare
echo "[1/5] Cloning repository..."
git clone --branch comparator-v2 "${REPO_URL}" "${WORK_DIR}"
cd "${WORK_DIR}"

echo "[2/5] Merging main into comparator-v2..."
git fetch origin main
git merge origin/main --no-commit --no-ff -X theirs

# Write filter config
echo "[3/5] Writing filter config..."
echo "${FILTER_JSON}" > "${FILTER_FILE}"

# Build Maven command
echo "[4/5] Running comparator..."
export DATABRICKS_COMPARATOR_TOKEN
export COMPARATOR_HOST
export COMPARATOR_WAREHOUSE

MVN_ARGS="-pl jdbc-core -Dtest=JDBCDriverComparisonTest"

if [ -n "${CONNECTION_CONFIG}" ]; then
  MVN_ARGS="${MVN_ARGS} -DCONNECTION_CONFIG=${CONNECTION_CONFIG}"
fi
if [ -n "${SUITES_RUN_ONLY}" ]; then
  MVN_ARGS="${MVN_ARGS} -DSUITES_RUN_ONLY=${SUITES_RUN_ONLY}"
fi
if [ -n "${METADATA_RUN_ONLY_METHODS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_RUN_ONLY_METHODS=${METADATA_RUN_ONLY_METHODS}"
fi
if [ -n "${METADATA_SKIP_SCHEMAS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_SKIP_SCHEMAS=${METADATA_SKIP_SCHEMAS}"
fi
if [ -n "${METADATA_PARALLEL_THREADS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_PARALLEL_THREADS=${METADATA_PARALLEL_THREADS}"
fi
if [ -n "${PRO_WAREHOUSE_ID}" ]; then
  MVN_ARGS="${MVN_ARGS} -DPRO_WAREHOUSE_ID=${PRO_WAREHOUSE_ID}"
fi
if [ -n "${WORKSPACE_SETUP}" ]; then
  MVN_ARGS="${MVN_ARGS} -DWORKSPACE_SETUP=${WORKSPACE_SETUP}"
fi

MVN_ARGS="${MVN_ARGS} -DMETADATA_FILTER_CONFIG=${FILTER_FILE}"

echo "Command: mvn test ${MVN_ARGS}"
echo ""

# Run (don't exit on Maven failure — we still need to collect output)
set +e
mvn test ${MVN_ARGS} 2>&1 | tee "${WORK_DIR}/full-output.txt"
MVN_EXIT=${PIPESTATUS[0]}
set -e

# Collect output
echo ""
echo "[5/5] Collecting output..."

# Copy logs
cp "${WORK_DIR}/full-output.txt" "${ORIGINAL_DIR}/${LOG_FILE}"

# Copy report
REPORT=$(ls ${WORK_DIR}/jdbc-core/jdbc-comparison-report-*.txt 2>/dev/null | head -1)
if [ -n "${REPORT}" ]; then
  cp "${REPORT}" "${ORIGINAL_DIR}/${REPORT_FILE}"
  REPORT_LINES=$(wc -l < "${ORIGINAL_DIR}/${REPORT_FILE}")
  echo "Report: ${ORIGINAL_DIR}/${REPORT_FILE} (${REPORT_LINES} lines)"
else
  echo "No report file generated"
fi

echo "Logs: ${ORIGINAL_DIR}/${LOG_FILE}"

# Cleanup
echo ""
echo "Cleaning up work directory..."
rm -rf "${WORK_DIR}"

# Summary
echo ""
echo "=== Done ==="
echo "Report: ${REPORT_FILE}"
echo "Logs: ${LOG_FILE}"
