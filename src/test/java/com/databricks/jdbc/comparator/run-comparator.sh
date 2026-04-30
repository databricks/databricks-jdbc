#!/bin/bash
# ============================================================================
# JDBC Driver Comparator — Standalone Runner
#
# This script pulls the latest code, merges main into comparator-v2,
# runs the comparator, and outputs logs + report.
#
# Usage:
#   1. Update DATABRICKS_COMPARATOR_TOKEN below with your PAT
#   2. chmod +x run-comparator.sh
#   3. ./run-comparator.sh
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
DATABRICKS_COMPARATOR_TOKEN="dapi..."  # Replace with your token

# Comparison axis — choose ONE of:
#
# (A) Legacy single-warehouse Thrift-vs-SEA mode
#     Set COMPARATOR_WAREHOUSE; leave LEFT_*/RIGHT_* empty.
#
# (B) Generic two-endpoint mode
#     Set both LEFT_* and RIGHT_*; COMPARATOR_WAREHOUSE is ignored.
#     Each side resolves its httpPath via this precedence (first match wins):
#       <SIDE>_HTTP_PATH > <SIDE>_CLUSTER (orgId:clusterId) > <SIDE>_WAREHOUSE
#     <SIDE>_TRANSPORT defaults to "sea" (alternative: "thrift").
#     <SIDE>_LABEL defaults to <SIDE>-<TRANSPORT> uppercased.
#
# Example: SEA-vs-SEA across two warehouses
#   LEFT_WAREHOUSE="33f48d57a0dc69f9"; LEFT_LABEL="DBR-SEA"
#   RIGHT_WAREHOUSE="000000000000000d"; RIGHT_LABEL="Reyden-SEA"
#
# Example: warehouse vs interactive cluster
#   LEFT_WAREHOUSE="abc123"
#   RIGHT_CLUSTER="1234567890:0413-104341-eajdv7uv"
#   RIGHT_TRANSPORT="thrift"
#
COMPARATOR_WAREHOUSE="6feab30b476abfa4"  # legacy mode default

LEFT_WAREHOUSE=""
LEFT_CLUSTER=""
LEFT_HTTP_PATH=""
LEFT_TRANSPORT=""
LEFT_LABEL=""

RIGHT_WAREHOUSE=""
RIGHT_CLUSTER=""
RIGHT_HTTP_PATH=""
RIGHT_TRANSPORT=""
RIGHT_LABEL=""

# Pro warehouse (leave empty to skip; runs the PRO_WAREHOUSE config against this third warehouse)
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
METADATA_SKIP_METHODS=""  # comma-separated methods to skip (e.g., getFunctions)
METADATA_SKIP_SCHEMAS="information_schema,global_temp"
METADATA_PARALLEL_THREADS="40"
SKIP_DIFF_PATTERNS=""  # pipe-separated patterns to exclude from report (e.g., "DATA_TYPE mismatch: 0 (Integer) vs 12 (Integer)")

# Workspace setup (empty = skip, "recreate" = drop + create all test data)
WORKSPACE_SETUP=""

# Git repo
REPO_URL="https://github.com/databricks/databricks-jdbc.git"
MERGE_BRANCH="main"  # branch to merge into comparator-v2 (e.g., main, feature-branch)
WORK_DIR="/tmp/jdbc-comparator-$$"

# Run name (used in log/report filenames for easy identification, empty = generic name)
RUN_NAME=""

# ============================================================================
# FILTER CONFIG — edit to skip known noisy argument combinations
# ============================================================================

FILTER_JSON='{
  "metadataSkipFilters": {
    "getTables": [
      {"schemaPattern": ""},
      {"types": "[]"},
      {"catalog": "compar%", "schemaPattern": "nonexistent"}
    ]
  }
}'

# ============================================================================
# DO NOT EDIT BELOW THIS LINE
# ============================================================================

set -e
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
if [ -n "${RUN_NAME}" ]; then
  LOG_FILE="${RUN_NAME}-logs-${TIMESTAMP}.txt"
  REPORT_FILE="${RUN_NAME}-report-${TIMESTAMP}.txt"
else
  LOG_FILE="comparator-logs-${TIMESTAMP}.txt"
  REPORT_FILE="comparator-report-${TIMESTAMP}.txt"
fi
FILTER_FILE="${WORK_DIR}/metadata-filters.json"

# Detect generic vs legacy axis and emit a one-line summary of each side.
ANY_LEFT_RIGHT="${LEFT_WAREHOUSE}${LEFT_CLUSTER}${LEFT_HTTP_PATH}${RIGHT_WAREHOUSE}${RIGHT_CLUSTER}${RIGHT_HTTP_PATH}"
echo "=== JDBC Driver Comparator ==="
echo "Timestamp: ${TIMESTAMP}"
echo "Workspace: ${COMPARATOR_HOST}"
if [ -n "${ANY_LEFT_RIGHT}" ]; then
  echo "LEFT  : ${LEFT_LABEL:-<auto>} | warehouse=${LEFT_WAREHOUSE:--} cluster=${LEFT_CLUSTER:--} httpPath=${LEFT_HTTP_PATH:--} transport=${LEFT_TRANSPORT:-sea}"
  echo "RIGHT : ${RIGHT_LABEL:-<auto>} | warehouse=${RIGHT_WAREHOUSE:--} cluster=${RIGHT_CLUSTER:--} httpPath=${RIGHT_HTTP_PATH:--} transport=${RIGHT_TRANSPORT:-sea}"
else
  echo "Warehouse: ${COMPARATOR_WAREHOUSE} (legacy Thrift-vs-SEA mode)"
fi
echo "Config: ${CONNECTION_CONFIG:-all}"
echo "Output: ${LOG_FILE}, ${REPORT_FILE}"
echo ""

# Capture current directory before cd
ORIGINAL_DIR="$(pwd)"

# Clone and prepare
echo "[1/5] Cloning repository..."
git clone --branch comparator-v2 "${REPO_URL}" "${WORK_DIR}"
cd "${WORK_DIR}"

echo "[2/5] Merging ${MERGE_BRANCH} into comparator-v2..."
git fetch origin "${MERGE_BRANCH}"
git merge "origin/${MERGE_BRANCH}" --no-commit --no-ff -X theirs

# Write filter config
echo "[3/5] Writing filter config..."
echo "${FILTER_JSON}" > "${FILTER_FILE}"

# Build Maven command
echo "[4/5] Running comparator..."
export DATABRICKS_COMPARATOR_TOKEN
export COMPARATOR_HOST
export COMPARATOR_WAREHOUSE

MVN_ARGS="-pl jdbc-core -Dtest=JDBCDriverComparisonTest"

if [ -n "${COMPARATOR_HOST}" ]; then
  MVN_ARGS="${MVN_ARGS} -DCOMPARATOR_HOST=${COMPARATOR_HOST}"
fi
if [ -n "${COMPARATOR_WAREHOUSE}" ]; then
  MVN_ARGS="${MVN_ARGS} -DCOMPARATOR_WAREHOUSE=${COMPARATOR_WAREHOUSE}"
fi

# Pass through LEFT_*/RIGHT_* generic-axis properties when set.
for var in LEFT_WAREHOUSE LEFT_CLUSTER LEFT_HTTP_PATH LEFT_TRANSPORT LEFT_LABEL \
           RIGHT_WAREHOUSE RIGHT_CLUSTER RIGHT_HTTP_PATH RIGHT_TRANSPORT RIGHT_LABEL; do
  val="${!var}"
  if [ -n "${val}" ]; then
    MVN_ARGS="${MVN_ARGS} -D${var}=${val}"
  fi
done

if [ -n "${CONNECTION_CONFIG}" ]; then
  MVN_ARGS="${MVN_ARGS} -DCONNECTION_CONFIG=${CONNECTION_CONFIG}"
fi
if [ -n "${SUITES_RUN_ONLY}" ]; then
  MVN_ARGS="${MVN_ARGS} -DSUITES_RUN_ONLY=${SUITES_RUN_ONLY}"
fi
if [ -n "${METADATA_RUN_ONLY_METHODS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_RUN_ONLY_METHODS=${METADATA_RUN_ONLY_METHODS}"
fi
if [ -n "${METADATA_SKIP_METHODS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_SKIP_METHODS=${METADATA_SKIP_METHODS}"
fi
if [ -n "${METADATA_SKIP_SCHEMAS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_SKIP_SCHEMAS=${METADATA_SKIP_SCHEMAS}"
fi
if [ -n "${METADATA_PARALLEL_THREADS}" ]; then
  MVN_ARGS="${MVN_ARGS} -DMETADATA_PARALLEL_THREADS=${METADATA_PARALLEL_THREADS}"
fi
if [ -n "${SKIP_DIFF_PATTERNS}" ]; then
  SKIP_DIFF_ARG="-DSKIP_DIFF_PATTERNS=${SKIP_DIFF_PATTERNS}"
fi
if [ -n "${PRO_WAREHOUSE_ID}" ]; then
  MVN_ARGS="${MVN_ARGS} -DPRO_WAREHOUSE_ID=${PRO_WAREHOUSE_ID}"
fi
if [ -n "${WORKSPACE_SETUP}" ]; then
  MVN_ARGS="${MVN_ARGS} -DWORKSPACE_SETUP=${WORKSPACE_SETUP}"
fi

MVN_ARGS="${MVN_ARGS} -DMETADATA_FILTER_CONFIG=${FILTER_FILE}"

echo "Command: mvn test ${MVN_ARGS} ${SKIP_DIFF_ARG:+\"$SKIP_DIFF_ARG\"}"
echo ""

# Run (don't exit on Maven failure — we still need to collect output)
set +e
mvn test ${MVN_ARGS} ${SKIP_DIFF_ARG:+"$SKIP_DIFF_ARG"} 2>&1 | tee "${WORK_DIR}/full-output.txt"
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

# Copy CSV results
CSV_FILE=$(ls ${WORK_DIR}/jdbc-core/jdbc-comparison-results-*.csv 2>/dev/null | head -1)
if [ -n "${CSV_FILE}" ]; then
  CSV_DEST="${ORIGINAL_DIR}/${RUN_NAME}-results-${TIMESTAMP}.csv"
  cp "${CSV_FILE}" "${CSV_DEST}"
  CSV_LINES=$(wc -l < "${CSV_DEST}")
  echo "CSV: ${CSV_DEST} (${CSV_LINES} rows)"
else
  echo "No CSV file generated"
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
