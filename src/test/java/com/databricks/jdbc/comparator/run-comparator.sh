#!/bin/bash
# ============================================================================
# JDBC Driver Comparator — Standalone Runner
#
# This script pulls the latest code, merges main into comparator-v2,
# runs the comparator, and outputs logs + report.
#
# Usage (local):
#   export DATABRICKS_COMPARATOR_TOKEN=dapi...
#   export COMPARATOR_HOST=adb-xxx.azuredatabricks.net
#   export COMPARATOR_WAREHOUSE=<warehouse-id>            # legacy mode
#   # OR set LEFT_*/RIGHT_* for generic two-endpoint mode (see below)
#   chmod +x run-comparator.sh
#   ./run-comparator.sh
#
# Usage (CI): invoked by .github/workflows/runJdbcComparator.yml — env vars
#   come from secrets (DATABRICKS_COMPARATOR_TOKEN) and repo Variables
#   (COMPARATOR_HOST, COMPARATOR_WAREHOUSE, PRO_WAREHOUSE_ID).
#
# Output (in $PWD at script start):
#   <run-name>-logs-<timestamp>.txt    (or comparator-logs-*.txt if RUN_NAME unset)
#   <run-name>-report-<timestamp>.txt
#   <run-name>-results-<timestamp>.csv
# ============================================================================

# ============================================================================
# CONFIGURATION — override via env, or edit defaults below
# ============================================================================

# Workspace — REQUIRED, must be set in env (no defaults)
COMPARATOR_HOST="${COMPARATOR_HOST:-}"
DATABRICKS_COMPARATOR_TOKEN="${DATABRICKS_COMPARATOR_TOKEN:-}"

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
COMPARATOR_WAREHOUSE="${COMPARATOR_WAREHOUSE:-}"  # legacy mode

LEFT_WAREHOUSE="${LEFT_WAREHOUSE:-}"
LEFT_CLUSTER="${LEFT_CLUSTER:-}"
LEFT_HTTP_PATH="${LEFT_HTTP_PATH:-}"
LEFT_TRANSPORT="${LEFT_TRANSPORT:-}"
LEFT_LABEL="${LEFT_LABEL:-}"

RIGHT_WAREHOUSE="${RIGHT_WAREHOUSE:-}"
RIGHT_CLUSTER="${RIGHT_CLUSTER:-}"
RIGHT_HTTP_PATH="${RIGHT_HTTP_PATH:-}"
RIGHT_TRANSPORT="${RIGHT_TRANSPORT:-}"
RIGHT_LABEL="${RIGHT_LABEL:-}"

# Pro warehouse (leave empty to skip; runs the PRO_WAREHOUSE config against this third warehouse)
PRO_WAREHOUSE_ID="${PRO_WAREHOUSE_ID:-}"

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
CONNECTION_CONFIG="${CONNECTION_CONFIG:-}"

# Test suites to run (comma-separated, empty = all suites for the config)
# Options: STATEMENT_SELECT, STATEMENT_SELECT_TRUNCATED, STATEMENT_DDL, STATEMENT_DML,
#          STATEMENT_OTHER, PREPARED_STATEMENT_TYPES, PREPARED_STATEMENT_METADATA,
#          COMPLEX_TYPES, GEOSPATIAL, NULL_HANDLING, VOLUME_OPERATIONS, DATABASE_METADATA
SUITES_RUN_ONLY="${SUITES_RUN_ONLY:-}"

# DatabaseMetaData options
# Available methods: getCatalogs, getSchemas, getTables, getColumns,
#   getPrimaryKeys, getImportedKeys, getExportedKeys, getCrossReference, getFunctions
# Note: getFunctions — skip, not supported in Thrift native (always returns 0 rows)
METADATA_RUN_ONLY_METHODS="${METADATA_RUN_ONLY_METHODS:-}"  # empty = all methods
METADATA_SKIP_METHODS="${METADATA_SKIP_METHODS:-getFunctions}"  # not supported in Thrift native (always returns 0 rows)
METADATA_SKIP_SCHEMAS="${METADATA_SKIP_SCHEMAS:-information_schema}"
METADATA_PARALLEL_THREADS="${METADATA_PARALLEL_THREADS:-50}"
SKIP_DIFF_PATTERNS="${SKIP_DIFF_PATTERNS:-}"  # geo and variant fixes merged — no filtering needed

# Error comparison mode: off | shadow.
#   off    — legacy: only the exception class is checked
#   shadow — deep error comparison (class/SQLState/code/message/one-sided) recorded as DIFF rows in
#            the report/CSV; never fails the run (DEFAULT)
ERROR_COMPARISON_MODE="${ERROR_COMPARISON_MODE:-shadow}"

# Workspace setup (empty = skip, "recreate" = drop + create all test data)
WORKSPACE_SETUP="${WORKSPACE_SETUP:-}"

# Git repo
REPO_URL="${REPO_URL:-https://github.com/databricks/databricks-jdbc.git}"
MERGE_BRANCH="${MERGE_BRANCH:-main}"  # branch to merge into comparator-v2 (e.g., main, feature-branch)
WORK_DIR="${WORK_DIR:-/tmp/jdbc-comparator-$$}"

# Run name (used in log/report filenames for easy identification, empty = generic name)
RUN_NAME="${RUN_NAME:-}"

# ============================================================================
# FILTER CONFIG — edit to skip known noisy argument combinations
# ============================================================================

FILTER_JSON='{
  "metadataSkipFilters": {
    "getSchemas": [
      {"catalog": "%", "reason": "Thrift treats % as catalog pattern, SEA does exact match (correct per JDBC spec)"},
      {"catalog": "compar%", "reason": "Thrift treats compar% as catalog pattern, SEA does exact match (correct per JDBC spec)"},
      {"catalog": "COMPARATOR-TESTS", "reason": "Thrift returns upper case value, SEA returns stored value (SEA behaviour is correct)"}
    ],
    "getTables": [
      {"catalog": "", "schemaPattern": "null", "tableNamePattern": "!"},
      {"catalog": "", "schemaPattern": "%", "tableNamePattern": "!"},
      {"catalog": "", "schemaPattern": "oss_jdbc_tests", "tableNamePattern": "!"},
      {"catalog": "", "schemaPattern": "oss%", "tableNamePattern": "!"},
      {"catalog": "", "schemaPattern": "!", "tableNamePattern": "null"},
      {"catalog": "", "schemaPattern": "!", "tableNamePattern": "%"},
      {"catalog": "", "schemaPattern": "!", "tableNamePattern": "test_result_set_types"},
      {"catalog": "", "schemaPattern": "!", "tableNamePattern": "test%"},
      {"catalog": "COMPARATOR-TESTS", "reason": "SEA resolves catalog case-insensitively (correct), Thrift does not"},
      {"catalog": "comparator\\_tests", "reason": "SEA unescapes underscore in catalog (correct), Thrift does not"}
    ],
    "getColumns": [
      {"catalog": "", "tableNamePattern": "null"},
      {"catalog": "", "tableNamePattern": "%"},
      {"catalog": "", "tableNamePattern": "test_result_set_types"},
      {"catalog": "", "tableNamePattern": "test%"},
      {"schemaPattern": "", "tableNamePattern": "null"},
      {"schemaPattern": "", "tableNamePattern": "%"},
      {"schemaPattern": "", "tableNamePattern": "test_result_set_types"},
      {"schemaPattern": "", "tableNamePattern": "test%"},
      {"catalog": "%"},
      {"catalog": "compar%"}
    ]
  }
}'
# ---- Skip filter reasons ----
# getSchemas:
#   catalog=%                — Thrift treats % as catalog pattern, SEA does exact match (correct per JDBC spec)
#   catalog=compar%          — Same as above
#   catalog=comparator\_tests — SEA returns escaped catalog name in TABLE_CATALOG. PR open: #1365
#
# getTables:
#   catalog="" — Thrift throws when catalog is empty AND at least one of schema/table is a pattern
#     AND neither schema nor table is empty "". SEA returns ResultSet.
#     Pattern schemas (4): null, %, oss_jdbc_tests, oss%
#     Pattern tables (4): null, %, test_result_set_types, test%
#     336 diffs total
#   catalog=COMPARATOR-TESTS — SEA resolves case-insensitively and finds tables, Thrift doesn't. 168 diffs.
#     SEA correct per JDBC spec.
#   catalog=comparator\_tests — SEA unescapes the input catalog and finds tables, Thrift doesn't. 168 diffs.
#     Same pattern as COMPARATOR-TESTS.
#
# getColumns:
#   catalog="" or schemaPattern="" with pattern table — Thrift throws when either catalog or schema
#     is empty "" AND table is a pattern (null, %, test_result_set_types, test%).
#     SEA returns ResultSet. 468 metadata diffs total (180 empty catalog + 288 empty schema).
#   catalog=% and compar% — Thrift treats as catalog pattern (returns all catalogs), SEA does exact match.
#     Same as getSchemas. 686 extra row diffs.
#   NOTE: GEOMETRY/GEOGRAPHY DATA_TYPE mismatch (0 vs 12) present across all catalogs — 3696 diffs.
#     Driver-side bug: Thrift missing geospatial disable check in getThriftRows(). Not filtered.
#
# getPrimaryKeys:
#   table=null or "" — Thrift throws for null/empty table. SEA returns ResultSet. 72 diffs.
#   schema=null + non-null catalog — Thrift throws for null schema with any non-null catalog. 55 diffs.
#
# getImportedKeys:
#   Same as getPrimaryKeys — table=null/empty (72 diffs) + schema=null with non-null catalog (40 diffs). 112 total.
#
# getExportedKeys:
#   table=null — Thrift throws for null table. SEA returns ResultSet. 36 diffs.
# ---- End skip filter reasons ----

# ============================================================================
# DO NOT EDIT BELOW THIS LINE
# ============================================================================

set -e

# Validate required env
if [ -z "${DATABRICKS_COMPARATOR_TOKEN}" ]; then
  echo "ERROR: DATABRICKS_COMPARATOR_TOKEN is not set." >&2
  echo "  Local: export DATABRICKS_COMPARATOR_TOKEN=dapi..." >&2
  echo "  CI: ensure the secret is wired in the workflow env block." >&2
  exit 1
fi
if [ -z "${COMPARATOR_HOST}" ]; then
  echo "ERROR: COMPARATOR_HOST is not set." >&2
  exit 1
fi
# Must specify legacy COMPARATOR_WAREHOUSE OR generic LEFT/RIGHT endpoint config
ANY_LEFT_RIGHT="${LEFT_WAREHOUSE}${LEFT_CLUSTER}${LEFT_HTTP_PATH}${RIGHT_WAREHOUSE}${RIGHT_CLUSTER}${RIGHT_HTTP_PATH}"
if [ -z "${COMPARATOR_WAREHOUSE}" ] && [ -z "${ANY_LEFT_RIGHT}" ]; then
  echo "ERROR: Set COMPARATOR_WAREHOUSE (legacy) or LEFT_*/RIGHT_* (generic axis)." >&2
  exit 1
fi

TIMESTAMP=$(date +%Y%m%d-%H%M%S)
PREFIX="${RUN_NAME:-comparator}"
LOG_FILE="${PREFIX}-logs-${TIMESTAMP}.txt"
REPORT_FILE="${PREFIX}-report-${TIMESTAMP}.txt"
CSV_FILE_NAME="${PREFIX}-results-${TIMESTAMP}.csv"
FILTER_FILE="${WORK_DIR}/metadata-filters.json"

# Detect generic vs legacy axis and emit a one-line summary of each side.
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
echo "Output: ${LOG_FILE}, ${REPORT_FILE}, ${CSV_FILE_NAME}"
echo ""

# Capture current directory before cd
ORIGINAL_DIR="$(pwd)"

# Clone and prepare
echo "[1/5] Cloning repository..."
git clone --branch comparator-v2 "${REPO_URL}" "${WORK_DIR}"
cd "${WORK_DIR}"

# Local-only git identity for the merge commit below (CI runners have no global config).
# --local scope keeps this confined to the temp clone; caller's global config is untouched.
git config --local user.name "JDBC Comparator Runner"
git config --local user.email "actions@github.com"

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
if [ -n "${ERROR_COMPARISON_MODE}" ]; then
  MVN_ARGS="${MVN_ARGS} -DERROR_COMPARISON_MODE=${ERROR_COMPARISON_MODE}"
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
  CSV_DEST="${ORIGINAL_DIR}/${CSV_FILE_NAME}"
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
echo "MVN exit: ${MVN_EXIT}"

exit ${MVN_EXIT:-0}
