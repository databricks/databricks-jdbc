# JDBC Driver Comparator

Compares two SQL endpoints by running identical JDBC API calls through both and reporting differences. The two endpoints (LEFT and RIGHT) can differ along any combination of:

- **Transport**: Thrift (`useThriftClient=1`) vs SEA (`useThriftClient=0`).
- **Resource**: a SQL warehouse, an interactive cluster, or any explicit JDBC `httpPath`.

The original use case — Thrift vs SEA against a single warehouse — is preserved as the legacy default. See [Comparison axis](#comparison-axis) for the configuration matrix.

## Quickstart

```bash
export DATABRICKS_COMPARATOR_TOKEN=dapi...

mvn test -pl jdbc-core -Dtest=JDBCDriverComparisonTest \
  -DCONNECTION_CONFIG=DEFAULT_PARAMS \
  -DMETADATA_SKIP_SCHEMAS=information_schema,global_temp \
  -DMETADATA_PARALLEL_THREADS=40 \
  -DMETADATA_FILTER_CONFIG=/absolute/path/to/metadata-filters.json
```

Sample `metadata-filters.json`:
```json
{
  "metadataRunOnlyFilters": {
    "getTables": [
      {"catalog": "comparator_tests"}
    ]
  },
  "metadataSkipFilters": {
    "getTables": [
      {"schemaPattern": ""},
      {"types": "[]"},
      {"catalog": "compar%", "schemaPattern": "nonexistent"}
    ],
    "getSchemas": [
      {"schemaPattern": ""}
    ],
    "getColumns": [
      {"schemaPattern": ""}
    ]
  }
}
```

## Comparison axis

Each comparator run defines two **endpoints**, LEFT and RIGHT, each specified by:

- `httpPath` — `/sql/1.0/warehouses/<id>` or `/sql/protocolv1/o/<orgId>/<clusterId>` or any value you supply.
- `transport` — `sea` (default) or `thrift`.
- `label` — free text used in headers; defaults to `<SIDE>-<TRANSPORT>` uppercased.

Per side, the path resolves via this precedence (first match wins): `<SIDE>_HTTP_PATH` → `<SIDE>_CLUSTER` → `<SIDE>_WAREHOUSE`.

If neither LEFT nor RIGHT is set, the comparator falls back to the legacy mode: same warehouse on both sides, Thrift on the left and SEA on the right (driven by `COMPARATOR_WAREHOUSE`).

Setup DDL (when `WORKSPACE_SETUP=recreate|validate`) always runs against the LEFT side. Make LEFT the side that supports the suite's DDL.

| Use case | Properties |
|---|---|
| Legacy (Thrift vs SEA, one warehouse) | `COMPARATOR_WAREHOUSE=<id>` |
| SEA vs SEA across two warehouses | `LEFT_WAREHOUSE=<dbr> RIGHT_WAREHOUSE=<reyden>` |
| Thrift vs Thrift across two warehouses | `LEFT_WAREHOUSE=<a> LEFT_TRANSPORT=thrift RIGHT_WAREHOUSE=<b> RIGHT_TRANSPORT=thrift` |
| Warehouse (SEA) vs cluster (Thrift) | `LEFT_WAREHOUSE=<a> RIGHT_CLUSTER=<orgId>:<clusterId> RIGHT_TRANSPORT=thrift` |
| Hand-rolled paths | `LEFT_HTTP_PATH=… RIGHT_HTTP_PATH=…` |

Both endpoints share `COMPARATOR_HOST` and the single `DATABRICKS_COMPARATOR_TOKEN`.

## Running

### Basic usage
```bash
mvn test -pl jdbc-core -Dtest=JDBCDriverComparisonTest
```

### Choose workspace
```bash
mvn test -pl jdbc-core -Dtest=JDBCDriverComparisonTest \
  -DCOMPARATOR_HOST=adb-xxx.azuredatabricks.net \
  -DCOMPARATOR_WAREHOUSE=abc123
```

### Choose what to run
```bash
# Specific connection configs
-DCONNECTION_CONFIG=DEFAULT_PARAMS,COMPRESSION_DISABLED

# Specific DatabaseMetaData methods
# Available: getCatalogs, getSchemas, getTables, getColumns, getPrimaryKeys,
#   getImportedKeys, getExportedKeys, getCrossReference, getFunctions
-DMETADATA_RUN_ONLY_METHODS=getCatalogs,getSchemas,getTables

# Pro warehouse (only runs when ID is provided)
-DPRO_WAREHOUSE_ID=7b03aaa124ecb70e

# Run only specific test suites
-DSUITES_RUN_ONLY=STATEMENT_SELECT,DATABASE_METADATA
```

### DatabaseMetaData tuning
```bash
# Parallel execution (default: 1 = sequential)
-DMETADATA_PARALLEL_THREADS=40

# Filter schemas from ResultSet comparison
-DMETADATA_SKIP_SCHEMAS=information_schema,global_temp

# Filter specific argument combinations via JSON config (use absolute path)
-DMETADATA_FILTER_CONFIG=/absolute/path/to/metadata-filters.json
```

### Workspace setup (creates all test data from scratch)
```bash
-DWORKSPACE_SETUP=recreate
```

### Full example
```bash
export DATABRICKS_COMPARATOR_TOKEN=dapi...

mvn test -pl jdbc-core -Dtest=JDBCDriverComparisonTest \
  -DCOMPARATOR_HOST=adb-7405613695221181.1.azuredatabricks.net \
  -DCOMPARATOR_WAREHOUSE=6feab30b476abfa4 \
  -DCONNECTION_CONFIG=DEFAULT_PARAMS \
  -DMETADATA_RUN_ONLY_METHODS=getTables \
  -DMETADATA_SKIP_SCHEMAS=information_schema,global_temp \
  -DMETADATA_PARALLEL_THREADS=40 \
  -DMETADATA_FILTER_CONFIG=/path/to/metadata-filters.json
```

## Output

- **Logs**: Console output with per-argument combination progress (`Started comparing`, `Finished comparing`, `Skipped`)
- **Reports**: `jdbc-core/jdbc-comparison-report-<timestamp>.txt`
- **Errors**: Print immediately with timestamp and test name

Redirect logs to file:
```bash
mvn test ... 2>&1 > my-test-folder/logs/run.txt
```

## JSON Filter Config

Filter specific argument combinations for DatabaseMetaData methods. Supports two modes:

- **`metadataRunOnlyFilters`** — whitelist: only run argument combinations matching at least one pattern
- **`metadataSkipFilters`** — blacklist: skip argument combinations matching any pattern

If both are present for a method, **runOnly takes precedence**: argument combination must pass the whitelist first, then must not match the blacklist.

```json
{
  "metadataRunOnlyFilters": {
    "getTables": [
      {"catalog": "comparator_tests", "schemaPattern": "oss_jdbc_tests"}
    ]
  },
  "metadataSkipFilters": {
    "getTables": [
      {"types": "[]"}
    ]
  }
}
```

- Each method has a list of filter patterns
- An argument combination matches if **ANY** pattern matches (OR across patterns)
- Within a pattern, **ALL** conditions must match (AND within pattern)
- Use absolute path: `-DMETADATA_FILTER_CONFIG=/absolute/path/to/file.json`

### Special values in filters

| JSON value | Matches |
|---|---|
| `""` | Empty string |
| `"null"` | Null argument |
| `"[]"` | Empty String[] array |
| `"!value"` | Negation — argument is NOT equal to value |
| `"!"` | Argument is NOT empty string |
| `"!null"` | Argument is NOT null |

---

## Appendix

### All System Properties

| Property | Default | Description |
|---|---|---|
| `COMPARATOR_HOST` | `adb-7405613695221181.1.azuredatabricks.net` | Workspace host (shared by LEFT and RIGHT) |
| `COMPARATOR_WAREHOUSE` | `6feab30b476abfa4` | Legacy single-warehouse ID; ignored when any `LEFT_*` / `RIGHT_*` is set |
| `LEFT_WAREHOUSE` / `RIGHT_WAREHOUSE` | _(none)_ | Warehouse ID for that side |
| `LEFT_CLUSTER` / `RIGHT_CLUSTER` | _(none)_ | Interactive cluster `orgId:clusterId` for that side |
| `LEFT_HTTP_PATH` / `RIGHT_HTTP_PATH` | _(none)_ | Full JDBC `httpPath` for that side (escape hatch) |
| `LEFT_TRANSPORT` / `RIGHT_TRANSPORT` | `sea` | `sea` or `thrift` |
| `LEFT_LABEL` / `RIGHT_LABEL` | `<SIDE>-<TRANSPORT>` | Free-text label used in report header and AssertionError messages |
| `PRO_WAREHOUSE_ID` | _(disabled)_ | Pro warehouse ID — used by the `PRO_WAREHOUSE` config |
| `CONNECTION_CONFIG` | _(all)_ | Comma-separated configs to run |
| `SUITES_RUN_ONLY` | _(all)_ | Comma-separated suites to run |
| `METADATA_RUN_ONLY_METHODS` | _(all)_ | Comma-separated methods to run |
| `METADATA_SKIP_METHODS` | _(none)_ | Comma-separated methods to skip |
| `METADATA_SKIP_SCHEMAS` | _(none)_ | Schemas to filter from comparison |
| `SKIP_DIFF_PATTERNS` | _(none)_ | Pipe-separated patterns to exclude from report |
| `METADATA_PARALLEL_THREADS` | `1` | Parallel threads |
| `METADATA_FILTER_CONFIG` | _(none)_ | Absolute path to JSON filter |
| `WORKSPACE_SETUP` | _(skip)_ | Set to `recreate` for fresh setup |

### Connection Configs

| Config | Extra Params | Suites |
|---|---|---|
| `DEFAULT_PARAMS` | _(none)_ | SELECT, SELECT_TRUNCATED, DDL, DML, OTHER, PREPARED_TYPES, PREPARED_METADATA, NULL_HANDLING, DATABASE_METADATA |
| `COMPRESSION_DISABLED` | `EnableQueryResultLZ4Compression=0` | SELECT |
| `DIRECT_RESULTS_DISABLED` | `EnableDirectResults=0` | SELECT |
| `COMPLEX_TYPES_ENABLED` | `EnableComplexDatatypeSupport=1` | COMPLEX_TYPES |
| `COMPLEX_TYPES_DISABLED` | `EnableComplexDatatypeSupport=0` | COMPLEX_TYPES |
| `GEOSPATIAL_ENABLED` | `EnableComplexDatatypeSupport=1, EnableGeoSpatialSupport=1` | GEOSPATIAL |
| `GEOSPATIAL_DISABLED` | `EnableComplexDatatypeSupport=1, EnableGeoSpatialSupport=0` | GEOSPATIAL |
| `USE_QUERY_FOR_METADATA` | `UseQueryForMetadata=1` | DATABASE_METADATA |
| `VOLUME_OPERATIONS` | `VolumeOperationAllowedLocalPaths=/tmp` | VOLUME_OPERATIONS |
| `PRO_WAREHOUSE` | `EnableComplexDatatypeSupport=1, EnableGeoSpatialSupport=1` | SELECT, COMPLEX_TYPES, GEOSPATIAL (requires `PRO_WAREHOUSE_ID`) |

### Filter Argument Names

| Method | Arguments (positional order) |
|---|---|
| `getSchemas` | catalog, schemaPattern |
| `getTables` | catalog, schemaPattern, tableNamePattern, types |
| `getColumns` | catalog, schemaPattern, tableNamePattern, columnNamePattern |
| `getPrimaryKeys` | catalog, schema, table |
| `getImportedKeys` | catalog, schema, table |
| `getExportedKeys` | catalog, schema, table |
| `getCrossReference` | parentCatalog, parentSchema, parentTable, foreignCatalog, foreignSchema, foreignTable |
| `getFunctions` | catalog, schemaPattern, functionNamePattern |

### Test Suites

| Suite | Description |
|---|---|
| `STATEMENT_SELECT` | SELECT queries at various sizes (inline + CloudFetch) |
| `STATEMENT_SELECT_TRUNCATED` | setMaxRows / setLargeMaxRows truncation |
| `STATEMENT_DDL` | CREATE / ALTER / DROP |
| `STATEMENT_DML` | INSERT / UPDATE / DELETE |
| `STATEMENT_OTHER` | SHOW / DESCRIBE / EXPLAIN / SET |
| `PREPARED_STATEMENT_TYPES` | Setters for all databricks supported types + CloudFetch |
| `PREPARED_STATEMENT_METADATA` | getMetaData / getParameterMetaData |
| `COMPLEX_TYPES` | ARRAY / MAP / STRUCT / nested types |
| `GEOSPATIAL` | GEOMETRY / GEOGRAPHY |
| `NULL_HANDLING` | wasNull() verification |
| `VOLUME_OPERATIONS` | UC Volume PUT / GET / DELETE |
| `DATABASE_METADATA` | All DatabaseMetaData methods (~13,500 argument combinations) |
| `NEGATIVE_STATEMENT_SELECT` | Error-provoking SELECTs (missing table/column, syntax, cast, wrong method) |
| `NEGATIVE_STATEMENT_OTHER` | Error-provoking SHOW / DESCRIBE / EXPLAIN / SET + JDBC-API misuse |
| `NEGATIVE_PARAM_BINDING` | Bad PreparedStatement bindings (index, count, type, precision) |
| `NEGATIVE_PREPARED_METADATA` | clearParameters + unbound execute; getMetaData on invalid SQL |
| `NEGATIVE_TYPE_CONVERSION` | Incompatible ResultSet.getX() conversions (overflow, wrong target) |
| `NEGATIVE_STATEMENT_DDL` | Error-provoking CREATE / ALTER / DROP (missing/duplicate objects, bad namespace, malformed) |
| `NEGATIVE_STATEMENT_DML` | Error-provoking INSERT / UPDATE / DELETE (type mismatch, NOT NULL, missing table, overflow) |
| `NEGATIVE_STATEMENT_BATCH` | executeBatch partial/full failure + per-element BatchUpdateException counts |
| `NEGATIVE_CONNECTION_STATE` | setCatalog/setSchema/setClientInfo/USE to nonexistent targets (own fresh connections) |
| `NEGATIVE_TRANSACTION` | commit/rollback with autocommit on; DDL inside a manual transaction (own fresh connections) |

Negative suites compare each endpoint's **error behavior** (exception class, SQLState, vendor code,
message) via the `ERROR_COMPARISON_MODE` gate (default `shadow`). See
[`error/`](error/) for the comparison engine.

Suites whose cases mutate or destroy connection/session state (`NEGATIVE_CONNECTION_STATE`,
`NEGATIVE_TRANSACTION`, and — later — connection/cancel/volume cases) open their **own dedicated,
uncached connections** via `ConnectionFactory.openFresh(side)` and close them in a `finally`, so
they never poison the shared connections the other suites reuse. Providers request this by
overriding the `execute(conn1, conn2, ConnectionFactory, testCase, label)` overload of
`SuiteProvider`.

