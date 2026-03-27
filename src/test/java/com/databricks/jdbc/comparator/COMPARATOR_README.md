# JDBC Driver Comparator

Compares Thrift vs SEA modes of the Databricks JDBC driver by running identical JDBC API calls through both code paths and reporting differences.

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
  "metadataSkipFilters": {
    "getTables": [
      {"schemaPattern": ""},
      {"types": "[]"},
      {"catalog": "comp%", "schemaPattern": "nonexistent"}
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

Skip specific argument combinations for DatabaseMetaData methods. Useful for filtering out known diffs.

```json
{
  "metadataSkipFilters": {
    "getTables": [
      {"schemaPattern": ""},
      {"types": "[]"},
      {"catalog": "comp%", "schemaPattern": "nonexistent"}
    ],
    "getSchemas": [
      {"schemaPattern": ""}
    ]
  }
}
```

- Each method has a list of filter patterns
- An argument combination is skipped if **ANY** pattern matches (OR)
- Within a pattern, **ALL** conditions must match (AND)
- Use absolute path: `-DMETADATA_FILTER_CONFIG=/absolute/path/to/file.json`

### Special values in filters

| JSON value | Matches |
|---|---|
| `""` | Empty string |
| `"null"` | Null argument |
| `"[]"` | Empty String[] array |

---

## Appendix

### All System Properties

| Property | Default | Description |
|---|---|---|
| `COMPARATOR_HOST` | `adb-7405613695221181.1.azuredatabricks.net` | Workspace host |
| `COMPARATOR_WAREHOUSE` | `6feab30b476abfa4` | Warehouse ID |
| `PRO_WAREHOUSE_ID` | _(disabled)_ | Pro warehouse ID |
| `CONNECTION_CONFIG` | _(all)_ | Comma-separated configs to run |
| `SUITES_RUN_ONLY` | _(all)_ | Comma-separated suites to run |
| `METADATA_RUN_ONLY_METHODS` | _(all)_ | Comma-separated methods |
| `METADATA_SKIP_SCHEMAS` | _(none)_ | Schemas to filter from comparison |
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
| `PREPARED_STATEMENT_TYPES` | All JDBC setter types + CloudFetch |
| `PREPARED_STATEMENT_METADATA` | getMetaData / getParameterMetaData |
| `COMPLEX_TYPES` | ARRAY / MAP / STRUCT / nested types |
| `GEOSPATIAL` | GEOMETRY / GEOGRAPHY |
| `NULL_HANDLING` | wasNull() verification |
| `VOLUME_OPERATIONS` | UC Volume PUT / GET / DELETE |
| `DATABASE_METADATA` | All DatabaseMetaData methods (~13,500 argument combinations) |

