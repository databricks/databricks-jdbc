package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOGRAPHY;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.GEOMETRY;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.INTERVAL;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.MEASURE;
import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.*;
import static com.databricks.jdbc.dbclient.impl.common.TypeValConstants.*;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.common.Nullable;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ColumnMetadata;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.sdk.service.sql.StatementState;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class MetadataResultSetBuilder {
  private static final IDatabricksResultSetAdapter defaultAdapter =
      new DefaultDatabricksResultSetAdapter();
  private static final IDatabricksResultSetAdapter importedKeysAdapter =
      new ImportedKeysDatabricksResultSetAdapter();
  private final IDatabricksConnectionContext ctx;

  public MetadataResultSetBuilder(IDatabricksConnectionContext ctx) {
    this.ctx = ctx;
  }

  public DatabricksResultSet getFunctionsResult(DatabricksResultSet resultSet, String catalog)
      throws SQLException {
    List<List<Object>> rows = getRowsForFunctions(resultSet, FUNCTION_COLUMNS, catalog);
    return buildResultSet(
        FUNCTION_COLUMNS,
        rows,
        GET_FUNCTIONS_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_FUNCTIONS);
  }

  public DatabricksResultSet getColumnsResult(DatabricksResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, COLUMN_COLUMNS, defaultAdapter);
    return buildResultSet(
        COLUMN_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_COLUMNS);
  }

  public DatabricksResultSet getCatalogsResult(DatabricksResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, CATALOG_COLUMNS, defaultAdapter);
    return buildResultSet(
        CATALOG_COLUMNS,
        rows,
        GET_CATALOGS_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_CATALOGS);
  }

  public DatabricksResultSet getSchemasResult(DatabricksResultSet resultSet, String catalog)
      throws SQLException {
    List<List<Object>> rows =
        getRowsForSchemas(
            resultSet, SCHEMA_COLUMNS, catalog, new SchemasDatabricksResultSetAdapter());
    return buildResultSet(
        SCHEMA_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_SCHEMAS);
  }

  public DatabricksResultSet getTablesResult(DatabricksResultSet resultSet, String[] tableTypes)
      throws SQLException {
    List<String> allowedTableTypes = List.of(tableTypes);
    List<List<Object>> rows =
        getRows(resultSet, TABLE_COLUMNS, defaultAdapter).stream()
            .filter(row -> allowedTableTypes.contains(row.get(3))) // Filtering based on table type
            .collect(Collectors.toList());

    // Sort in order TABLE_TYPE, TABLE_CAT, TABLE_SCHEM, TABLE_NAME (matching Thrift mode)
    rows.sort(
        Comparator.comparing((List<Object> r) -> (String) r.get(3)) // TABLE_TYPE
            .thenComparing(r -> (String) r.get(0)) // TABLE_CAT
            .thenComparing(r -> (String) r.get(1)) // TABLE_SCHEM
            .thenComparing(r -> (String) r.get(2))); // TABLE_NAME

    return buildResultSet(
        TABLE_COLUMNS,
        rows,
        GET_TABLES_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_TABLES);
  }

  public DatabricksResultSet getTableTypesResult() {
    List<List<Object>> tableTypesRows =
        ctx.getEnableMetricViewMetadata()
            ? TABLE_TYPES_ROWS
            : TABLE_TYPES_ROWS.stream()
                .filter(row -> !"METRIC_VIEW".equals(row.get(0)))
                .collect(Collectors.toList());

    return buildResultSet(
        TABLE_TYPE_COLUMNS,
        tableTypesRows,
        GET_TABLE_TYPE_STATEMENT_ID,
        CommandName.LIST_TABLE_TYPES);
  }

  public DatabricksResultSet getPrimaryKeysResult(DatabricksResultSet resultSet)
      throws SQLException {
    List<List<Object>> rows = getRows(resultSet, PRIMARY_KEYS_COLUMNS, defaultAdapter);
    return buildResultSet(
        PRIMARY_KEYS_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.LIST_PRIMARY_KEYS);
  }

  public DatabricksResultSet getImportedKeysResult(DatabricksResultSet resultSet)
      throws SQLException {
    List<List<Object>> rows = getRows(resultSet, IMPORTED_KEYS_COLUMNS, importedKeysAdapter);
    return buildResultSet(
        IMPORTED_KEYS_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.GET_IMPORTED_KEYS);
  }

  public DatabricksResultSet getCrossReferenceKeysResult(
      DatabricksResultSet resultSet,
      String targetParentCatalogName,
      String targetParentNamespaceName,
      String targetParentTableName)
      throws SQLException {
    final CrossReferenceKeysDatabricksResultSetAdapter crossReferenceKeysResultSetAdapter =
        new CrossReferenceKeysDatabricksResultSetAdapter(
            targetParentCatalogName, targetParentNamespaceName, targetParentTableName);
    List<List<Object>> rows =
        getRows(resultSet, CROSS_REFERENCE_COLUMNS, crossReferenceKeysResultSetAdapter);

    return buildResultSet(
        CROSS_REFERENCE_COLUMNS,
        rows,
        METADATA_STATEMENT_ID,
        resultSet.getMetaData(),
        CommandName.GET_CROSS_REFERENCE);
  }

  private boolean isTextType(String typeVal) {
    return (typeVal.contains(TEXT_TYPE)
        || typeVal.contains(CHAR_TYPE)
        || typeVal.contains(VARCHAR_TYPE)
        || typeVal.contains(STRING_TYPE));
  }

  List<List<Object>> getRows(
      DatabricksResultSet resultSet,
      List<ResultColumn> columns,
      IDatabricksResultSetAdapter adapter)
      throws SQLException {
    List<List<Object>> rows = new ArrayList<>();
    resultSet.setSilenceNonTerminalExceptions();
    while (resultSet.next()) {
      // Check if this row should be included based on the adapter's filter
      if (!adapter.includeRow(resultSet, columns)) {
        continue;
      }

      List<Object> row = new ArrayList<>();
      for (ResultColumn column : columns) {
        // Map the column using the adapter
        ResultColumn mappedColumn = adapter.mapColumn(column);

        // TODO: Put these transformations under IDatabricksResultSetAdapter#transformValue
        Object object;
        String typeVal = null;
        try {
          typeVal =
              resultSet.getString(
                  COLUMN_TYPE_COLUMN
                      .getResultSetColumnName()); // only valid for result set of getColumns
        } catch (SQLException ignored) {
        }
        switch (mappedColumn.getColumnName()) {
          case "SQL_DATA_TYPE":
            if (typeVal == null) { // safety check
              object = null;
            } else {
              // Check if geospatial support is disabled and this is a geospatial type
              if (!ctx.isGeoSpatialSupportEnabled() && isGeospatialType(typeVal)) {
                object = Types.VARCHAR;
              } else if (!ctx.isComplexDatatypeSupportEnabled() && isComplexType(typeVal)) {
                // Check if complex datatype support is disabled and this is a complex type
                object = Types.VARCHAR;
              } else {
                object = getCode(stripBaseTypeName(typeVal));
              }
            }
            break;
          case "SQL_DATETIME_SUB":
            // check if typeVal is a date/time related field
            if (typeVal != null
                && (stripBaseTypeName(typeVal).contains(DATE_TYPE)
                    || stripBaseTypeName(typeVal).contains(TIMESTAMP_TYPE))) {
              object = getCode(stripBaseTypeName(typeVal));
            } else {
              object = null;
            }
            break;
          default:
            // If column does not match any of the special cases, try to get it from the ResultSet
            try {
              object = resultSet.getObject(mappedColumn.getResultSetColumnName());
              if (mappedColumn.getColumnName().equals(IS_NULLABLE_COLUMN.getColumnName())) {
                if (object == null || object.equals("true")) {
                  object = "YES";
                } else {
                  object = "NO";
                }
              } else if (mappedColumn
                  .getColumnName()
                  .equals(DECIMAL_DIGITS_COLUMN.getColumnName())) {
                object = getUpdatedDecimalDigits(stripBaseTypeName(typeVal), object);
              } else if (mappedColumn
                  .getColumnName()
                  .equals(NUM_PREC_RADIX_COLUMN.getColumnName())) {
                if (object == null) {
                  object = 0;
                }
              } else if (mappedColumn.getColumnName().equals(REMARKS_COLUMN.getColumnName())) {
                if (object == null) {
                  object = "";
                }
              }
            } catch (SQLException e) {
              if (mappedColumn.getColumnName().equals(DATA_TYPE_COLUMN.getColumnName())) {
                // Check if geospatial support is disabled and this is a geospatial type
                if (!ctx.isGeoSpatialSupportEnabled() && isGeospatialType(typeVal)) {
                  object = Types.VARCHAR;
                } else if (!ctx.isComplexDatatypeSupportEnabled() && isComplexType(typeVal)) {
                  object = Types.VARCHAR;
                } else {
                  object = getCode(stripBaseTypeName(typeVal));
                }
              } else if (mappedColumn
                  .getColumnName()
                  .equals(CHAR_OCTET_LENGTH_COLUMN.getColumnName())) {
                object = getCharOctetLength(typeVal);
                if (object.equals(0)) {
                  object = null;
                }
              } else if (mappedColumn
                  .getColumnName()
                  .equals(BUFFER_LENGTH_COLUMN.getColumnName())) {
                object = getBufferLength(typeVal);
              } else {
                // Handle other cases where the result set does not contain the expected column
                object = null;
              }
            }
            if (mappedColumn.getColumnName().equals(NULLABLE_COLUMN.getColumnName())) {
              object = resultSet.getObject(IS_NULLABLE_COLUMN.getResultSetColumnName());
              if (object == null || object.equals("true")) {
                object = 1;
              } else {
                object = 0;
              }
            }
            if (mappedColumn.getColumnName().equals(TABLE_TYPE_COLUMN.getColumnName())
                && (object == null || object.equals(""))) {
              object = "TABLE";
            }

            // Handle TYPE_NAME separately for potential modifications
            if (mappedColumn.getColumnName().equals(COLUMN_TYPE_COLUMN.getColumnName())) {
              if (typeVal != null
                  && (typeVal.contains(MEASURE)
                      || typeVal.contains(ARRAY_TYPE)
                      || typeVal.contains(MAP_TYPE)
                      || typeVal.contains(
                          STRUCT_TYPE))) { // for complex data types, do not strip type name
                object = typeVal;
              } else {
                object = stripBaseTypeName(typeVal);
              }
            }
            // Set COLUMN_SIZE to 255 if it's not present
            if (mappedColumn.getColumnName().equals(COLUMN_SIZE_COLUMN.getColumnName())) {
              object = getColumnSize(typeVal);
            }

            break;
        }

        // Apply any transformations from the adapter
        object = adapter.transformValue(mappedColumn, object);

        // Add the object to the current row
        row.add(object);
      }
      rows.add(row);
    }
    resultSet.unsetSilenceNonTerminalExceptions();
    return rows;
  }

  /**
   * Extracts the size from a SQL type definition in the format DATA_TYPE(size).
   *
   * @param typeVal The SQL type string (e.g., "VARCHAR(5000)", "CHAR(100)").
   * @return The size as an integer, or -1 if the size cannot be determined.
   */
  int getSizeFromTypeVal(String typeVal) {
    if (typeVal.isEmpty()) {
      return -1; // Return -1 for invalid input
    }

    // Regular expression to match DATA_TYPE(size) and extract the size
    String regex = "\\w+\\((\\d+)\\)";
    java.util.regex.Pattern pattern =
        java.util.regex.Pattern.compile(regex, java.util.regex.Pattern.CASE_INSENSITIVE);
    java.util.regex.Matcher matcher = pattern.matcher(typeVal);

    if (matcher.find()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        return -1;
      }
    }

    return -1;
  }

  /*
   * Extracts the precision from DECIMAL, NUMERIC types.
   * @param typeVal The SQL type string
   * Note: typeVal can be of format <data_type>, <data_type>(p), <data_type>(p,s)
   */
  int extractPrecision(String typeVal) {
    String lowerType = typeVal.toLowerCase().trim();
    Pattern pattern = Pattern.compile("\\((\\d+)(?:,\\s*\\d+)?\\)");
    Matcher matcher = pattern.matcher(lowerType);
    if (matcher.find()) {
      try {
        return Integer.parseInt(matcher.group(1));
      } catch (NumberFormatException e) {
        // In case of a parsing error, return default
        return 10;
      }
    }
    // If no parentheses with precision are found, return default
    return 10;
  }

  int getColumnSize(String typeVal) {
    if (typeVal == null || typeVal.isEmpty() || typeVal.contains(INTERVAL)) {
      return 0;
    }
    int sizeFromTypeVal = getSizeFromTypeVal(typeVal);
    if (sizeFromTypeVal != -1) {
      return sizeFromTypeVal;
    }
    if (isTextType(typeVal)) {
      return ctx.getDefaultStringColumnLength();
    }
    String typeName = stripBaseTypeName(typeVal);
    switch (typeName) {
      case "DECIMAL":
      case "NUMERIC":
        return extractPrecision(typeVal);
      case "SMALLINT":
        return 5;
      case "DATE":
      case "INT":
        return 10;
      case "BIGINT":
        return 19;
      case "FLOAT":
        return 7;
      case "DOUBLE":
        return 15;
      case "TIMESTAMP":
        return 29;
      case "BOOLEAN":
      case "BINARY":
        return 1;
      default:
        return 255;
    }
  }

  /*
   * Extracts the size in bytes from a given SQL type.
   * @param sqlType The SQL type
   */
  int getSizeInBytes(int sqlType) {
    switch (sqlType) {
      case Types.TIME:
      case Types.DATE:
        return 6;
      case Types.TIMESTAMP:
        return 16;
      case Types.NUMERIC:
      case Types.DECIMAL:
        return 40;
      case Types.REAL:
      case Types.INTEGER:
        return 4;
      case Types.FLOAT:
      case Types.DOUBLE:
      case Types.BIGINT:
        return 8;
      case Types.BINARY:
        return 32767;
      case Types.BIT:
      case Types.BOOLEAN:
      case Types.TINYINT:
        return 1;
      case Types.SMALLINT:
        return 2;
      default:
        return 0;
    }
  }

  int getBufferLength(String typeVal) {
    if (typeVal == null || typeVal.isEmpty()) {
      return 0;
    }
    if (typeVal.contains("ARRAY") || typeVal.contains("MAP") || typeVal.contains("STRUCT")) {
      return 255;
    }
    if (isTextType(typeVal)) {
      return getColumnSize(typeVal);
    }
    int sqlType = getCode(stripBaseTypeName(typeVal));
    return getSizeInBytes(sqlType);
  }

  /**
   * Overrides DECIMAL_DIGITS value for specific data types. Returns non-zero only for DECIMAL,
   * NUMERIC, TIMESTAMP, and TIMESTAMP_NTZ.
   *
   * @param baseTypeVal the column type name
   * @param scaleObject the original scale value (can be null)
   * @return scale value for DECIMAL/NUMERIC, 9 for TIMESTAMP types, 0 otherwise
   * @example
   *     <pre>
   * getUpdatedDecimalDigits("DECIMAL", 2) → 2
   * getUpdatedDecimalDigits("TIMESTAMP", 6) → 9
   * getUpdatedDecimalDigits("FLOAT", 7) → 0
   * </pre>
   */
  int getUpdatedDecimalDigits(String baseTypeVal, Object scaleObject) {
    if (scaleObject == null) {
      return 0;
    }
    int scale = (int) scaleObject;
    if (isNullOrEmpty(baseTypeVal)) {
      return 0;
    }
    if (baseTypeVal.contains(DECIMAL_TYPE) || baseTypeVal.contains(NUMERIC_TYPE)) {
      return scale;
    }
    if (baseTypeVal.contains(TIMESTAMP_TYPE) || baseTypeVal.contains(TIMESTAMP_NTZ_TYPE)) {
      return 9;
    }
    return 0;
  }

  /**
   * Extracts the character octet length from a given SQL type definition. For example, for input
   * "VARCHAR(100)", it returns 100. For inputs without a specified length or invalid inputs, it
   * returns 0.
   *
   * @param typeVal the SQL type definition
   * @return the character octet length or 0 if not applicable
   */
  int getCharOctetLength(String typeVal) {
    if (typeVal == null || !(isTextType(typeVal) || typeVal.contains(BINARY_TYPE))) return 0;

    if (!typeVal.contains("(")) {
      if (typeVal.contains(BINARY_TYPE)) {
        return 32767;
      } else {
        if (isTextType(typeVal)) {
          return ctx.getDefaultStringColumnLength();
        }
        return 255;
      }
    }
    String[] lengthConstraints = typeVal.substring(typeVal.indexOf('(') + 1).split("[,)]");
    if (lengthConstraints.length == 0) {
      return 0;
    }
    String octetLength = lengthConstraints[0].trim();
    try {
      return Integer.parseInt(octetLength);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  @VisibleForTesting
  public String stripTypeName(String typeName) {
    if (typeName == null) {
      return null;
    }
    boolean endsWithClosingBracket = typeName.endsWith(")");
    if (endsWithClosingBracket) {
      typeName = typeName.substring(0, typeName.lastIndexOf('('));
    }
    return typeName;
  }

  public String stripBaseTypeName(String typeName) {
    if (typeName == null) {
      return null;
    }
    // Checking '<' first and then '(' to handle cases like MAP<STRING,INT>(50)

    // Checks for ARRAY<STRING> -> ARRAY
    int typeArgumentIndex = typeName.indexOf('<');
    if (typeArgumentIndex != -1) {
      return typeName.substring(0, typeArgumentIndex);
    }

    // Checks for DECIMAL(10,2) -> DECIMAL
    typeArgumentIndex = typeName.indexOf('(');
    if (typeArgumentIndex != -1) {
      return typeName.substring(0, typeArgumentIndex);
    }

    return typeName;
  }

  /**
   * Checks if the given type string represents a complex type (ARRAY, MAP, or STRUCT).
   *
   * @param typeVal The type string to check
   * @return true if the type is a complex type, false otherwise
   */
  private boolean isComplexType(String typeVal) {
    if (typeVal == null) {
      return false;
    }
    String baseType = stripBaseTypeName(typeVal);
    return baseType.contains(ARRAY_TYPE)
        || baseType.contains(MAP_TYPE)
        || baseType.contains(STRUCT_TYPE);
  }

  /**
   * Checks if the given type string represents a geospatial type (GEOMETRY or GEOGRAPHY).
   *
   * @param typeVal The type string to check
   * @return true if the type is a geospatial type, false otherwise
   */
  private boolean isGeospatialType(String typeVal) {
    if (typeVal == null) {
      return false;
    }
    String baseType = stripBaseTypeName(typeVal);
    return baseType.contains(GEOMETRY) || baseType.contains(GEOGRAPHY);
  }

  int getCode(String s) {
    switch (s) {
      case "STRING":
      case "VARCHAR":
        return 12;
      case "INT":
      case "INTEGER":
        return 4;
      case "DOUBLE":
        return 8;
      case "FLOAT":
        return 6;
      case "BOOLEAN":
        return 16;
      case "DATE":
        return 91;
      case "TIMESTAMP_NTZ":
      case "TIMESTAMP":
        return 93;
      case "DECIMAL":
        return 3;
      case "NUMERIC":
        return 2;
      case "BINARY":
        return -2;
      case "ARRAY":
        return 2003;
      case "MAP":
      case "STRUCT":
      case "UNIONTYPE":
        return 2002;
      case "BYTE":
      case "TINYINT":
        return -6;
      case "SHORT":
      case "SMALLINT":
        return 5;
      case "LONG":
      case "BIGINT":
        return -5;
      case "NULL":
      case "VOID":
        return 0;
      case "CHAR":
      case "CHARACTER":
        return 1;
      case "VARIANT":
        return 1111;
    }
    if (s.startsWith(INTERVAL)) {
      return 12;
    }
    return 0;
  }

  private List<List<Object>> getRowsForFunctions(
      DatabricksResultSet resultSet, List<ResultColumn> columns, String catalog)
      throws SQLException {
    List<List<Object>> rows = new ArrayList<>();
    while (resultSet.next()) {
      List<Object> row = new ArrayList<>();
      for (ResultColumn column : columns) {
        if (column.getColumnName().equals("FUNCTION_CAT")) {
          row.add(catalog);
          continue;
        }
        Object object;
        try {
          object = resultSet.getObject(column.getResultSetColumnName());
          if (object == null) {
            object = NULL_STRING;
          }
        } catch (DatabricksSQLException e) {
          // Remove non-relevant columns from the obtained result set
          object = NULL_STRING;
        }
        row.add(object);
      }
      rows.add(row);
    }
    return rows;
  }

  private List<List<Object>> getRowsForSchemas(
      DatabricksResultSet resultSet,
      List<ResultColumn> columns,
      String catalog,
      IDatabricksResultSetAdapter adapter)
      throws SQLException {
    List<List<Object>> rows = new ArrayList<>();
    while (resultSet.next()) {
      // Check if this row should be included based on the adapter's filter
      if (!adapter.includeRow(resultSet, columns)) {
        continue;
      }

      List<Object> row = new ArrayList<>();
      for (ResultColumn column : columns) {
        // Map the expected column on client to column in the result set using the adapter
        ResultColumn mappedColumn = adapter.mapColumn(column);

        if (mappedColumn
            .getResultSetColumnName()
            .equals(CATALOG_RESULT_COLUMN.getResultSetColumnName())) {
          try {
            resultSet.findColumn(mappedColumn.getResultSetColumnName());
          } catch (SQLException e) {
            // Result set does not have a catalog column
            // Manually add the catalog and move to next column
            row.add(catalog);
            continue;
          }
        }

        Object object;
        try {
          object = resultSet.getObject(mappedColumn.getResultSetColumnName());
          if (object == null) {
            object = NULL_STRING;
          }
        } catch (DatabricksSQLException e) {
          // Remove non-relevant columns from the obtained result set
          object = NULL_STRING;
        }
        row.add(object);
      }
      rows.add(row);
    }
    return rows;
  }

  private DatabricksResultSet buildResultSet(
      List<ResultColumn> columns,
      List<List<Object>> rows,
      String statementId,
      CommandName commandName) {
    List<ResultColumn> nonNullableColumns =
        NON_NULLABLE_COLUMNS_MAP.getOrDefault(
            commandName, new ArrayList<>()); // Get non-nullable columns
    List<Nullable> nullableList = new ArrayList<>();
    for (ResultColumn column : columns) {
      if (nonNullableColumns.contains(column)) {
        nullableList.add(Nullable.NO_NULLS);
      } else {
        nullableList.add(Nullable.NULLABLE);
      }
    }

    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId(statementId),
        columns.stream().map(ResultColumn::getColumnName).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnTypeString).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnTypeInt).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnPrecision).collect(Collectors.toList()),
        nullableList,
        rows,
        StatementType.METADATA);
  }

  private DatabricksResultSet buildResultSet(
      List<ResultColumn> columns,
      List<List<Object>> rows,
      String statementId,
      ResultSetMetaData metaData,
      CommandName commandName)
      throws SQLException {

    // Create a map of resultSetColumnName to index from ResultSetMetaData for fast lookup
    Map<String, Integer> metaDataColumnMap = new HashMap<>();
    for (int i = 1; i <= metaData.getColumnCount(); i++) {
      metaDataColumnMap.put(metaData.getColumnName(i), i);
    }

    List<ColumnMetadata> columnMetadataList = new ArrayList<>();
    List<ResultColumn> nonNullableColumns =
        NON_NULLABLE_COLUMNS_MAP.get(commandName); // Get non-nullable columns

    for (ResultColumn column : columns) {
      String columnName = column.getColumnName();
      String resultSetColumnName = column.getResultSetColumnName();
      String typeText = column.getColumnTypeString();
      int typeInt = column.getColumnTypeInt();
      // Lookup the column index in the metadata using the map
      Integer metaColumnIndex = metaDataColumnMap.get(resultSetColumnName);

      // Check if the column is nullable
      int nullable =
          (nonNullableColumns != null && nonNullableColumns.contains(column))
              ? ResultSetMetaData.columnNoNulls
              : ResultSetMetaData.columnNullable;

      // Fetch metadata from ResultSetMetaData or use default values from the ResultColumn
      int precision =
          metaColumnIndex != null
                  && metaData.getPrecision(metaColumnIndex) != 0
                  && (typeInt == Types.DECIMAL || typeInt == Types.NUMERIC)
              ? metaData.getPrecision(metaColumnIndex)
              : column.getColumnPrecision();

      int scale =
          metaColumnIndex != null
                  && metaData.getScale(metaColumnIndex) != 0
                  && (typeInt == Types.DECIMAL || typeInt == Types.NUMERIC)
              ? metaData.getScale(metaColumnIndex)
              : column.getColumnScale();

      ColumnMetadata columnMetadata =
          new ColumnMetadata.Builder()
              .name(columnName)
              .typeText(typeText)
              .typeInt(typeInt)
              .precision(precision)
              .scale(scale)
              .nullable(nullable)
              .build();

      columnMetadataList.add(columnMetadata);
    }

    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        new StatementId(statementId),
        columnMetadataList,
        rows,
        StatementType.METADATA);
  }

  public DatabricksResultSet getCatalogsResult(List<List<Object>> rows) {
    return buildResultSet(
        CATALOG_COLUMNS,
        getThriftRows(rows, CATALOG_COLUMNS),
        GET_CATALOGS_STATEMENT_ID,
        CommandName.LIST_CATALOGS);
  }

  public DatabricksResultSet getSchemasResult(List<List<Object>> rows) {
    return buildResultSet(
        SCHEMA_COLUMNS,
        getThriftRows(rows, SCHEMA_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.LIST_SCHEMAS);
  }

  public DatabricksResultSet getCrossRefsResult(List<List<Object>> rows) {
    return buildResultSet(
        CROSS_REFERENCE_COLUMNS,
        getThriftRows(rows, CROSS_REFERENCE_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.GET_CROSS_REFERENCE);
  }

  public DatabricksResultSet getImportedKeys(List<List<Object>> rows) {
    return buildResultSet(
        IMPORTED_KEYS_COLUMNS,
        getThriftRows(rows, IMPORTED_KEYS_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.GET_IMPORTED_KEYS);
  }

  public DatabricksResultSet getExportedKeys(List<List<Object>> rows) {
    return buildResultSet(
        EXPORTED_KEYS_COLUMNS,
        getThriftRows(rows, EXPORTED_KEYS_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.GET_EXPORTED_KEYS);
  }

  public DatabricksResultSet getResultSetWithGivenRowsAndColumns(
      List<ResultColumn> columns,
      List<List<Object>> rows,
      String statementId,
      CommandName commandName) {
    return buildResultSet(columns, rows, statementId, commandName);
  }

  public DatabricksResultSet getTablesResult(
      String catalog, String[] tableTypes, List<List<Object>> rows) {
    List<List<Object>> updatedRows = new ArrayList<>();
    for (List<Object> row : rows) {
      // If the catalog is not null and the catalog does not match, skip the row
      if (catalog != null && !row.get(0).toString().equals(catalog)) {
        continue;
      }

      // If the table type is empty or null, set it to "TABLE"
      Object tableType = row.get(3);
      if (tableType == null || tableType.equals("")) {
        row.set(3, "TABLE");
      }

      if (tableTypes != null && tableTypes.length > 0) {
        // If the table type is not in the list of allowed table types, skip the row
        if (!Arrays.asList(tableTypes).contains(row.get(3).toString())) {
          continue;
        }
      }
      updatedRows.add(row);
    }
    // sort in order TABLE_TYPE, CATALOG_NAME, SCHEMA_NAME, TABLE_NAME
    updatedRows.sort(
        Comparator.comparing((List<Object> r) -> (String) r.get(3))
            .thenComparing(r -> (String) r.get(0))
            .thenComparing(r -> (String) r.get(1))
            .thenComparing(r -> (String) r.get(2)));

    return buildResultSet(
        TABLE_COLUMNS,
        getThriftRows(updatedRows, TABLE_COLUMNS),
        GET_TABLES_STATEMENT_ID,
        CommandName.LIST_TABLES);
  }

  public DatabricksResultSet getColumnsResult(List<List<Object>> rows) {
    return buildResultSet(
        COLUMN_COLUMNS,
        getThriftRows(rows, COLUMN_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.LIST_COLUMNS);
  }

  // process resultData from thrift to construct complete result set
  List<List<Object>> getThriftRows(List<List<Object>> rows, List<ResultColumn> columns) {
    if (rows == null || rows.isEmpty()) {
      return new ArrayList<>();
    }
    List<List<Object>> updatedRows = new ArrayList<>();
    for (List<Object> row : rows) {
      List<Object> updatedRow = new ArrayList<>();
      String typeVal = null;
      int col_type_index = columns.indexOf(COLUMN_TYPE_COLUMN); // only relevant for getColumns
      if (col_type_index != -1) {
        typeVal = (String) row.get(col_type_index);
      }
      for (ResultColumn column : columns) {
        if (NULL_COLUMN_COLUMNS.contains(column) || NULL_TABLE_COLUMNS.contains(column)) {
          updatedRow.add(null);
          continue;
        }
        Object object;
        switch (column.getColumnName()) {
          case "SQL_DATA_TYPE":
            if (typeVal == null) { // safety check
              object = null;
            } else {
              // Check if geospatial support is disabled and this is a geospatial type
              if (!ctx.isGeoSpatialSupportEnabled() && isGeospatialType(typeVal)) {
                object = Types.VARCHAR;
              } else if (!ctx.isComplexDatatypeSupportEnabled() && isComplexType(typeVal)) {
                // Check if complex datatype support is disabled and this is a complex type
                object = Types.VARCHAR;
              } else {
                object = getCode(stripBaseTypeName(typeVal));
              }
            }
            break;
          case "SQL_DATETIME_SUB":
            // check if typeVal is a date/time related field
            if (typeVal != null
                && (stripBaseTypeName(typeVal).contains(DATE_TYPE)
                    || stripBaseTypeName(typeVal).contains(TIMESTAMP_TYPE))) {
              object = getCode(stripBaseTypeName(typeVal));
            } else {
              object = null;
            }
            break;
          case "ORDINAL_POSITION":
            int ordinalPositionIndex = columns.indexOf(ORDINAL_POSITION_COLUMN);
            object = (int) row.get(ordinalPositionIndex) + 1; // 1-based index
            break;
          case "COLUMN_DEF":
            object = row.get(columns.indexOf(COLUMN_TYPE_COLUMN));
            break;
          default:
            int index = columns.indexOf(column);
            if (index >= row.size()) { // index out of bound (eg: IS_GENERATED_COL in getColumns)
              object = null;
            } else {
              object = row.get(index);
              if (column.getColumnName().equals(IS_NULLABLE_COLUMN.getColumnName())) {
                object = row.get(columns.indexOf(NULLABLE_COLUMN));
                if (object.equals(0)) {
                  object = "NO";
                } else {
                  object = "YES";
                }
              }
              if (column.getColumnName().equals(DECIMAL_DIGITS_COLUMN.getColumnName())) {
                object = getUpdatedDecimalDigits(stripBaseTypeName(typeVal), object);
              }
              if (column.getColumnName().equals(NUM_PREC_RADIX_COLUMN.getColumnName())) {
                if (object == null) {
                  object = 0;
                }
              }
              if (column.getColumnName().equals(REMARKS_COLUMN.getColumnName())) {
                if (object == null) {
                  object = "";
                }
              }
              if (column.getColumnName().equals(DATA_TYPE_COLUMN.getColumnName())) {
                // Check if complex datatype support is disabled and this is a complex type
                if (!ctx.isComplexDatatypeSupportEnabled() && isComplexType(typeVal)) {
                  object = Types.VARCHAR;
                } else {
                  object = getCode(stripBaseTypeName(typeVal));
                }
              }
              if (column.getColumnName().equals(CHAR_OCTET_LENGTH_COLUMN.getColumnName())) {
                object = getCharOctetLength(typeVal);
                if (object.equals(0)) {
                  object = null;
                }
              }
              if (column.getColumnName().equals(BUFFER_LENGTH_COLUMN.getColumnName())) {
                object = getBufferLength(typeVal);
              }
              if (column.getColumnName().equals(TABLE_TYPE_COLUMN.getColumnName())
                  && (object == null || object.equals(""))) {
                object = "TABLE";
              }

              // Handle TYPE_NAME separately for potential modifications
              if (column.getColumnName().equals(COLUMN_TYPE_COLUMN.getColumnName())) {
                if (typeVal != null
                    && (typeVal.contains(MEASURE)
                        || typeVal.contains(ARRAY_TYPE)
                        || typeVal.contains(MAP_TYPE)
                        || typeVal.contains(STRUCT_TYPE))) {
                  object = typeVal;
                } else {
                  object = stripBaseTypeName(typeVal);
                }
              }
              // Set COLUMN_SIZE to 255 if it's not present
              if (column.getColumnName().equals(COLUMN_SIZE_COLUMN.getColumnName())) {
                object = getColumnSize(typeVal);
              }
            }
            break;
        }

        // Add the object to the current row
        updatedRow.add(object);
      }
      updatedRows.add(updatedRow);
    }
    return updatedRows;
  }

  public DatabricksResultSet getPrimaryKeysResult(List<List<Object>> rows) {
    return buildResultSet(
        PRIMARY_KEYS_COLUMNS,
        getThriftRows(rows, PRIMARY_KEYS_COLUMNS),
        METADATA_STATEMENT_ID,
        CommandName.LIST_PRIMARY_KEYS);
  }

  public DatabricksResultSet getFunctionsResult(String catalog, List<List<Object>> rows) {
    // set FUNCTION_CAT col to be catalog for all rows
    if (rows != null) { // check for EmptyMetadataClient result
      rows.forEach(row -> row.set(FUNCTION_COLUMNS.indexOf(FUNCTION_CATALOG_COLUMN), catalog));
    }
    return buildResultSet(
        FUNCTION_COLUMNS,
        getThriftRows(rows, FUNCTION_COLUMNS),
        GET_FUNCTIONS_STATEMENT_ID,
        CommandName.LIST_FUNCTIONS);
  }
}
