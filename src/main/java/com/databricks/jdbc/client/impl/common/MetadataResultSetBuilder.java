package com.databricks.jdbc.client.impl.common;

import static com.databricks.jdbc.client.impl.common.CommandConstants.*;
import static com.databricks.jdbc.common.MetadataResultConstants.*;

import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import com.google.common.annotations.VisibleForTesting;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataResultSetBuilder {
  public static DatabricksResultSet getFunctionsResult(ResultSet resultSet, String catalog)
      throws SQLException {
    List<List<Object>> rows = getRowsForFunctions(resultSet, FUNCTION_COLUMNS, catalog);
    return buildResultSet(FUNCTION_COLUMNS, rows, GET_FUNCTIONS_STATEMENT_ID);
  }

  public static DatabricksResultSet getColumnsResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, COLUMN_COLUMNS);
    return buildResultSet(COLUMN_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getCatalogsResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, CATALOG_COLUMNS);
    return buildResultSet(CATALOG_COLUMNS, rows, GET_CATALOGS_STATEMENT_ID);
  }

  public static DatabricksResultSet getSchemasResult(ResultSet resultSet, String catalog)
      throws SQLException {
    List<List<Object>> rows = getRowsForSchemas(resultSet, SCHEMA_COLUMNS, catalog);
    return buildResultSet(SCHEMA_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getTablesResult(ResultSet resultSet, String[] tableTypes)
      throws SQLException {
    List<String> allowedTableTypes = List.of(tableTypes);
    List<List<Object>> rows =
        getRows(resultSet, TABLE_COLUMNS).stream()
            .filter(row -> allowedTableTypes.contains(row.get(3))) // Filtering based on table type
            .collect(Collectors.toList());
    return buildResultSet(TABLE_COLUMNS, rows, GET_TABLES_STATEMENT_ID);
  }

  public static DatabricksResultSet getTableTypesResult() {
    return buildResultSet(TABLE_TYPE_COLUMNS, TABLE_TYPES_ROWS, GET_TABLE_TYPE_STATEMENT_ID);
  }

  public static DatabricksResultSet getTableTypesResult(List<List<Object>> rows) {
    return buildResultSet(TABLE_TYPE_COLUMNS, rows, GET_TABLE_TYPE_STATEMENT_ID);
  }

  public static DatabricksResultSet getTypeInfoResult(List<List<Object>> rows) {
    return buildResultSet(TYPE_INFO_COLUMNS, rows, GET_TYPE_INFO_STATEMENT_ID);
  }

  public static DatabricksResultSet getPrimaryKeysResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, PRIMARY_KEYS_COLUMNS);
    return buildResultSet(PRIMARY_KEYS_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  private static List<List<Object>> getRows(ResultSet resultSet, List<ResultColumn> columns)
      throws SQLException {
    List<List<Object>> rows = new ArrayList<>();

    while (resultSet.next()) {
      List<Object> row = new ArrayList<>();
      for (ResultColumn column : columns) {
        Object object = null;
        switch (column.getColumnName()) {
          case "SQL_DATA_TYPE":
          case "SQL_DATETIME_SUB":
            object = 0;
            break;
          default:
            // If column does not match any of the special cases, try to get it from the ResultSet
            try {
              object = resultSet.getObject(column.getResultSetColumnName());
              if (column.getColumnName().equals("IS_NULLABLE")) {
                if (object == null || object.equals("true")) {
                  object = "YES";
                } else {
                  object = "NO";
                }
              }
              if (column.getColumnName().equals("DECIMAL_DIGITS")
                  || column.getColumnName().equals("NUM_PREC_RADIX")) {
                if (object == null) {
                  object = 0;
                }
              }
            } catch (SQLException e) {
              if (column.getColumnName().equals("DATA_TYPE")) {
                String typeVal = resultSet.getString("columnType");
                if (typeVal.contains("(")) typeVal = typeVal.substring(0, typeVal.indexOf('('));
                object = getCode(typeVal);
              } else if (column.getColumnName().equals("CHAR_OCTET_LENGTH")) {
                String typeVal = resultSet.getString("columnType");

                object = getCharOctetLength(typeVal);
              } else {
                // Handle other cases where the result set does not contain the expected column
                object = null;
              }
            }
            // Handle TYPE_NAME separately for potential modifications
            if (column.getColumnName().equals("TYPE_NAME")) {
              object = stripTypeName((String) object);
            }
            // Set COLUMN_SIZE to 255 if it's not present
            if (column.getColumnName().equals("COLUMN_SIZE") && object == null) {
              object = 255;
            }

            if (column.getColumnName().equals("BUFFER_LENGTH")) {
              String typeVal = resultSet.getString("columnType");
              int columnSize =
                  (resultSet.getObject("columnSize") == null) ? 0 : resultSet.getInt("columnSize");
              object = getBufferLength(typeVal, columnSize);
            }
            break;
        }

        // Add the object to the current row
        row.add(object);
      }
      rows.add(row);
    }
    return rows;
  }

  /**
   * Extracts the character octet length from a given SQL type definition. For example, for input
   * "VARCHAR(100)", it returns 100. For inputs without a specified length or invalid inputs, it
   * returns 0.
   *
   * @param typeVal the SQL type definition
   * @return the character octet length or 0 if not applicable
   */
  static int getBufferLength(String typeVal, int columnSize) {
    if (typeVal == null || typeVal.isEmpty()) {
      return 0;
    }
    if (!typeVal.contains("(")) {
      if (typeVal.equals("DATE")) {
        return 6;
      }
      if (typeVal.equals("TIMESTAMP")) {
        return 16;
      }
      if (typeVal.equals("BINARY")) {
        return 32767;
      }
      if (typeVal.equals("STRING")) {
        return 255;
      }
      return columnSize;
    }

    String[] lengthConstraints = typeVal.substring(typeVal.indexOf('(') + 1).split("[,)]");
    if (lengthConstraints.length == 0) {
      return 0;
    }
    String max_char_length = lengthConstraints[0].trim();
    try {
      if (typeVal.contains("CHAR") || typeVal.contains("VARCHAR") || typeVal.contains("STRING"))
        return Integer.parseInt(max_char_length);
      else return 4 * Integer.parseInt(max_char_length);
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  static int getCharOctetLength(String typeVal) {
    //    if (typeVal == null || !typeVal.contains("(")) return 0;
    if (typeVal == null
        || !(typeVal.contains("CHAR")
            || typeVal.contains("STRING")
            || typeVal.contains("VARCHAR")
            || typeVal.contains("TEXT")
            || typeVal.contains("BINARY"))) return 0;

    if (!typeVal.contains("(")) {
      if (typeVal.contains("BINARY")) {
        return 32767;
      } else {
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
  static String stripTypeName(String typeName) {
    if (typeName == null) {
      return null;
    }
    int typeArgumentIndex = typeName.indexOf('(');
    if (typeArgumentIndex != -1) {
      return typeName.substring(0, typeName.indexOf('('));
    }

    return typeName;
  }

  static int getCode(String s) {
    switch (s) {
      case "STRING":
        return 12;
      case "INT":
        return 4;
      case "DOUBLE":
        return 8;
      case "FLOAT":
        return 6;
      case "BOOLEAN":
        return 16;
      case "DATE":
        return 91;
      case "TIMESTAMP":
        return 93;
      case "DECIMAL":
        return 3;
      case "BINARY":
        return -2;
      case "ARRAY":
        return 2003;
      case "MAP":
        return 2002;
      case "STRUCT":
        return 2002;
      case "UNIONTYPE":
        return 2002;
      case "BYTE":
        return -6;
      case "SHORT":
        return 5;
      case "LONG":
        return -5;
      case "NULL":
        return 0;
      case "VOID":
        return 0;
      case "CHAR":
        return 1;
      case "VARCHAR":
        return 12;
      case "CHARACTER":
        return 1;
      case "BIGINT":
        return -5;
      case "TINYINT":
        return -6;
      case "SMALLINT":
        return 5;
      case "INTEGER":
        return 4;
    }
    return 0;
  }

  private static List<List<Object>> getRowsForFunctions(
      ResultSet resultSet, List<ResultColumn> columns, String catalog) throws SQLException {
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

  private static List<List<Object>> getRowsForSchemas(
      ResultSet resultSet, List<ResultColumn> columns, String catalog) throws SQLException {
    // TODO(PECO-1677): Remove this method once the server side ResultSet metadata contains catalogs
    List<List<Object>> rows = new ArrayList<>();
    while (resultSet.next()) {
      List<Object> row = new ArrayList<>();
      for (ResultColumn column : columns) {
        if (column.getColumnName().equals("TABLE_CATALOG")) {
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

  private static DatabricksResultSet buildResultSet(
      List<ResultColumn> columns, List<List<Object>> rows, String statementId) {
    return new DatabricksResultSet(
        new StatementStatus().setState(StatementState.SUCCEEDED),
        statementId,
        columns.stream().map(ResultColumn::getColumnName).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnTypeString).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnTypeInt).collect(Collectors.toList()),
        columns.stream().map(ResultColumn::getColumnPrecision).collect(Collectors.toList()),
        rows,
        StatementType.METADATA);
  }

  public static DatabricksResultSet getCatalogsResult(List<List<Object>> rows) {
    return buildResultSet(CATALOG_COLUMNS, rows, GET_CATALOGS_STATEMENT_ID);
  }

  public static DatabricksResultSet getSchemasResult(List<List<Object>> rows) {
    return buildResultSet(SCHEMA_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getTablesResult(List<List<Object>> rows) {
    return buildResultSet(TABLE_COLUMNS_ALL_PURPOSE, rows, GET_TABLES_STATEMENT_ID);
  }

  public static DatabricksResultSet getColumnsResult(List<List<Object>> rows) {
    return buildResultSet(COLUMN_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getPrimaryKeysResult(List<List<Object>> rows) {
    return buildResultSet(PRIMARY_KEYS_COLUMNS_ALL_PURPOSE, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getFunctionsResult(List<List<Object>> rows) {
    return buildResultSet(FUNCTION_COLUMNS_ALL_PURPOSE, rows, GET_FUNCTIONS_STATEMENT_ID);
  }
}
