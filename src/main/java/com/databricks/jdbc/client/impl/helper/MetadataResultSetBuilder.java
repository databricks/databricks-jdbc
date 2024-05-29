package com.databricks.jdbc.client.impl.helper;

import static com.databricks.jdbc.client.impl.helper.CommandConstants.*;
import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.*;

import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.sdk.service.sql.StatementState;
import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class MetadataResultSetBuilder {
  public static DatabricksResultSet getFunctionsResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, FUNCTION_COLUMNS);
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

  public static DatabricksResultSet getSchemasResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, SCHEMA_COLUMNS);
    return buildResultSet(SCHEMA_COLUMNS, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getTablesResult(ResultSet resultSet) throws SQLException {
    List<List<Object>> rows = getRows(resultSet, TABLE_COLUMNS);
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
    return buildResultSet(COLUMN_COLUMNS_ALL_PURPOSE, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getPrimaryKeysResult(List<List<Object>> rows) {
    return buildResultSet(PRIMARY_KEYS_COLUMNS_ALL_PURPOSE, rows, METADATA_STATEMENT_ID);
  }

  public static DatabricksResultSet getFunctionsResult(List<List<Object>> rows) {
    return buildResultSet(FUNCTION_COLUMNS_ALL_PURPOSE, rows, GET_FUNCTIONS_STATEMENT_ID);
  }
}
