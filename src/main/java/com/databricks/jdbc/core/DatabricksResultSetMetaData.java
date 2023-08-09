package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

public class DatabricksResultSetMetaData implements ResultSetMetaData {

  private final String statementId;
  private final ImmutableList<ImmutableDatabricksColumn> columns;
  private final ImmutableMap<String, Integer> columnNameIndex;

  // TODO: Add handling for Arrow stream results
  public DatabricksResultSetMetaData(String statementId, ResultManifest resultManifest) {
    this.statementId = statementId;

    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<String, Integer> columnIndexBuilder = ImmutableMap.builder();
    int currIndex = 0;
    for (ColumnInfo columnInfo : resultManifest.getSchema().getColumns()) {
      ImmutableDatabricksColumn.Builder columnBuilder = ImmutableDatabricksColumn.builder()
          .columnName(columnInfo.getName())
          .columnType(getColumnType(columnInfo.getTypeName()))
          .columnTypeText(columnInfo.getTypeText());
      if (columnInfo.getTypePrecision() != null) {
        columnBuilder.typePrecision(columnInfo.getTypePrecision().intValue());
      } else if (columnInfo.getTypeName().equals(ColumnInfoTypeName.STRING)) {
        columnBuilder.typePrecision(255);
      } else {
        columnBuilder.typePrecision(0);
      }
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnIndexBuilder.put(columnInfo.getName(), ++currIndex);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = columnIndexBuilder.build();
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int isNullable(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getColumnLabel(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).columnName();
  }

  @Override
  public String getColumnName(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).columnName();
  }

  @Override
  public String getSchemaName(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).typePrecision();
  }

  @Override
  public int getScale(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getTableName(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getColumnType(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).columnType();
  }

  @Override
  public String getColumnTypeName(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).columnTypeText();
  }

  @Override
  public boolean isReadOnly(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  private int getColumnType(ColumnInfoTypeName typeName) {
    switch (typeName) {
      case BYTE:
        return Types.TINYINT;
      case SHORT:
        return Types.SMALLINT;
      case INT:
        return Types.INTEGER;
      case LONG:
        return Types.BIGINT;
      case FLOAT:
        return Types.FLOAT;
      case DOUBLE:
        return Types.DOUBLE;
      case DECIMAL:
        return Types.DECIMAL;
      case BINARY:
        return Types.BINARY;
      case BOOLEAN:
        return Types.BOOLEAN;
      case CHAR:
        return Types.CHAR;
      case STRING:
        return Types.VARCHAR;
      case TIMESTAMP:
        return Types.TIMESTAMP;
      case DATE:
        return Types.DATE;
      case STRUCT:
        return Types.STRUCT;
      case ARRAY:
        return Types.ARRAY;
      case NULL:
        return Types.NULL;
      default:
        throw new IllegalStateException("Unknown column type: " + typeName);
    }
  }

  private int getEffectiveIndex(int columnIndex) {
    if (columnIndex > 0 && columnIndex <= columns.size()) {
      return columnIndex - 1;
    } else {
      throw new IllegalStateException("Invalid column index: " + columnIndex);
    }
  }

  /**
   * Returns index of column-name in metadata starting from 1
   * @param columnName column-name
   * @return index of column if exists, else -1
   */
  public int getColumnNameIndex(String columnName) {
    return columnNameIndex.getOrDefault(columnName, -1);
  }
}
