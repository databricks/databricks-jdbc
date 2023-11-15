package com.databricks.jdbc.core;

import com.databricks.jdbc.commons.util.WrapperUtil;
import com.databricks.jdbc.core.types.AccessType;
import com.databricks.jdbc.core.types.Nullable;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.ResultManifest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

public class DatabricksResultSetMetaData implements ResultSetMetaData {

  private final String statementId;
  private final ImmutableList<ImmutableDatabricksColumn> columns;
  private final ImmutableMap<String, Integer> columnNameIndex;
  private final long totalRows;

  // TODO: Add handling for Arrow stream results
  public DatabricksResultSetMetaData(
      String statementId, ResultManifest resultManifest, IDatabricksSession session) {
    this.statementId = statementId;

    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<String, Integer> columnIndexBuilder = ImmutableMap.builder();
    int currIndex = 0;
    for (ColumnInfo columnInfo : resultManifest.getSchema().getColumns()) {
      ColumnInfoTypeName columnTypeName = columnInfo.getTypeName();
      ImmutableDatabricksColumn.Builder columnBuilder =
          ImmutableDatabricksColumn.builder()
              .columnName(columnInfo.getName())
              .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
              .columnType(DatabricksTypeUtil.getColumnType(columnTypeName))
              .columnTypeText(columnInfo.getTypeText());
      int precision = getPrecision(columnInfo);
      columnBuilder.typePrecision(precision);
      columnBuilder.displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision));
      columnBuilder.isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      columnBuilder.isAutoIncrement(
          false); // TODO: for hive, it is false. But, we need to figure out otherwise
      columnBuilder.isSearchable(true);
      columnBuilder.nullable(
          Nullable.UNKNOWN); // TODO: add once it is introduced in columnInfo(databricks-sdk-java)
      columnBuilder.accessType(AccessType.UNKNOWN);
      columnBuilder.isDefinitelyWritable(false);
      columnBuilder.schemaName(session.getSchema());
      columnBuilder.catalogName(session.getCatalog());
      columnBuilder.typeScale(0); // TODO initialise TableName
      columnBuilder.isCaseSensitive(DatabricksTypeUtil.isCaseSensitive(columnTypeName));

      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnIndexBuilder.put(columnInfo.getName(), ++currIndex);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = columnIndexBuilder.build();
    this.totalRows = resultManifest.getTotalRowCount();
  }

  private int getPrecision(ColumnInfo columnInfo) {
    if (columnInfo.getTypePrecision() != null) {
      return columnInfo.getTypePrecision().intValue();
    } else if (columnInfo.getTypeName().equals(ColumnInfoTypeName.STRING)) {
      return 255;
    }
    return 0;
  }

  public DatabricksResultSetMetaData(
      String statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
      long totalRows) {
    this.statementId = statementId;

    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<String, Integer> columnIndexBuilder = ImmutableMap.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      ImmutableDatabricksColumn.Builder columnBuilder =
          ImmutableDatabricksColumn.builder()
              .columnName(columnNames.get(i))
              .columnType(columnTypes.get(i))
              .columnTypeText(columnTypeText.get(i))
              .typePrecision(columnTypePrecisions.get(i));
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnIndexBuilder.put(columnNames.get(i), i + 1);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = columnIndexBuilder.build();
    this.totalRows = totalRows;
  }

  public DatabricksResultSetMetaData(
      String statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
      List<Integer> columnTypeScale,
      long totalRows) {
    this.statementId = statementId;

    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    ImmutableMap.Builder<String, Integer> columnIndexBuilder = ImmutableMap.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      ImmutableDatabricksColumn.Builder columnBuilder =
          ImmutableDatabricksColumn.builder()
              .columnName(columnNames.get(i))
              .columnType(columnTypes.get(i))
              .columnTypeText(columnTypeText.get(i))
              .typeScale(columnTypeScale.get(i))
              .isCurrency(false)
              .accessType(AccessType.UNKNOWN)
              .typePrecision(columnTypePrecisions.get(i));
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnIndexBuilder.put(columnNames.get(i), i + 1);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = columnIndexBuilder.build();
    this.totalRows = totalRows;
  }

  @Override
  public int getColumnCount() throws SQLException {
    return columns.size();
  }

  @Override
  public boolean isAutoIncrement(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isAutoIncrement();
  }

  @Override
  public boolean isCaseSensitive(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isCaseSensitive();
  }

  @Override
  public boolean isSearchable(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isSearchable();
  }

  @Override
  public boolean isCurrency(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isCurrency();
  }

  @Override
  public int isNullable(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).nullable().getValue();
  }

  @Override
  public boolean isSigned(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isSigned();
  }

  @Override
  public int getColumnDisplaySize(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).displaySize();
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
    return columns.get(getEffectiveIndex(column)).schemaName();
  }

  @Override
  public int getPrecision(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).typePrecision();
  }

  @Override
  public int getScale(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).typeScale();
  }

  @Override
  public String getTableName(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).tableName();
  }

  @Override
  public String getCatalogName(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).catalogName();
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
    AccessType columnAccessType = columns.get(getEffectiveIndex(column)).accessType();
    return columnAccessType.equals(AccessType.READ_ONLY)
        || columnAccessType.equals(AccessType.UNKNOWN);
  }

  @Override
  public boolean isWritable(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).accessType().equals(AccessType.WRITE);
  }

  @Override
  public boolean isDefinitelyWritable(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).isDefinitelyWritable();
  }

  @Override
  public String getColumnClassName(int column) throws SQLException {
    return columns.get(getEffectiveIndex(column)).columnTypeClassName();
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return WrapperUtil.unwrap(iface, this);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return WrapperUtil.isWrapperFor(iface, this);
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
   *
   * @param columnName column-name
   * @return index of column if exists, else -1
   */
  public int getColumnNameIndex(String columnName) {
    return columnNameIndex.getOrDefault(columnName, -1);
  }

  public long getTotalRows() {
    return totalRows;
  }
}
