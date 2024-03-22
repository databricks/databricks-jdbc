package com.databricks.jdbc.core;

import com.databricks.jdbc.client.sqlexec.ResultManifest;
import com.databricks.jdbc.commons.util.WrapperUtil;
import com.databricks.jdbc.core.types.AccessType;
import com.databricks.jdbc.core.types.Nullable;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksResultSetMetaData implements ResultSetMetaData {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksResultSetMetaData.class);
  private final String statementId;
  private final ImmutableList<ImmutableDatabricksColumn> columns;
  private final ImmutableMap<String, Integer> columnNameIndex;
  private final long totalRows;
  private static final String DEFAULT_CATALOGUE_NAME = "Spark";
  private static final String NULL_STRING = "null";

  // TODO: Add handling for Arrow stream results

  public DatabricksResultSetMetaData(String statementId, ResultManifest resultManifest) {
    this.statementId = statementId;
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    int currIndex = 0;
    LOGGER.debug(
        "Result manifest for statement {} has schema: {}", statementId, resultManifest.getSchema());
    if (resultManifest.getSchema().getColumnCount() > 0) {
      for (ColumnInfo columnInfo : resultManifest.getSchema().getColumns()) {
        ColumnInfoTypeName columnTypeName = columnInfo.getTypeName();
        int precision = DatabricksTypeUtil.getPrecision(columnTypeName);
        ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
        columnBuilder
            .columnName(columnInfo.getName())
            .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
            .columnType(DatabricksTypeUtil.getColumnType(columnTypeName))
            .columnTypeText(columnInfo.getTypeText())
            .typePrecision(precision)
            .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision))
            .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));

        columnsBuilder.add(columnBuilder.build());
        // Keep index starting from 1, to be consistent with JDBC convention
        columnNameToIndexMap.putIfAbsent(columnInfo.getName(), ++currIndex);
      }
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = ImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = resultManifest.getTotalRowCount();
  }

  public DatabricksResultSetMetaData(
      String statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
      long totalRows) {
    this.statementId = statementId;

    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      ColumnInfoTypeName columnTypeName =
          ColumnInfoTypeName.valueOf(
              DatabricksTypeUtil.getDatabricksTypeFromSQLType(columnTypes.get(i)));
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(columnNames.get(i))
          .columnType(columnTypes.get(i))
          .columnTypeText(columnTypeText.get(i))
          .typePrecision(columnTypePrecisions.get(i))
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
          .displaySize(
              DatabricksTypeUtil.getDisplaySize(columnTypeName, columnTypePrecisions.get(i)))
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnNameToIndexMap.putIfAbsent(columnNames.get(i), i + 1);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = ImmutableMap.copyOf(columnNameToIndexMap);
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

  private ImmutableDatabricksColumn.Builder getColumnBuilder() {
    return ImmutableDatabricksColumn.builder()
        .isAutoIncrement(false)
        .isSearchable(true)
        .nullable(Nullable.NULLABLE)
        .accessType(AccessType.READ_ONLY)
        .isDefinitelyWritable(false)
        .schemaName(NULL_STRING)
        .tableName(NULL_STRING)
        .catalogName(DEFAULT_CATALOGUE_NAME)
        .isCurrency(false)
        .typeScale(0)
        .isCaseSensitive(false);
  }
}
