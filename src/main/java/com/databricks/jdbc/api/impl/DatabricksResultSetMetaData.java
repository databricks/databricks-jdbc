package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getTypeFromTypeDesc;

import com.databricks.jdbc.common.AccessType;
import com.databricks.jdbc.common.Nullable;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.WrapperUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ColumnMetadata;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.sdk.service.sql.ColumnInfo;
import com.databricks.sdk.service.sql.ColumnInfoTypeName;
import com.databricks.sdk.service.sql.Format;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DatabricksResultSetMetaData implements ResultSetMetaData {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksResultSetMetaData.class);
  private final String statementId;
  private final ImmutableList<ImmutableDatabricksColumn> columns;
  private final ImmutableMap<String, Integer> columnNameIndex;
  private final long totalRows;
  private Long chunkCount;
  private final boolean isCloudFetchUsed;

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for a SEA result set.
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param resultManifest the manifest containing metadata about the result set, including column
   *     information and types
   */
  public DatabricksResultSetMetaData(String statementId, ResultManifest resultManifest) {
    this.statementId = statementId;
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();

    int currIndex = 0;
    if (resultManifest.getIsVolumeOperation() != null && resultManifest.getIsVolumeOperation()) {
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(VOLUME_OPERATION_STATUS_COLUMN_NAME)
          .columnType(Types.VARCHAR)
          .columnTypeText(ColumnInfoTypeName.STRING.name())
          .typePrecision(0)
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.STRING))
          .displaySize(DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.STRING, 0))
          .isSigned(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.STRING));
      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(VOLUME_OPERATION_STATUS_COLUMN_NAME, ++currIndex);
    } else {
      if (resultManifest.getSchema().getColumnCount() > 0) {
        for (ColumnInfo columnInfo : resultManifest.getSchema().getColumns()) {
          ColumnInfoTypeName columnTypeName = columnInfo.getTypeName();
          int columnType = DatabricksTypeUtil.getColumnType(columnTypeName);
          int[] scaleAndPrecision = getScaleAndPrecision(columnInfo, columnType);
          int precision = scaleAndPrecision[0];
          int scale = scaleAndPrecision[1];
          ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
          columnBuilder
              .columnName(columnInfo.getName())
              .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
              .columnType(columnType)
              .columnTypeText(columnInfo.getTypeText())
              .typePrecision(precision)
              .typeScale(scale)
              .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision))
              .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
          columnsBuilder.add(columnBuilder.build());
          // Keep index starting from 1, to be consistent with JDBC convention
          columnNameToIndexMap.putIfAbsent(columnInfo.getName(), ++currIndex);
        }
      }
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = ImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = resultManifest.getTotalRowCount();
    this.chunkCount = resultManifest.getTotalChunkCount();
    this.isCloudFetchUsed = getIsCloudFetchFromManifest(resultManifest);
  }

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for a Thrift-based result set.
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param resultManifest the response containing metadata about the result set, including column
   *     information and types, obtained through the Thrift protocol
   * @param rows the total number of rows in the result set
   * @param chunkCount the total number of data chunks in the result set
   */
  public DatabricksResultSetMetaData(
      String statementId, TGetResultSetMetadataResp resultManifest, long rows, long chunkCount) {
    this.statementId = statementId;
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    LOGGER.debug(
        String.format(
            "Result manifest for statement {%s} has schema: {%s}",
            statementId, resultManifest.getSchema()));
    int currIndex = 0;
    if (resultManifest.isSetIsStagingOperation() && resultManifest.isIsStagingOperation()) {
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(VOLUME_OPERATION_STATUS_COLUMN_NAME)
          .columnType(Types.VARCHAR)
          .columnTypeText(ColumnInfoTypeName.STRING.name())
          .typePrecision(0)
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.STRING))
          .displaySize(DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.STRING, 0))
          .isSigned(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.STRING));
      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(VOLUME_OPERATION_STATUS_COLUMN_NAME, ++currIndex);
    } else {
      if (resultManifest.getSchema() != null && resultManifest.getSchema().getColumnsSize() > 0) {
        for (TColumnDesc columnInfo : resultManifest.getSchema().getColumns()) {
          ColumnInfoTypeName columnTypeName = getTypeFromTypeDesc(columnInfo.getTypeDesc());
          int columnType = DatabricksTypeUtil.getColumnType(columnTypeName);
          int[] scaleAndPrecision = getScaleAndPrecision(columnInfo, columnType);
          int precision = scaleAndPrecision[0];
          int scale = scaleAndPrecision[1];

          ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
          columnBuilder
              .columnName(columnInfo.getColumnName())
              .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
              .columnType(columnType)
              .columnTypeText(columnTypeName.name())
              .typePrecision(precision)
              .typeScale(scale)
              .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision))
              .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
          columnsBuilder.add(columnBuilder.build());
          columnNameToIndexMap.putIfAbsent(columnInfo.getColumnName(), ++currIndex);
        }
      }
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = ImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = rows;
    this.chunkCount = chunkCount;
    this.isCloudFetchUsed = getIsCloudFetchFromManifest(resultManifest);
  }

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for metadata result set (SEA Flow)
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param columnMetadataList the list containing metadata for each column in the result set, such
   *     as column names, types, and precision
   * @param totalRows the total number of rows in the result set
   */
  public DatabricksResultSetMetaData(
      String statementId, List<ColumnMetadata> columnMetadataList, long totalRows) {
    this.statementId = statementId;
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();

    for (int i = 0; i < columnMetadataList.size(); i++) {
      ColumnMetadata metadata = columnMetadataList.get(i);
      ColumnInfoTypeName columnTypeName =
          ColumnInfoTypeName.valueOf(
              DatabricksTypeUtil.getDatabricksTypeFromSQLType(metadata.getTypeInt()));
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(metadata.getName())
          .columnType(metadata.getTypeInt())
          .columnTypeText(metadata.getTypeText())
          .typePrecision(metadata.getPrecision())
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
          .typeScale(metadata.getScale())
          .nullable(DatabricksTypeUtil.getNullableFromValue(metadata.getNullable()))
          .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, metadata.getPrecision()))
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));

      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(metadata.getName(), i + 1); // JDBC index starts from 1
    }

    this.columns = columnsBuilder.build();
    this.columnNameIndex = ImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = totalRows;
    this.isCloudFetchUsed = false;
  }

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for metadata result set (Thrift Flow)
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param columnNames names of each column
   * @param columnTypeText type text of each column
   * @param columnTypes types of each column
   * @param columnTypePrecisions precisions of each column
   * @param totalRows total number of rows in result set
   */
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
    this.isCloudFetchUsed = false;
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

  public boolean getIsCloudFetchUsed() {
    return isCloudFetchUsed;
  }

  private boolean getIsCloudFetchFromManifest(ResultManifest resultManifest) {
    return resultManifest.getFormat() == Format.ARROW_STREAM;
  }

  private boolean getIsCloudFetchFromManifest(TGetResultSetMetadataResp resultManifest) {
    return resultManifest.getResultFormat() == TSparkRowSetType.URL_BASED_SET;
  }

  public Long getChunkCount() {
    return chunkCount;
  }

  public int[] getScaleAndPrecision(ColumnInfo columnInfo, int columnType) {
    int precision = DatabricksTypeUtil.getPrecision(columnType);
    int scale = DatabricksTypeUtil.getScale(columnType);
    if (columnInfo.getTypePrecision() != null) {
      precision = Math.toIntExact(columnInfo.getTypePrecision());
      scale = Math.toIntExact(columnInfo.getTypeScale());
    }
    return new int[] {precision, scale};
  }

  public int[] getScaleAndPrecision(TColumnDesc columnInfo, int columnType) {
    int precision = DatabricksTypeUtil.getPrecision(columnType);
    int scale = DatabricksTypeUtil.getScale(columnType);
    if (columnInfo.getTypeDesc() != null && columnInfo.getTypeDesc().getTypesSize() > 0) {
      TTypeEntry tTypeEntry = columnInfo.getTypeDesc().getTypes().get(0);
      if (tTypeEntry.isSetPrimitiveEntry()
          && tTypeEntry.getPrimitiveEntry().isSetTypeQualifiers()
          && tTypeEntry.getPrimitiveEntry().getTypeQualifiers().isSetQualifiers()) {
        Map<String, TTypeQualifierValue> qualifiers =
            tTypeEntry.getPrimitiveEntry().getTypeQualifiers().getQualifiers();
        scale = qualifiers.get("scale").getI32Value();
        precision = qualifiers.get("precision").getI32Value();
      }
    }
    return new int[] {precision, scale};
  }

  private ImmutableDatabricksColumn.Builder getColumnBuilder() {
    return ImmutableDatabricksColumn.builder()
        .isAutoIncrement(false)
        .isSearchable(false)
        .nullable(Nullable.NULLABLE)
        .accessType(AccessType.READ_ONLY)
        .isDefinitelyWritable(false)
        .schemaName(EMPTY_STRING)
        .tableName(EMPTY_STRING)
        .catalogName(EMPTY_STRING)
        .isCurrency(false)
        .typeScale(0)
        .isCaseSensitive(false);
  }
}
