package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_STATUS_COLUMN_NAME;
import static com.databricks.jdbc.common.MetadataResultConstants.LARGE_DISPLAY_COLUMNS;
import static com.databricks.jdbc.common.MetadataResultConstants.REMARKS_COLUMN;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.AccessType;
import com.databricks.jdbc.common.Nullable;
import com.databricks.jdbc.common.util.DatabricksTypeUtil;
import com.databricks.jdbc.common.util.WrapperUtil;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.ColumnMetadata;
import com.databricks.jdbc.model.core.ResultManifest;
import com.google.common.collect.ImmutableList;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DatabricksResultSetMetaData implements ResultSetMetaData {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksResultSetMetaData.class);
  private final StatementId statementId;
  private IDatabricksConnectionContext ctx;
  private final ImmutableList<ImmutableDatabricksColumn> columns;
  private final CaseInsensitiveImmutableMap<Integer> columnNameIndex;
  private final long totalRows;
  private Long chunkCount;
  private final boolean isCloudFetchUsed;

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for a SEA result set.
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param resultManifest the manifest containing metadata about the result set, including column
   *     information and types
   * @param usesExternalLinks whether or not the resultData contains external links (cloud fetch is
   *     used)
   */
  public DatabricksResultSetMetaData(
      StatementId statementId,
      ResultManifest resultManifest,
      boolean usesExternalLinks,
      IDatabricksConnectionContext ctx) {
    this.ctx = ctx;
    this.statementId = statementId;
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    MetadataResultSetBuilder metadataResultSetBuilder = new MetadataResultSetBuilder(ctx);

    int currIndex = 0;
    if (resultManifest.getIsVolumeOperation() != null && resultManifest.getIsVolumeOperation()) {
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(VOLUME_OPERATION_STATUS_COLUMN_NAME)
          .columnType(Types.VARCHAR)
          .columnTypeText(
              ColumnInfoTypeName.STRING.name()) // status column is string. eg: SUCCEEDED
          .typePrecision(0)
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(ColumnInfoTypeName.STRING))
          .displaySize(
              DatabricksTypeUtil.getDisplaySize(
                  ColumnInfoTypeName.STRING, 0, 0)) // passing default scale, precision
          .isSearchable(true)
          .schemaName(null)
          .tableName(null)
          .isSigned(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.STRING));
      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(VOLUME_OPERATION_STATUS_COLUMN_NAME, ++currIndex);
    } else {
      if (resultManifest.getSchema().getColumnCount() > 0) {
        for (ColumnInfo columnInfo : resultManifest.getSchema().getColumns()) {
          ColumnInfoTypeName columnTypeName = columnInfo.getTypeName();
          // For TIMESTAMP_NTZ columns, getTypeName() returns null.
          // use typeText (initially "TIMESTAMP_NTZ") to identify the type,
          // overwrite it to "TIMESTAMP" to maintain parity with thrift output.
          if (columnInfo.getTypeText().equalsIgnoreCase(TIMESTAMP_NTZ)) {
            columnTypeName = ColumnInfoTypeName.TIMESTAMP;
            columnInfo.setTypeText(TIMESTAMP);
          }

          // Check if we need to convert geospatial types to string when geospatial support is
          // disabled
          String typeText = columnInfo.getTypeText();
          if (!ctx.isGeoSpatialSupportEnabled() && isGeospatialType(columnTypeName)) {
            LOGGER.debug(
                "Geospatial support is disabled, converting {} to STRING in metadata",
                columnTypeName);
            columnTypeName = ColumnInfoTypeName.STRING;
            typeText = "STRING";
          }

          // store base type eg. DECIMAL instead of DECIMAL(7,2) except for geospatial datatypes
          String finalTypeText =
              isGeospatialType(columnTypeName)
                  ? typeText
                  : metadataResultSetBuilder.stripTypeName(typeText);

          int columnType = DatabricksTypeUtil.getColumnType(columnTypeName);
          int[] precisionAndScale = getPrecisionAndScale(columnInfo, columnType);
          int precision = precisionAndScale[0];
          int scale = precisionAndScale[1];
          ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
          columnBuilder
              .columnName(columnInfo.getName())
              .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
              .columnType(columnType)
              .columnTypeText(finalTypeText)
              .typePrecision(precision)
              .typeScale(scale)
              .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision, scale))
              .isSearchable(true) // set all columns to be searchable in execute query result set
              .schemaName(
                  null) // set schema and table name to null, as server do not return these fields.
              .tableName(null)
              .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
          columnsBuilder.add(columnBuilder.build());
          // Keep index starting from 1, to be consistent with JDBC convention
          columnNameToIndexMap.putIfAbsent(columnInfo.getName(), ++currIndex);
        }
      }
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = resultManifest.getTotalRowCount();
    this.chunkCount = resultManifest.getTotalChunkCount();
    this.isCloudFetchUsed = usesExternalLinks;
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
      StatementId statementId,
      TGetResultSetMetadataResp resultManifest,
      long rows,
      long chunkCount,
      List<String> arrowMetadata,
      IDatabricksConnectionContext ctx) {
    this.ctx = ctx;
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
          .displaySize(DatabricksTypeUtil.getDisplaySize(ColumnInfoTypeName.STRING, 0, 0))
          .isSearchable(true)
          .schemaName(null)
          .tableName(null)
          .isSigned(DatabricksTypeUtil.isSigned(ColumnInfoTypeName.STRING));
      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(VOLUME_OPERATION_STATUS_COLUMN_NAME, ++currIndex);
    } else {
      if (resultManifest.getSchema() != null && resultManifest.getSchema().getColumnsSize() > 0) {
        for (int columnIndex = 0;
            columnIndex < resultManifest.getSchema().getColumnsSize();
            columnIndex++) {
          TColumnDesc columnDesc = resultManifest.getSchema().getColumns().get(columnIndex);

          String columnArrowMetadata =
              arrowMetadata != null && columnIndex < arrowMetadata.size()
                  ? arrowMetadata.get(columnIndex)
                  : null;
          ColumnInfo columnInfo = getColumnInfoFromTColumnDesc(columnDesc, columnArrowMetadata);
          int[] precisionAndScale = getPrecisionAndScale(columnInfo);
          int precision = precisionAndScale[0];
          int scale = precisionAndScale[1];

          ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
          // Use arrowMetadata for type text if available, as it contains full type information
          // like "ARRAY<INT>" whereas TTypeDesc only has primitive "ARRAY_TYPE"
          String columnTypeText =
              (arrowMetadata != null
                      && columnIndex < arrowMetadata.size()
                      && arrowMetadata.get(columnIndex) != null)
                  ? arrowMetadata.get(columnIndex)
                  : getTypeTextFromTypeDesc(columnDesc.getTypeDesc());

          // Normalize TIMESTAMP_NTZ to TIMESTAMP for consistency with SEA path
          if (columnTypeText != null && columnTypeText.equalsIgnoreCase(TIMESTAMP_NTZ)) {
            columnTypeText = TIMESTAMP;
          }

          columnBuilder
              .columnName(columnInfo.getName())
              .columnTypeClassName(
                  DatabricksTypeUtil.getColumnTypeClassName(columnInfo.getTypeName()))
              .columnType(DatabricksTypeUtil.getColumnType(columnInfo.getTypeName()))
              .columnTypeText(columnTypeText)
              // columnInfoTypeName does not have BIGINT, SMALLINT. Extracting from thriftType in
              // typeDesc
              .typePrecision(precision)
              .typeScale(scale)
              .displaySize(
                  DatabricksTypeUtil.getDisplaySize(columnInfo.getTypeName(), precision, scale))
              .isSearchable(true)
              .schemaName(null)
              .tableName(null)
              .isSigned(DatabricksTypeUtil.isSigned(columnInfo.getTypeName()));
          if (isVariantColumn(arrowMetadata, columnIndex)) {
            columnBuilder
                .columnTypeClassName("java.lang.String")
                .columnType(Types.OTHER)
                .columnTypeText(VARIANT);
          } else if ((isGeometryColumn(arrowMetadata, columnIndex)
                  || isGeographyColumn(arrowMetadata, columnIndex))
              && !ctx.isGeoSpatialSupportEnabled()) {
            // Convert geospatial types to STRING when support is disabled
            LOGGER.debug(
                "Geospatial support is disabled, converting column {} to STRING in Thrift metadata",
                columnInfo.getName());
            columnBuilder
                .columnTypeClassName("java.lang.String")
                .columnType(Types.VARCHAR)
                .columnTypeText("STRING");
          }
          columnsBuilder.add(columnBuilder.build());
          columnNameToIndexMap.putIfAbsent(columnInfo.getName(), ++currIndex);
        }
      }
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
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
      StatementId statementId, List<ColumnMetadata> columnMetadataList, long totalRows) {
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
          .displaySize(
              DatabricksTypeUtil.getDisplaySize(
                  metadata.getTypeInt(),
                  metadata.getPrecision())) // pass scale and precision from metadata result set
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      if (isLargeColumn(
          metadata.getName())) { // special case: overriding default value of 128 for VARCHAR cols.
        columnBuilder.typePrecision(254);
        columnBuilder.displaySize(254);
      }

      columnsBuilder.add(columnBuilder.build());
      columnNameToIndexMap.putIfAbsent(metadata.getName(), i + 1); // JDBC index starts from 1
    }

    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
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
   * @param columnNullables nullable value of each column
   * @param totalRows total number of rows in result set
   */
  public DatabricksResultSetMetaData(
      StatementId statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      List<Integer> columnTypes,
      List<Integer> columnTypePrecisions,
      List<Nullable> columnNullables,
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
              DatabricksTypeUtil.getDisplaySize(columnTypes.get(i), columnTypePrecisions.get(i)))
          .nullable(columnNullables.get(i))
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      if (isLargeColumn(columnNames.get(i))) {
        columnBuilder.typePrecision(254);
        columnBuilder.displaySize(254);
      }
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnNameToIndexMap.putIfAbsent(columnNames.get(i), i + 1);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = totalRows;
    this.isCloudFetchUsed = false;
  }

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for predefined metadata result set.
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param columnNames the names of each column
   * @param columnTypeText the textual representation of the column types
   * @param columnTypes the integer values representing the SQL types of each column
   * @param columnTypePrecisions the precisions of each column
   * @param isNullables the nullability status of each column
   * @param totalRows the total number of rows in the result set
   */
  public DatabricksResultSetMetaData(
      StatementId statementId,
      List<String> columnNames,
      List<String> columnTypeText,
      int[] columnTypes,
      int[] columnTypePrecisions,
      int[] isNullables,
      long totalRows) {
    this.statementId = statementId;

    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    for (int i = 0; i < columnNames.size(); i++) {
      ColumnInfoTypeName columnTypeName =
          ColumnInfoTypeName.valueOf(
              DatabricksTypeUtil.getDatabricksTypeFromSQLType(columnTypes[i]));
      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(columnNames.get(i))
          .columnType(columnTypes[i])
          .columnTypeText(columnTypeText.get(i))
          .typePrecision(columnTypePrecisions[i])
          .nullable(DatabricksTypeUtil.getNullableFromValue(isNullables[i]))
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
          .displaySize(
              DatabricksTypeUtil.getDisplaySize(
                  columnTypes[i],
                  columnTypePrecisions[i])) // using method for pre-defined metadata resultset
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      if (columnNames
          .get(i)
          .equals(
              REMARKS_COLUMN
                  .getColumnName())) { // special case: overriding default value of 128 for VARCHAR
        // cols.
        columnBuilder.displaySize(254);
      }
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnNameToIndexMap.putIfAbsent(columnNames.get(i), i + 1);
    }
    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
    this.totalRows = totalRows;
    this.isCloudFetchUsed = false;
  }

  /**
   * Constructs a {@code DatabricksResultSetMetaData} object for metadata result set obtained from
   * DESCRIBE QUERY Works for both SEA and Thrift flows as result set obtained from DESCRIBE QUERY
   * is already parsed.
   *
   * @param statementId the unique identifier of the SQL statement execution
   * @param columnNames names of each column
   * @param columnDataTypes types of each column
   * @param ctx connection context
   */
  public DatabricksResultSetMetaData(
      StatementId statementId,
      List<String> columnNames,
      List<String> columnDataTypes,
      IDatabricksConnectionContext ctx) {
    this.ctx = ctx;
    ImmutableList.Builder<ImmutableDatabricksColumn> columnsBuilder = ImmutableList.builder();
    Map<String, Integer> columnNameToIndexMap = new HashMap<>();
    MetadataResultSetBuilder metadataResultSetBuilder = new MetadataResultSetBuilder(ctx);

    // Capitalize all the columnDataTypes
    columnDataTypes =
        columnDataTypes.stream()
            .map(String::toUpperCase)
            .collect(Collectors.toCollection(ArrayList::new));

    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      String columnTypeText = columnDataTypes.get(i);

      ColumnInfoTypeName columnTypeName;
      if (columnTypeText.equalsIgnoreCase(TIMESTAMP_NTZ)) {
        columnTypeName = ColumnInfoTypeName.TIMESTAMP;
        columnTypeText = TIMESTAMP;
      } else if (columnTypeText.equalsIgnoreCase(VARIANT)) {
        columnTypeName = ColumnInfoTypeName.STRING;
        columnTypeText = VARIANT;
      } else if (columnTypeText.toUpperCase().startsWith(INTERVAL)) {
        columnTypeName = ColumnInfoTypeName.INTERVAL;
      } else {
        columnTypeName =
            ColumnInfoTypeName.valueOf(metadataResultSetBuilder.stripBaseTypeName(columnTypeText));
      }

      int columnType = DatabricksTypeUtil.getColumnType(columnTypeName);
      int[] precisionAndScale = getPrecisionAndScale(columnTypeText, columnType);
      int precision = precisionAndScale[0];
      int scale = precisionAndScale[1];

      ImmutableDatabricksColumn.Builder columnBuilder = getColumnBuilder();
      columnBuilder
          .columnName(columnName)
          .columnTypeClassName(DatabricksTypeUtil.getColumnTypeClassName(columnTypeName))
          .columnType(columnType)
          .columnTypeText(
              metadataResultSetBuilder.stripBaseTypeName(
                  columnTypeText)) // store base type eg. DECIMAL instead of DECIMAL(7,2), ARRAY
          // instead of ARRAY<STRING>
          .typePrecision(precision)
          .typeScale(scale)
          .displaySize(DatabricksTypeUtil.getDisplaySize(columnTypeName, precision, scale))
          .isSearchable(true) // set all columns to be searchable in execute query result set
          .schemaName(
              null) // set schema and table name to null, as server do not return these fields.
          .tableName(null)
          .isSigned(DatabricksTypeUtil.isSigned(columnTypeName));
      columnsBuilder.add(columnBuilder.build());
      // Keep index starting from 1, to be consistent with JDBC convention
      columnNameToIndexMap.putIfAbsent(columnName, i + 1);
    }
    this.statementId = statementId;
    this.isCloudFetchUsed = false;
    this.totalRows = -1;
    this.columns = columnsBuilder.build();
    this.columnNameIndex = CaseInsensitiveImmutableMap.copyOf(columnNameToIndexMap);
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

  private boolean getIsCloudFetchFromManifest(TGetResultSetMetadataResp resultManifest) {
    return resultManifest.getResultFormat() == TSparkRowSetType.URL_BASED_SET;
  }

  public Long getChunkCount() {
    return chunkCount;
  }

  public int[] getPrecisionAndScale(String columnTypeText, int columnType) {
    int[] result = getBasePrecisionAndScale(columnType, ctx);
    Pattern pattern = Pattern.compile("decimal\\((\\d+),\\s*(\\d+)\\)", Pattern.CASE_INSENSITIVE);
    Matcher matcher = pattern.matcher(columnTypeText);

    if (matcher.matches()) {
      result[0] = Integer.parseInt(matcher.group(1));
      result[1] = Integer.parseInt(matcher.group(2));
    }
    return result;
  }

  public int[] getPrecisionAndScale(ColumnInfo columnInfo, int columnType) {
    int[] result = getBasePrecisionAndScale(columnType, ctx);
    if (columnInfo.getTypePrecision() != null) {
      result[0] = Math.toIntExact(columnInfo.getTypePrecision()); // precision
      result[1] = Math.toIntExact(columnInfo.getTypeScale()); // scale
    }
    return result;
  }

  public int[] getPrecisionAndScale(ColumnInfo columnInfo) {
    return getPrecisionAndScale(
        columnInfo, DatabricksTypeUtil.getColumnType(columnInfo.getTypeName()));
  }

  private boolean isLargeColumn(String columnName) {
    return LARGE_DISPLAY_COLUMNS.stream()
        .anyMatch(column -> column.getColumnName().equals(columnName));
  }

  private boolean isVariantColumn(List<String> arrowMetadata, int i) {
    return arrowMetadata != null
        && arrowMetadata.size() > i
        && arrowMetadata.get(i) != null
        && arrowMetadata.get(i).equalsIgnoreCase(VARIANT);
  }

  private boolean isGeometryColumn(List<String> arrowMetadata, int index) {
    return arrowMetadata != null
        && arrowMetadata.size() > index
        && arrowMetadata.get(index) != null
        && arrowMetadata.get(index).contains(GEOMETRY);
  }

  private boolean isGeographyColumn(List<String> arrowMetadata, int index) {
    return arrowMetadata != null
        && arrowMetadata.size() > index
        && arrowMetadata.get(index) != null
        && arrowMetadata.get(index).contains(GEOGRAPHY);
  }

  /**
   * Checks if the given column type is a geospatial type.
   *
   * @param type the column type to check
   * @return true if the type is GEOMETRY or GEOGRAPHY, false otherwise
   */
  private boolean isGeospatialType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.GEOMETRY || type == ColumnInfoTypeName.GEOGRAPHY;
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
