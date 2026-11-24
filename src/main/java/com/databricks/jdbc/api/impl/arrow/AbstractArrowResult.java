package com.databricks.jdbc.api.impl.arrow;

import com.databricks.jdbc.api.impl.ComplexDataTypeParser;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.MetadataParser;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;

/** Shares cursor mechanics without coupling distinct Arrow wire formats. */
public abstract class AbstractArrowResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(AbstractArrowResult.class);

  private final ChunkProvider chunkProvider;
  private final List<ColumnInfo> columnInfos;
  private final IDatabricksSession session;
  private long currentRowIndex = -1;
  private boolean isClosed;
  private ArrowResultChunkIterator chunkIterator;

  AbstractArrowResult(
      ChunkProvider chunkProvider, List<ColumnInfo> columnInfos, IDatabricksSession session) {
    this.chunkProvider = chunkProvider;
    this.columnInfos = columnInfos;
    this.session = session;
  }

  public List<String> getArrowMetadata() throws DatabricksSQLException {
    if (chunkProvider.getChunk() == null) {
      return null;
    }
    return chunkProvider.getChunk().getArrowMetadata();
  }

  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    if (columnIndex < 0 || columnIndex >= columnInfos.size()) {
      throw new DatabricksSQLException(
          "Column index out of bounds: " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }
    ColumnInfo columnInfo = columnInfos.get(columnIndex);
    ColumnInfoTypeName requiredType = columnInfo.getTypeName();
    String arrowMetadata = chunkIterator.getType(columnIndex);
    if (arrowMetadata == null) {
      arrowMetadata = columnInfo.getTypeText();
    }
    return getObjectWithComplexTypeHandling(
        session, chunkIterator, columnIndex, requiredType, arrowMetadata, columnInfo);
  }

  @VisibleForTesting
  public static boolean isComplexType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.ARRAY
        || type == ColumnInfoTypeName.MAP
        || type == ColumnInfoTypeName.STRUCT;
  }

  @VisibleForTesting
  public static boolean isGeospatialType(ColumnInfoTypeName type) {
    return type == ColumnInfoTypeName.GEOMETRY || type == ColumnInfoTypeName.GEOGRAPHY;
  }

  protected static Object getObjectWithComplexTypeHandling(
      IDatabricksSession session,
      ArrowResultChunkIterator chunkIterator,
      int columnIndex,
      ColumnInfoTypeName requiredType,
      String arrowMetadata,
      ColumnInfo columnInfo)
      throws DatabricksSQLException {
    boolean isComplexDatatypeSupportEnabled =
        session.getConnectionContext().isComplexDatatypeSupportEnabled();
    boolean isGeoSpatialSupportEnabled =
        session.getConnectionContext().isGeoSpatialSupportEnabled();

    if (!isGeoSpatialSupportEnabled && isGeospatialType(requiredType)) {
      LOGGER.debug("Geospatial support is disabled, converting {} to STRING", requiredType);
      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfo);
      if (result == null) {
        return null;
      }
      return result;
    }

    if (!isComplexDatatypeSupportEnabled && isComplexType(requiredType)) {
      LOGGER.debug("Complex datatype support is disabled, converting complex type to STRING");
      Object result =
          chunkIterator.getColumnObjectAtCurrentRow(
              columnIndex, ColumnInfoTypeName.STRING, "STRING", columnInfo);
      if (result == null) {
        return null;
      }
      ComplexDataTypeParser parser = new ComplexDataTypeParser();
      if (MetadataParser.containsGeospatialType(arrowMetadata)) {
        return parser.formatGeospatialComplexTypeString(
            result.toString(), requiredType.name(), arrowMetadata);
      }
      return parser.formatComplexTypeString(result.toString(), requiredType.name(), arrowMetadata);
    }

    if (isComplexType(requiredType)) {
      return chunkIterator.getColumnObjectAtCurrentRow(
          columnIndex, requiredType, arrowMetadata, columnInfo, isGeoSpatialSupportEnabled);
    }

    return chunkIterator.getColumnObjectAtCurrentRow(
        columnIndex, requiredType, arrowMetadata, columnInfo);
  }

  @Override
  public Object getTemporalObject(int columnIndex) throws DatabricksSQLException {
    if (columnIndex < 0 || columnIndex >= columnInfos.size()) {
      return getObject(columnIndex);
    }
    if (chunkIterator == null) {
      throw new DatabricksSQLException(
          "Cursor is before first row",
          DatabricksDriverErrorCode.INVALID_STATE.name(),
          DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (chunkIterator.hasJavaTimeVector(columnIndex)) {
      return chunkIterator.getJavaTimeObjectAtCurrentRow(columnIndex);
    }
    return getObject(columnIndex);
  }

  @Override
  public long getCurrentRow() {
    return currentRowIndex;
  }

  @Override
  public boolean next() throws DatabricksSQLException {
    if (isClosed) {
      return false;
    }
    if (chunkIterator != null && chunkIterator.nextRow()) {
      currentRowIndex++;
      return true;
    }
    if (!chunkProvider.hasNextChunk() || !chunkProvider.next()) {
      return false;
    }
    chunkIterator = chunkProvider.getChunk().getChunkIterator();
    currentRowIndex++;
    return chunkIterator.nextRow();
  }

  @Override
  public boolean hasNext() {
    if (isClosed) {
      return false;
    }
    return (chunkIterator != null && chunkIterator.hasNextRow()) || chunkProvider.hasNextChunk();
  }

  @Override
  public void close() {
    isClosed = true;
    chunkProvider.close();
  }

  @Override
  public long getRowCount() {
    return chunkProvider.getRowCount();
  }

  @Override
  public long getChunkCount() {
    return chunkProvider.getChunkCount();
  }

  @VisibleForTesting
  public ChunkProvider getChunkProvider() {
    return chunkProvider;
  }
}
