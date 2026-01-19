package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.getColumnInfoFromTColumnDesc;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.*;
import static com.databricks.jdbc.common.util.DecompressionUtil.decompress;

import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.util.DatabricksThriftUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.SchemaUtility;

/**
 * Lazy implementation for thrift-based inline Arrow results that fetches arrow batches on demand.
 * Similar to LazyThriftResult but processes Arrow data instead of columnar thrift data.
 */
public class LazyThriftInlineArrowResult implements IExecutionResult {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(LazyThriftInlineArrowResult.class);

  private TFetchResultsResp currentResponse;
  private ArrowResultChunk currentChunk;
  private ArrowResultChunkIterator currentChunkIterator;
  private long globalRowIndex;
  private final IDatabricksSession session;
  private final IDatabricksStatementInternal statement;
  private final int maxRows;
  private boolean hasReachedEnd;
  private boolean isClosed;
  private long totalRowsFetched;
  private List<ColumnInfo> columnInfos;

  /**
   * Creates a new LazyThriftInlineArrowResult that lazily fetches arrow data on demand.
   *
   * @param initialResponse the initial response from the server
   * @param statement the statement that generated this result
   * @param session the session to use for fetching additional data
   * @throws DatabricksSQLException if the initial response cannot be processed
   */
  public LazyThriftInlineArrowResult(
      TFetchResultsResp initialResponse,
      IDatabricksStatementInternal statement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.currentResponse = initialResponse;
    this.statement = statement;
    this.session = session;
    this.maxRows = statement != null ? statement.getMaxRows() : DEFAULT_RESULT_ROW_LIMIT;
    this.globalRowIndex = -1;
    this.hasReachedEnd = false;
    this.isClosed = false;
    this.totalRowsFetched = 0;

    // Initialize column info from metadata
    setColumnInfo(initialResponse.getResultSetMetadata());

    // Load initial chunk
    loadCurrentChunk();
    LOGGER.debug(
        "LazyThriftInlineArrowResult initialized with {} rows in first chunk, hasMoreRows: {}",
        currentChunk.numRows,
        currentResponse.hasMoreRows);
  }

  /**
   * Gets the value at the specified column index for the current row.
   *
   * @param columnIndex the zero-based column index
   * @return the value at the specified column
   * @throws DatabricksSQLException if the result is closed, cursor is invalid, or column index is
   *     out of bounds
   */
  @Override
  public Object getObject(int columnIndex) throws DatabricksSQLException {
    validateGetObjectState(columnIndex);

    ColumnInfo columnInfo = columnInfos.get(columnIndex);
    ColumnInfoTypeName requiredType = columnInfo.getTypeName();
    String arrowMetadata = currentChunkIterator.getType(columnIndex);
    if (arrowMetadata == null) {
      arrowMetadata = columnInfo.getTypeText();
    }

    return ArrowStreamResult.getObjectWithComplexTypeHandling(
        session, currentChunkIterator, columnIndex, requiredType, arrowMetadata, columnInfo);
  }

  /**
   * Validates the state before getting an object at the specified column index.
   *
   * @param columnIndex the zero-based column index to validate
   * @throws DatabricksSQLException if the result is closed, cursor is invalid, or column index is
   *     out of bounds
   */
  private void validateGetObjectState(int columnIndex) throws DatabricksSQLException {
    if (isClosed) {
      LOGGER.warn("Attempted to get object from closed result");
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (globalRowIndex == -1) {
      LOGGER.warn("Attempted to get object before calling next()");
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (currentChunkIterator == null) {
      LOGGER.warn("No current chunk available when getting object");
      throw new DatabricksSQLException(
          "No current chunk available", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < 0 || columnIndex >= columnInfos.size()) {
      LOGGER.warn("Column index {} out of bounds (size: {})", columnIndex, columnInfos.size());
      throw new DatabricksSQLException(
          "Column index out of bounds " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  /**
   * Gets the current row index (0-based). Returns -1 if before the first row.
   *
   * @return the current row index
   */
  @Override
  public long getCurrentRow() {
    return globalRowIndex;
  }

  /**
   * Moves the cursor to the next row. Fetches additional data from server if needed.
   *
   * @return true if there is a next row, false if at the end
   * @throws DatabricksSQLException if an error occurs while fetching data
   */
  @Override
  public boolean next() throws DatabricksSQLException {
    if (isClosed || hasReachedEnd) {
      return false;
    }

    if (!hasNext()) {
      return false;
    }

    // Check if we've reached the maxRows limit
    boolean hasRowLimit = maxRows > 0;
    if (hasRowLimit && globalRowIndex + 1 >= maxRows) {
      hasReachedEnd = true;
      return false;
    }

    // Try to advance in current chunk
    if (currentChunkIterator != null && currentChunkIterator.hasNextRow()) {
      boolean advanced = currentChunkIterator.nextRow();
      if (advanced) {
        globalRowIndex++;
        return true;
      }
    }

    // Need to fetch next chunk
    while (currentResponse.hasMoreRows) {
      fetchNextChunk();

      // If we got a chunk with data, advance to first row
      if (currentChunkIterator != null && currentChunkIterator.hasNextRow()) {
        boolean advanced = currentChunkIterator.nextRow();
        if (advanced) {
          globalRowIndex++;
          return true;
        }
      }
    }

    // No more data available
    hasReachedEnd = true;
    return false;
  }

  /**
   * Checks if there are more rows available without advancing the cursor.
   *
   * @return true if there are more rows, false otherwise
   */
  @Override
  public boolean hasNext() {
    if (isClosed || hasReachedEnd) {
      return false;
    }

    // Check maxRows limit
    boolean hasRowLimit = maxRows > 0;
    if (hasRowLimit && globalRowIndex + 1 >= maxRows) {
      return false;
    }

    // Check if there are more rows in current chunk
    if (currentChunkIterator != null && currentChunkIterator.hasNextRow()) {
      return true;
    }

    // Check if there are more chunks to fetch
    return currentResponse.hasMoreRows;
  }

  /** Closes this result and releases associated resources. */
  @Override
  public void close() {
    this.isClosed = true;
    if (currentChunk != null) {
      currentChunk.releaseChunk();
    }
    this.currentChunk = null;
    this.currentChunkIterator = null;
    this.currentResponse = null;
    LOGGER.debug(
        "LazyThriftInlineArrowResult closed after fetching {} total rows", totalRowsFetched);
  }

  /**
   * Gets the number of rows in the current chunk.
   *
   * @return the number of rows in the current chunk
   */
  @Override
  public long getRowCount() {
    return currentChunk != null ? currentChunk.numRows : 0;
  }

  /**
   * Gets the chunk count. Always returns 0 for lazy thrift inline arrow results.
   *
   * @return 0 (lazy results don't use chunks in the same sense as buffered results)
   */
  @Override
  public long getChunkCount() {
    return 0;
  }

  /**
   * Gets the Arrow metadata for the current chunk.
   *
   * @return list of arrow metadata strings, or null if no chunk is loaded
   * @throws DatabricksSQLException if an error occurs
   */
  public List<String> getArrowMetadata() throws DatabricksSQLException {
    if (currentChunk == null) {
      return null;
    }
    return currentChunk.getArrowMetadata();
  }

  private void loadCurrentChunk() throws DatabricksSQLException {
    try {
      ByteArrayInputStream byteStream = createArrowByteStream(currentResponse);
      long rowCount = getTotalRowsInResponse(currentResponse);

      ArrowResultChunk.Builder builder =
          ArrowResultChunk.builder().withInputStream(byteStream, rowCount);

      if (statement != null) {
        builder.withStatementId(statement.getStatementId());
      }

      currentChunk = builder.build();
      currentChunkIterator = currentChunk.getChunkIterator();
      totalRowsFetched += rowCount;

      LOGGER.debug(
          "Loaded arrow chunk with {} rows, total fetched: {}", rowCount, totalRowsFetched);
    } catch (DatabricksParsingException e) {
      LOGGER.error("Failed to load current chunk: {}", e.getMessage());
      // Clean up any partially loaded chunk to prevent memory leaks
      if (currentChunk != null) {
        currentChunk.releaseChunk();
        currentChunk = null;
      }
      currentChunkIterator = null;
      hasReachedEnd = true;
      throw new DatabricksSQLException(
          "Failed to process arrow data", DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
    }
  }

  /**
   * Fetches the next chunk of data from the server and creates arrow chunks.
   *
   * @throws DatabricksSQLException if the fetch operation fails
   */
  private void fetchNextChunk() throws DatabricksSQLException {
    try {
      LOGGER.debug("Fetching next arrow chunk, current total rows fetched: {}", totalRowsFetched);
      currentResponse = session.getDatabricksClient().getMoreResults(statement);

      // Release previous chunk to free memory
      if (currentChunk != null) {
        currentChunk.releaseChunk();
      }

      loadCurrentChunk();

      LOGGER.debug(
          "Fetched arrow chunk with {} rows, hasMoreRows: {}",
          currentChunk.numRows,
          currentResponse.hasMoreRows);
    } catch (DatabricksSQLException e) {
      LOGGER.error("Failed to fetch next arrow chunk: {}", e.getMessage());
      hasReachedEnd = true;
      throw e;
    }
  }

  private ByteArrayInputStream createArrowByteStream(TFetchResultsResp resultsResp)
      throws DatabricksParsingException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    CompressionCodec compressionType =
        CompressionCodec.getCompressionMapping(resultsResp.getResultSetMetadata());
    try {
      byte[] serializedSchema = getSerializedSchema(resultsResp.getResultSetMetadata());
      if (serializedSchema != null) {
        baos.write(serializedSchema);
      }
      writeArrowBatchesToStream(compressionType, resultsResp.getResults().getArrowBatches(), baos);
      return new ByteArrayInputStream(baos.toByteArray());
    } catch (DatabricksSQLException | IOException e) {
      handleError(e);
    }
    return null;
  }

  private void writeArrowBatchesToStream(
      CompressionCodec compressionCodec,
      List<TSparkArrowBatch> arrowBatchList,
      ByteArrayOutputStream baos)
      throws DatabricksSQLException, IOException {
    for (TSparkArrowBatch arrowBatch : arrowBatchList) {
      byte[] decompressedBytes =
          decompress(
              arrowBatch.getBatch(),
              compressionCodec,
              String.format(
                  "Data fetch for lazy inline arrow batch [%d] and statement [%s] with decompression algorithm : [%s]",
                  arrowBatch.getRowCount(), statement, compressionCodec));
      baos.write(decompressedBytes);
    }
  }

  private long getTotalRowsInResponse(TFetchResultsResp resultsResp) {
    long totalRows = 0;
    if (resultsResp.getResults() != null && resultsResp.getResults().getArrowBatches() != null) {
      for (TSparkArrowBatch arrowBatch : resultsResp.getResults().getArrowBatches()) {
        totalRows += arrowBatch.getRowCount();
      }
    }
    return totalRows;
  }

  private byte[] getSerializedSchema(TGetResultSetMetadataResp metadata)
      throws DatabricksSQLException {
    if (metadata.getArrowSchema() != null) {
      return metadata.getArrowSchema();
    }
    Schema arrowSchema = hiveSchemaToArrowSchema(metadata.getSchema());
    try {
      return SchemaUtility.serialize(arrowSchema);
    } catch (IOException e) {
      handleError(e);
    }
    return null;
  }

  private Schema hiveSchemaToArrowSchema(TTableSchema hiveSchema)
      throws DatabricksParsingException {
    List<Field> fields = new ArrayList<>();
    if (hiveSchema == null) {
      return new Schema(fields);
    }
    try {
      hiveSchema
          .getColumns()
          .forEach(
              columnDesc -> {
                try {
                  fields.add(getArrowField(columnDesc));
                } catch (SQLException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (RuntimeException e) {
      handleError(e);
    }
    return new Schema(fields);
  }

  private Field getArrowField(TColumnDesc columnDesc) throws SQLException {
    TPrimitiveTypeEntry primitiveTypeEntry = getTPrimitiveTypeOrDefault(columnDesc.getTypeDesc());
    ArrowType arrowType = mapThriftToArrowType(primitiveTypeEntry.getType());
    FieldType fieldType = new FieldType(true, arrowType, null);
    return new Field(columnDesc.getColumnName(), fieldType, null);
  }

  private void setColumnInfo(TGetResultSetMetadataResp resultManifest)
      throws DatabricksSQLException {
    columnInfos = new ArrayList<>();
    if (resultManifest.getSchema() == null) {
      return;
    }
    List<String> arrowMetadata = DatabricksThriftUtil.getArrowMetadata(resultManifest);
    List<TColumnDesc> columns = resultManifest.getSchema().getColumns();
    for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
      TColumnDesc tColumnDesc = columns.get(columnIndex);
      String columnArrowMetadata = arrowMetadata != null ? arrowMetadata.get(columnIndex) : null;
      columnInfos.add(getColumnInfoFromTColumnDesc(tColumnDesc, columnArrowMetadata));
    }
  }

  @VisibleForTesting
  void handleError(Exception e) throws DatabricksParsingException {
    String errorMessage =
        String.format("Cannot process lazy thrift inline arrow format. Error: %s", e.getMessage());
    LOGGER.error(errorMessage);
    throw new DatabricksParsingException(
        errorMessage, e, DatabricksDriverErrorCode.INLINE_CHUNK_PARSING_ERROR);
  }

  /**
   * Gets the total number of rows fetched from the server so far.
   *
   * @return the total number of rows fetched from the server
   */
  long getTotalRowsFetched() {
    return totalRowsFetched;
  }

  /**
   * Checks if all data has been fetched from the server.
   *
   * @return true if all data has been fetched (either reached end or maxRows limit)
   */
  boolean isCompletelyFetched() {
    return hasReachedEnd || !currentResponse.hasMoreRows;
  }
}
