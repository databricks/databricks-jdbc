package com.databricks.jdbc.api.impl.arrow;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_STREAMING_BATCH_TIMEOUT_SECONDS;
import static com.databricks.jdbc.common.util.ArrowUtil.getColumnInfoList;

import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.streaming.StreamingBatch;
import com.databricks.jdbc.api.impl.streaming.ThriftStreamingProvider;
import com.databricks.jdbc.api.impl.thrift.ThriftBatchFetcher;
import com.databricks.jdbc.api.impl.thrift.ThriftBatchFetcherImpl;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ColumnInfo;
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.List;

/**
 * High-throughput streaming implementation for inline Arrow results.
 *
 * <p>Uses {@link ThriftStreamingProvider} for proactive batch prefetching, achieving throughput
 * comparable to eager loading while maintaining the memory benefits of lazy loading.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Background prefetch thread fetches Arrow batches ahead of consumption
 *   <li>Sliding window limits memory usage to a configurable number of batches
 *   <li>Non-blocking iteration when prefetch keeps up with consumption
 *   <li>Automatic native memory cleanup via type-safe release actions
 *   <li>Type-safe: Uses generic {@code ThriftStreamingProvider<ArrowResultChunk>}
 * </ul>
 *
 * <p>This implementation replaces {@code LazyThriftInlineArrowResult} for improved throughput.
 */
public class StreamingInlineArrowResult implements IExecutionResult {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingInlineArrowResult.class);

  // Streaming infrastructure - type-safe generic provider
  private final ThriftStreamingProvider<ArrowResultChunk> provider;
  private final IDatabricksSession session;

  // Current position
  private StreamingBatch<ArrowResultChunk> currentBatch;
  private ArrowResultChunkIterator currentChunkIterator;
  private long globalRowIndex;

  // Metadata
  private List<ColumnInfo> columnInfos;
  private final int maxRows;

  // State
  private boolean hasReachedEnd;
  private volatile boolean isClosed;

  /**
   * Creates a new StreamingInlineArrowResult.
   *
   * <p>Configuration values (maxBatchesInMemory, timeout) are read from the session's connection
   * context.
   *
   * @param initialResponse The initial Thrift response containing the first Arrow batch
   * @param statement The statement that generated this result
   * @param session The session for fetching additional batches
   * @throws DatabricksSQLException if initialization fails
   */
  public StreamingInlineArrowResult(
      TFetchResultsResp initialResponse,
      IDatabricksStatementInternal statement,
      IDatabricksSession session)
      throws DatabricksSQLException {

    this.session = session;
    this.maxRows = statement != null ? statement.getMaxRows() : DEFAULT_RESULT_ROW_LIMIT;

    this.globalRowIndex = -1;
    this.hasReachedEnd = false;
    this.isClosed = false;

    // Initialize column info from metadata
    this.columnInfos = getColumnInfoList(initialResponse.getResultSetMetadata());

    // Create batch fetcher and type-safe generic provider for Arrow
    ThriftBatchFetcher fetcher = new ThriftBatchFetcherImpl(session, statement);
    this.provider =
        ThriftStreamingProvider.forInlineArrow(
            fetcher,
            initialResponse,
            statement != null ? statement.getStatementId() : null,
            session.getConnectionContext().getThriftMaxBatchesInMemory(),
            DEFAULT_STREAMING_BATCH_TIMEOUT_SECONDS);

    // Move to first batch (check nextBatch() return value to handle empty initial batches)
    if (provider.nextBatch()) {
      currentBatch = provider.getCurrentBatch();
      // Type-safe: getData() returns ArrowResultChunk directly!
      currentChunkIterator = currentBatch.getData().getChunkIterator();
    }

    LOGGER.debug(
        "StreamingInlineArrowResult initialized - firstBatchRows={}, maxRows={}, maxBatchesInMemory={}",
        currentBatch != null ? currentBatch.getRowCount() : 0,
        maxRows,
        session.getConnectionContext().getThriftMaxBatchesInMemory());
  }

  /**
   * Gets the value at the specified column index for the current row.
   *
   * @param columnIndex the zero-based column index
   * @return the value at the specified column
   * @throws DatabricksSQLException if access fails
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

    // Use shared complex type handling from ArrowStreamResult
    return ArrowStreamResult.getObjectWithComplexTypeHandling(
        session, currentChunkIterator, columnIndex, requiredType, arrowMetadata, columnInfo);
  }

  /** Validates state before getting an object. */
  private void validateGetObjectState(int columnIndex) throws DatabricksSQLException {
    if (isClosed) {
      LOGGER.error("Attempted to access closed result");
      throw new DatabricksSQLException(
          "Result is closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (globalRowIndex == -1) {
      LOGGER.error("Attempted to access data before first row");
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (currentChunkIterator == null) {
      LOGGER.error("No current chunk available at row {}", globalRowIndex);
      throw new DatabricksSQLException(
          "No current chunk available", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < 0 || columnIndex >= columnInfos.size()) {
      LOGGER.error("Column index {} out of bounds (0-{})", columnIndex, columnInfos.size() - 1);
      throw new DatabricksSQLException(
          "Column index out of bounds: " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
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
   * Moves the cursor to the next row. Fetches additional batches from server if needed.
   *
   * @return true if there is a next row, false if at the end
   * @throws DatabricksSQLException if an error occurs
   */
  @Override
  public boolean next() throws DatabricksSQLException {
    if (isClosed || hasReachedEnd) {
      return false;
    }

    if (!hasNext()) {
      return false;
    }

    // Check maxRows limit
    boolean hasRowLimit = maxRows > 0;
    if (hasRowLimit && globalRowIndex + 1 >= maxRows) {
      hasReachedEnd = true;
      return false;
    }

    globalRowIndex++;

    // Try to move to next row in current chunk
    if (currentChunkIterator != null && currentChunkIterator.hasNextRow()) {
      currentChunkIterator.nextRow();
      return true;
    }

    // Need to move to next batch
    if (provider.hasNextBatch()) {
      provider.nextBatch();
      currentBatch = provider.getCurrentBatch();

      if (currentBatch != null) {
        // Type-safe: getData() returns ArrowResultChunk directly!
        ArrowResultChunk chunk = currentBatch.getData();
        if (chunk == null) {
          LOGGER.warn("Batch {} has null data", currentBatch.getBatchIndex());
          hasReachedEnd = true;
          globalRowIndex--;
          return false;
        }
        currentChunkIterator = chunk.getChunkIterator();
        currentChunkIterator.nextRow();

        LOGGER.debug(
            "Moved to batch {} - globalRow={}, batchesInMemory={}",
            currentBatch.getBatchIndex(),
            globalRowIndex,
            provider.getBatchesInMemory());

        return true;
      }
    }

    // No more data
    hasReachedEnd = true;
    globalRowIndex--;
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

    // Check current chunk
    if (currentChunkIterator != null && currentChunkIterator.hasNextRow()) {
      return true;
    }

    // Check if more batches available
    return provider.hasNextBatch();
  }

  /** Closes this result and releases associated resources. */
  @Override
  public void close() {
    if (isClosed) {
      return;
    }

    long totalRows = provider.getTotalRowsFetched();
    isClosed = true;
    currentBatch = null;
    currentChunkIterator = null;

    // Provider will release all Arrow chunks using the type-safe Consumer<ArrowResultChunk>
    provider.close();

    LOGGER.debug("Closed - totalRowsFetched={}, rowsConsumed={}", totalRows, globalRowIndex + 1);
  }

  /**
   * Gets the number of rows in the current batch.
   *
   * @return the number of rows in the current batch
   */
  @Override
  public long getRowCount() {
    return currentBatch != null ? currentBatch.getRowCount() : 0;
  }

  /**
   * Gets the chunk count. Always returns 0 for streaming results.
   *
   * @return 0
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
    if (currentBatch == null) {
      return null;
    }
    ArrowResultChunk chunk = currentBatch.getData();
    return chunk != null ? chunk.getArrowMetadata() : null;
  }

  /**
   * Gets the total number of rows fetched from the server so far.
   *
   * @return the total rows fetched
   */
  public long getTotalRowsFetched() {
    return provider.getTotalRowsFetched();
  }

  /**
   * Checks if all data has been fetched from the server.
   *
   * @return true if end of stream reached
   */
  public boolean isCompletelyFetched() {
    return hasReachedEnd || provider.isEndOfStreamReached();
  }

  /**
   * Gets the number of batches currently in memory.
   *
   * @return the batch count in memory
   */
  public int getBatchesInMemory() {
    return provider.getBatchesInMemory();
  }
}
