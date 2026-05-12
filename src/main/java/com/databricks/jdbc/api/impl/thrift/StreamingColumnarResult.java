package com.databricks.jdbc.api.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_STREAMING_BATCH_TIMEOUT_SECONDS;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.api.impl.IExecutionResult;
import com.databricks.jdbc.api.impl.streaming.StreamingBatch;
import com.databricks.jdbc.api.impl.streaming.ThriftStreamingProvider;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/**
 * High-throughput streaming implementation for Thrift columnar results.
 *
 * <p>Uses {@link ThriftStreamingProvider} for proactive batch prefetching, achieving throughput
 * comparable to eager loading while maintaining the memory benefits of lazy loading.
 *
 * <p>Key features:
 *
 * <ul>
 *   <li>Background prefetch thread fetches batches ahead of consumption
 *   <li>Sliding window limits memory usage to a configurable number of batches
 *   <li>Non-blocking iteration when prefetch keeps up with consumption
 *   <li>Maintains correct row ordering through sequential fetch
 *   <li>Type-safe: Uses generic {@code ThriftStreamingProvider<ColumnarRowView>}
 * </ul>
 *
 * <p>This implementation replaces {@code LazyThriftResult} for improved throughput while
 * maintaining the same {@link IExecutionResult} interface.
 */
public class StreamingColumnarResult implements IExecutionResult {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(StreamingColumnarResult.class);

  // Streaming infrastructure - type-safe generic provider
  private final ThriftStreamingProvider<ColumnarRowView> provider;

  // Current position within the current batch
  private StreamingBatch<ColumnarRowView> currentBatch;
  private int currentBatchRowIndex;
  private long globalRowIndex;

  // Limits
  private final int maxRows;

  // State
  private boolean hasReachedEnd;
  private volatile boolean isClosed;

  /**
   * Creates a new StreamingColumnarResult.
   *
   * <p>Configuration values (maxBatchesInMemory, timeout) are read from the session's connection
   * context.
   *
   * @param initialResponse The initial Thrift response containing the first batch
   * @param statement The statement that generated this result
   * @param session The session for fetching additional batches
   * @throws DatabricksSQLException if initialization fails
   */
  public StreamingColumnarResult(
      TFetchResultsResp initialResponse,
      IDatabricksStatementInternal statement,
      IDatabricksSession session)
      throws DatabricksSQLException {

    this.maxRows = statement != null ? statement.getMaxRows() : DEFAULT_RESULT_ROW_LIMIT;

    this.globalRowIndex = -1;
    this.currentBatchRowIndex = -1;
    this.hasReachedEnd = false;
    this.isClosed = false;

    // Create batch fetcher and type-safe generic provider
    ThriftBatchFetcher fetcher = new ThriftBatchFetcherImpl(session, statement);
    this.provider =
        ThriftStreamingProvider.forColumnar(
            fetcher,
            initialResponse,
            session.getConnectionContext().getThriftMaxBatchesInMemory(),
            DEFAULT_STREAMING_BATCH_TIMEOUT_SECONDS);

    // Move to first batch (check nextBatch() return value to handle empty initial batches)
    if (provider.nextBatch()) {
      currentBatch = provider.getCurrentBatch();
    }

    LOGGER.debug(
        "StreamingColumnarResult initialized - firstBatchRows={}, maxRows={}, maxBatchesInMemory={}",
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
    if (currentBatch == null || currentBatchRowIndex < 0) {
      LOGGER.error(
          "Invalid cursor position: batch={}, rowIndex={}", currentBatch, currentBatchRowIndex);
      throw new DatabricksSQLException(
          "Invalid cursor position", DatabricksDriverErrorCode.INVALID_STATE);
    }

    // Type-safe: getData() returns ColumnarRowView directly, no casting!
    ColumnarRowView view = currentBatch.getData();
    if (view == null) {
      LOGGER.error("Batch data not available at row {}", globalRowIndex);
      throw new DatabricksSQLException(
          "Batch data not available", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < 0 || columnIndex >= view.getColumnCount()) {
      LOGGER.error("Column index {} out of bounds (0-{})", columnIndex, view.getColumnCount() - 1);
      throw new DatabricksSQLException(
          "Column index out of bounds: " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }

    return view.getValue(currentBatchRowIndex, columnIndex);
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

    // Move to next row
    currentBatchRowIndex++;
    globalRowIndex++;

    // Check if we need to move to next batch
    ColumnarRowView batchData = currentBatch != null ? currentBatch.getData() : null;
    long batchRowCount = batchData != null ? batchData.getRowCount() : 0;
    if (currentBatch != null && currentBatchRowIndex >= batchRowCount) {

      // Try to move to next batch
      if (provider.hasNextBatch()) {
        provider.nextBatch();
        currentBatch = provider.getCurrentBatch();
        currentBatchRowIndex = 0;

        if (currentBatch == null) {
          LOGGER.warn("Got null batch after nextBatch()");
          hasReachedEnd = true;
          globalRowIndex--;
          currentBatchRowIndex--;
          return false;
        }

        // Log batch transition
        LOGGER.debug(
            "Moved to batch {} - globalRow={}, batchesInMemory={}",
            currentBatch.getBatchIndex(),
            globalRowIndex,
            provider.getBatchesInMemory());
      } else {
        // No more batches
        hasReachedEnd = true;
        globalRowIndex--;
        currentBatchRowIndex--;
        return false;
      }
    }

    return true;
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

    // Check current batch - type-safe getData() returns ColumnarRowView
    if (currentBatch != null) {
      ColumnarRowView view = currentBatch.getData();
      if (view != null && currentBatchRowIndex + 1 < view.getRowCount()) {
        return true;
      }
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
   * Gets the chunk count. Always returns 0 for thrift columnar results (chunks are an Arrow
   * concept).
   *
   * @return 0
   */
  @Override
  public long getChunkCount() {
    return 0;
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
