package com.databricks.jdbc.api.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;

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

  // Configuration defaults
  private static final int DEFAULT_MAX_BATCHES_IN_MEMORY = 3;
  private static final int DEFAULT_BATCH_READY_TIMEOUT_SECONDS = 300;

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
   * Creates a new StreamingColumnarResult with default configuration.
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
    this(
        initialResponse,
        statement,
        session,
        DEFAULT_MAX_BATCHES_IN_MEMORY,
        DEFAULT_BATCH_READY_TIMEOUT_SECONDS);
  }

  /**
   * Creates a new StreamingColumnarResult with custom configuration.
   *
   * @param initialResponse The initial Thrift response containing the first batch
   * @param statement The statement that generated this result
   * @param session The session for fetching additional batches
   * @param maxBatchesInMemory Maximum batches to keep in memory (sliding window)
   * @param batchReadyTimeoutSeconds Timeout waiting for batch to be ready
   * @throws DatabricksSQLException if initialization fails
   */
  public StreamingColumnarResult(
      TFetchResultsResp initialResponse,
      IDatabricksStatementInternal statement,
      IDatabricksSession session,
      int maxBatchesInMemory,
      int batchReadyTimeoutSeconds)
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
            fetcher, initialResponse, maxBatchesInMemory, batchReadyTimeoutSeconds);

    // Move to first batch
    if (provider.hasNextBatch()) {
      provider.nextBatch();
      currentBatch = provider.getCurrentBatch();
    }

    LOGGER.debug(
        "[STREAMING] StreamingColumnarResult initialized - firstBatchRows={}, maxRows={}, maxBatchesInMemory={}",
        currentBatch != null ? currentBatch.getRowCount() : 0,
        maxRows,
        maxBatchesInMemory);
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
      throw new DatabricksSQLException(
          "Result is closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (globalRowIndex == -1) {
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (currentBatch == null || currentBatchRowIndex < 0) {
      throw new DatabricksSQLException(
          "Invalid cursor position", DatabricksDriverErrorCode.INVALID_STATE);
    }

    // Type-safe: getData() returns ColumnarRowView directly, no casting!
    ColumnarRowView view = currentBatch.getData();
    if (view == null) {
      throw new DatabricksSQLException(
          "Batch data not available", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (columnIndex < 0 || columnIndex >= view.getColumnCount()) {
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
          LOGGER.warn("[CONSUMER] Got null batch after nextBatch()");
          hasReachedEnd = true;
          globalRowIndex--;
          currentBatchRowIndex--;
          return false;
        }

        // Log batch transition
        LOGGER.debug(
            "[CONSUMER] Moved to batch {} - globalRow={}, batchesInMemory={}",
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

    // Log progress periodically (every 500K rows)
    if (globalRowIndex > 0 && globalRowIndex % 500000 == 0 && currentBatch != null) {
      LOGGER.debug(
          "[CONSUMER] Progress - rows={}, batch={}, batchesInMemory={}",
          globalRowIndex,
          currentBatch.getBatchIndex(),
          provider.getBatchesInMemory());
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

    long totalRows = provider != null ? provider.getTotalRowsFetched() : 0;
    isClosed = true;
    currentBatch = null;

    if (provider != null) {
      provider.close();
    }

    LOGGER.debug(
        "[STREAMING] Closed - totalRowsFetched={}, rowsConsumed={}", totalRows, globalRowIndex + 1);
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
    return provider != null ? provider.getTotalRowsFetched() : 0;
  }

  /**
   * Checks if all data has been fetched from the server.
   *
   * @return true if end of stream reached
   */
  public boolean isCompletelyFetched() {
    return hasReachedEnd || (provider != null && provider.isEndOfStreamReached());
  }

  /**
   * Gets the number of batches currently in memory.
   *
   * @return the batch count in memory
   */
  public int getBatchesInMemory() {
    return provider != null ? provider.getBatchesInMemory() : 0;
  }
}
