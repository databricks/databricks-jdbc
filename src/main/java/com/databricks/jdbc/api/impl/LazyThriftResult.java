package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.extractRowsFromColumnar;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.util.List;

public class LazyThriftResult implements IExecutionResult {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(LazyThriftResult.class);

  private TFetchResultsResp currentResponse;
  private List<List<Object>> currentBatch;
  private int currentBatchIndex;
  private long globalRowIndex;
  private final IDatabricksSession session;
  private final IDatabricksStatementInternal statement;
  private final int maxRows;
  private boolean hasReachedEnd;
  private boolean isClosed;
  private long totalRowsFetched;

  /**
   * Creates a new LazyThriftResult that lazily fetches data on demand.
   *
   * @param initialResponse the initial response from the server
   * @param statement the statement that generated this result
   * @param session the session to use for fetching additional data
   * @throws DatabricksSQLException if the initial response cannot be processed
   */
  public LazyThriftResult(
      TFetchResultsResp initialResponse,
      IDatabricksStatementInternal statement,
      IDatabricksSession session)
      throws DatabricksSQLException {
    this.currentResponse = initialResponse;
    this.statement = statement;
    this.session = session;
    this.maxRows = statement != null ? statement.getMaxRows() : DEFAULT_RESULT_ROW_LIMIT;
    this.globalRowIndex = -1;
    this.currentBatchIndex = -1;
    this.hasReachedEnd = false;
    this.isClosed = false;
    this.totalRowsFetched = 0;

    // Load initial batch
    loadCurrentBatch();
    LOGGER.debug(
        "LazyThriftResult initialized with {} rows in first batch, hasMoreRows: {}",
        currentBatch.size(),
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
    if (isClosed) {
      throw new DatabricksSQLException(
          "Result is already closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
    if (globalRowIndex == -1) {
      throw new DatabricksSQLException(
          "Cursor is before first row", DatabricksDriverErrorCode.INVALID_STATE);
    }
    if (currentBatchIndex < 0 || currentBatchIndex >= currentBatch.size()) {
      throw new DatabricksSQLException(
          "Invalid cursor position", DatabricksDriverErrorCode.INVALID_STATE);
    }
    List<Object> currentRowData = currentBatch.get(currentBatchIndex);
    if (columnIndex < 0 || columnIndex >= currentRowData.size()) {
      throw new DatabricksSQLException(
          "Column index out of bounds " + columnIndex, DatabricksDriverErrorCode.INVALID_STATE);
    }
    return currentRowData.get(columnIndex);
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
      // Ideally the client code should first call, hasNext() and then next()
      // However, the client code like in DatabricksResultSet#next directly calls next
      // So, this is a safeguard to ensure we don't move past the end
      return false;
    }

    // Check if we've reached the maxRows limit
    boolean hasRowLimit = maxRows > 0;
    if (hasRowLimit && globalRowIndex + 1 >= maxRows) {
      hasReachedEnd = true;
      return false;
    }

    // Move to next row in current batch
    currentBatchIndex++;
    globalRowIndex++;

    // Check if we need to fetch the next batch
    if (currentBatchIndex >= currentBatch.size()) {
      // Keep fetching until we get a non-empty batch or no more rows
      while (currentResponse.hasMoreRows) {
        fetchNextBatch();

        // If we got a non-empty batch, we can proceed
        if (!currentBatch.isEmpty()) {
          currentBatchIndex = 0; // Reset to first row of new batch
          break;
        }

        // If batch is still empty but hasMoreRows is false after fetch, we'll exit the loop
      }

      // If we exited the loop and still have an empty batch, we've reached the end
      if (currentBatch.isEmpty()) {
        hasReachedEnd = true;
        globalRowIndex--; // Revert the increment since we didn't actually move to a new row
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

    // Check if there are more rows in current batch
    if (currentBatchIndex + 1 < currentBatch.size()) {
      return true;
    }

    // Check if there are more batches to fetch
    return currentResponse.hasMoreRows;
  }

  /** Closes this result and releases associated resources. */
  @Override
  public void close() {
    this.isClosed = true;
    this.currentBatch = null;
    this.currentResponse = null;
    LOGGER.debug("LazyThriftResult closed after fetching {} total rows", totalRowsFetched);
  }

  /**
   * Gets the number of rows in the current batch.
   *
   * @return the number of rows in the current batch
   */
  @Override
  public long getRowCount() {
    // Return the number of rows in the current batch
    return currentBatch != null ? currentBatch.size() : 0;
  }

  /**
   * Gets the chunk count. Always returns 0 for thrift columnar results.
   *
   * @return 0 (thrift results don't use chunks like Arrow)
   */
  @Override
  public long getChunkCount() {
    // For thrift columnar results, we don't have chunks in the same sense as Arrow
    return 0;
  }

  /**
   * Loads the current response data into memory as a batch of rows.
   *
   * @throws DatabricksSQLException if the response data cannot be processed
   */
  private void loadCurrentBatch() throws DatabricksSQLException {
    currentBatch = extractRowsFromColumnar(currentResponse.getResults());
    currentBatchIndex = -1; // Reset batch index
    totalRowsFetched += currentBatch.size();
    LOGGER.debug(
        "Loaded batch with {} rows, total fetched: {}", currentBatch.size(), totalRowsFetched);
  }

  /**
   * Fetches the next batch of data from the server and loads it into memory.
   *
   * @throws DatabricksSQLException if the fetch operation fails
   */
  private void fetchNextBatch() throws DatabricksSQLException {
    try {
      LOGGER.debug("Fetching next batch, current total rows fetched: {}", totalRowsFetched);
      currentResponse = session.getDatabricksClient().getMoreResults(statement);
      loadCurrentBatch();

      LOGGER.debug(
          "Fetched batch with {} rows, hasMoreRows: {}",
          currentBatch.size(),
          currentResponse.hasMoreRows);
    } catch (DatabricksSQLException e) {
      LOGGER.error("Failed to fetch next batch: {}", e.getMessage());
      hasReachedEnd = true;
      throw e; // Propagate exception to fail fast
    }
  }

  /**
   * Gets the total number of rows fetched from the server so far. This is different from
   * getRowCount() which returns current batch size.
   *
   * @return the total number of rows fetched from the server
   */
  public long getTotalRowsFetched() {
    return totalRowsFetched;
  }

  /**
   * Checks if all data has been fetched from the server.
   *
   * @return true if all data has been fetched (either reached end or maxRows limit)
   */
  public boolean isCompletelyFetched() {
    return hasReachedEnd || !currentResponse.hasMoreRows;
  }
}
