package com.databricks.jdbc.api.impl.thrift;

import com.databricks.jdbc.api.impl.ColumnarRowView;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Container for a single batch of Thrift columnar results. Tracks fetch status and provides access
 * to batch data with non-blocking status checks and blocking wait capabilities.
 *
 * <p>Lifecycle: PENDING → FETCHING → READY (or ERROR) → RELEASED
 */
public class ThriftBatch {

  /** Status of the batch in its lifecycle. */
  public enum Status {
    /** Batch created but fetch not started */
    PENDING,
    /** Fetch in progress */
    FETCHING,
    /** Data available for consumption */
    READY,
    /** Fetch failed with error */
    ERROR,
    /** Memory released, batch no longer usable */
    RELEASED
  }

  private final long batchIndex;
  private final long startRowOffset;
  private volatile Status status;
  private volatile TFetchResultsResp response;
  private volatile ColumnarRowView columnarView;
  private volatile Throwable error;
  private volatile long rowCount;
  private final CompletableFuture<ThriftBatch> readyFuture;

  /**
   * Creates a new ThriftBatch in PENDING state.
   *
   * @param batchIndex The sequential index of this batch (0-based)
   * @param startRowOffset The row offset where this batch starts in the overall result set
   */
  public ThriftBatch(long batchIndex, long startRowOffset) {
    this.batchIndex = batchIndex;
    this.startRowOffset = startRowOffset;
    this.status = Status.PENDING;
    this.rowCount = 0;
    this.readyFuture = new CompletableFuture<>();
  }

  /**
   * Marks the batch as currently being fetched. Should be called before initiating the network
   * request.
   */
  public void setFetching() {
    this.status = Status.FETCHING;
  }

  /**
   * Sets the batch data after successful fetch. Transitions status to READY and completes the
   * readyFuture.
   *
   * @param response The Thrift fetch response
   * @param view The columnar view created from the response
   */
  public void setData(TFetchResultsResp response, ColumnarRowView view) {
    this.response = response;
    this.columnarView = view;
    this.rowCount = view.getRowCount();
    this.status = Status.READY;
    this.readyFuture.complete(this);
  }

  /**
   * Sets an error that occurred during fetch. Transitions status to ERROR and completes the
   * readyFuture exceptionally.
   *
   * @param error The exception that occurred
   */
  public void setError(Throwable error) {
    this.error = error;
    this.status = Status.ERROR;
    this.readyFuture.completeExceptionally(error);
  }

  /**
   * Blocks until the batch is ready (READY or ERROR state).
   *
   * @param timeoutSeconds Maximum time to wait
   * @throws InterruptedException if the waiting thread is interrupted
   * @throws ExecutionException if the fetch failed with an error
   * @throws TimeoutException if the timeout expires before the batch is ready
   */
  public void waitUntilReady(long timeoutSeconds)
      throws InterruptedException, ExecutionException, TimeoutException {
    readyFuture.get(timeoutSeconds, TimeUnit.SECONDS);
  }

  /**
   * Checks if the batch is ready without blocking.
   *
   * @return true if status is READY
   */
  public boolean isReady() {
    return status == Status.READY;
  }

  /**
   * Releases memory associated with this batch. After calling this method, the batch data is no
   * longer accessible.
   */
  public void release() {
    this.response = null;
    this.columnarView = null;
    this.status = Status.RELEASED;
  }

  /**
   * Returns the sequential batch index (0-based).
   *
   * @return the batch index
   */
  public long getBatchIndex() {
    return batchIndex;
  }

  /**
   * Returns the row offset where this batch starts.
   *
   * @return the start row offset
   */
  public long getStartRowOffset() {
    return startRowOffset;
  }

  /**
   * Returns the number of rows in this batch. Only valid after setData() is called.
   *
   * @return the row count
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * Checks if there are more rows after this batch. Only valid after setData() is called.
   *
   * @return true if the server indicates more rows are available
   */
  public boolean hasMoreRows() {
    return response != null && response.hasMoreRows;
  }

  /**
   * Returns the columnar view for accessing batch data. Only valid after setData() is called.
   *
   * @return the columnar view, or null if not ready
   */
  public ColumnarRowView getColumnarView() {
    return columnarView;
  }

  /**
   * Returns the current status of this batch.
   *
   * @return the status
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Returns the error that occurred during fetch. Only valid if status is ERROR.
   *
   * @return the error, or null if no error occurred
   */
  public Throwable getError() {
    return error;
  }

  /**
   * Returns the raw Thrift response. Only valid after setData() is called.
   *
   * @return the response, or null if not ready
   */
  public TFetchResultsResp getResponse() {
    return response;
  }

  @Override
  public String toString() {
    return String.format(
        "ThriftBatch[index=%d, offset=%d, rows=%d, status=%s]",
        batchIndex, startRowOffset, rowCount, status);
  }
}
