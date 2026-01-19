package com.databricks.jdbc.api.impl.streaming;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

/**
 * Type-safe batch container for streaming results.
 *
 * <p>This generic container holds batch data of any type (ColumnarRowView or ArrowResultChunk) and
 * manages batch lifecycle including status tracking and resource cleanup.
 *
 * @param <T> The type of data held (ColumnarRowView or ArrowResultChunk)
 */
public class StreamingBatch<T> {

  /** Batch lifecycle status. */
  public enum Status {
    PENDING, // Not yet fetched
    FETCHING, // Fetch in progress
    READY, // Data available
    ERROR, // Fetch failed
    RELEASED // Memory released
  }

  private final long batchIndex;
  private final long rowOffset;
  private volatile Status status;

  // Type-safe data storage
  private volatile T data;
  private final Consumer<T> releaseAction;

  // Metadata from response
  private volatile long rowCount;
  private volatile boolean hasMoreRows;
  private volatile Throwable error;
  private final CompletableFuture<StreamingBatch<T>> readyFuture;

  /**
   * Creates a new batch with the specified release action.
   *
   * @param batchIndex The batch index
   * @param rowOffset The starting row offset
   * @param releaseAction Action to call when releasing data (e.g., ArrowResultChunk::releaseChunk)
   */
  public StreamingBatch(long batchIndex, long rowOffset, Consumer<T> releaseAction) {
    this.batchIndex = batchIndex;
    this.rowOffset = rowOffset;
    this.releaseAction = releaseAction;
    this.status = Status.PENDING;
    this.readyFuture = new CompletableFuture<>();
  }

  /**
   * Sets the batch data and marks as ready.
   *
   * @param data The batch data
   * @param rowCount Number of rows in this batch
   * @param hasMoreRows Whether more batches are available after this one
   */
  public void setData(T data, long rowCount, boolean hasMoreRows) {
    this.data = data;
    this.rowCount = rowCount;
    this.hasMoreRows = hasMoreRows;
    this.status = Status.READY;
    this.readyFuture.complete(this);
  }

  /**
   * Gets the typed data. No casting required!
   *
   * @return The batch data
   */
  public T getData() {
    return data;
  }

  /** Sets the batch as currently fetching. */
  public void setFetching() {
    this.status = Status.FETCHING;
  }

  /**
   * Sets error state.
   *
   * @param error The error that occurred
   */
  public void setError(Throwable error) {
    this.error = error;
    this.status = Status.ERROR;
    this.readyFuture.completeExceptionally(error);
  }

  /**
   * Waits for the batch to be ready.
   *
   * @param timeoutSeconds Maximum time to wait in seconds
   * @throws InterruptedException if waiting is interrupted
   * @throws ExecutionException if the batch fetch failed
   * @throws TimeoutException if the timeout is exceeded
   */
  public void waitUntilReady(long timeoutSeconds)
      throws InterruptedException, ExecutionException, TimeoutException {
    readyFuture.get(timeoutSeconds, TimeUnit.SECONDS);
  }

  /**
   * Checks if the batch is ready.
   *
   * @return true if data is available
   */
  public boolean isReady() {
    return status == Status.READY;
  }

  /** Releases the batch data using the type-specific release action. */
  public void release() {
    if (data != null && releaseAction != null) {
      releaseAction.accept(data);
    }
    this.data = null;
    this.status = Status.RELEASED;
  }

  // Getters

  /**
   * Gets the batch index.
   *
   * @return The zero-based batch index
   */
  public long getBatchIndex() {
    return batchIndex;
  }

  /**
   * Gets the row offset.
   *
   * @return The starting row offset for this batch
   */
  public long getRowOffset() {
    return rowOffset;
  }

  /**
   * Gets the row count.
   *
   * @return The number of rows in this batch
   */
  public long getRowCount() {
    return rowCount;
  }

  /**
   * Checks if more rows are available after this batch.
   *
   * @return true if more batches are available
   */
  public boolean hasMoreRows() {
    return hasMoreRows;
  }

  /**
   * Gets the current status.
   *
   * @return The batch status
   */
  public Status getStatus() {
    return status;
  }

  /**
   * Gets the error if status is ERROR.
   *
   * @return The error, or null if no error occurred
   */
  public Throwable getError() {
    return error;
  }
}
