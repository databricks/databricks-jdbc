package com.databricks.jdbc.api.impl.streaming;

import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import java.util.function.Consumer;

/**
 * Processes a Thrift fetch response into typed result data.
 *
 * <p>Implementations handle the conversion of {@link TFetchResultsResp} into specific data types
 * (ColumnarRowView for columnar results, ArrowResultChunk for inline Arrow results).
 *
 * @param <T> The type of data produced (ColumnarRowView or ArrowResultChunk)
 */
public interface ThriftResponseProcessor<T> {

  /**
   * Processes the initial response and returns batch data.
   *
   * <p>This method is called once when the streaming provider is created. It may perform additional
   * initialization such as caching schema information.
   *
   * @param response The initial Thrift fetch response
   * @return A StreamingBatch containing the processed data
   * @throws DatabricksSQLException if processing fails
   */
  StreamingBatch<T> processInitialResponse(TFetchResultsResp response)
      throws DatabricksSQLException;

  /**
   * Processes a fetched response and returns batch data.
   *
   * <p>This method is called for each subsequent batch fetched from the server.
   *
   * @param response The Thrift fetch response
   * @param batchIndex The zero-based batch index
   * @param rowOffset The starting row offset for this batch
   * @return A StreamingBatch containing the processed data
   * @throws DatabricksSQLException if processing fails
   */
  StreamingBatch<T> processResponse(TFetchResultsResp response, long batchIndex, long rowOffset)
      throws DatabricksSQLException;

  /**
   * Creates a release action for the data type.
   *
   * <p>This action is called when the batch is released from memory. For types that require
   * explicit cleanup (like ArrowResultChunk with native memory), this should perform that cleanup.
   * For types that don't need cleanup (like ColumnarRowView), this can be a no-op.
   *
   * @return A Consumer that releases the data
   */
  Consumer<T> getReleaseAction();
}
