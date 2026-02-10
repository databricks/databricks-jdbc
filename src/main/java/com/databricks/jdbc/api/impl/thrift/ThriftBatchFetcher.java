package com.databricks.jdbc.api.impl.thrift;

import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import java.sql.SQLException;

/**
 * Interface for fetching Thrift columnar result batches from the server.
 *
 * <p>This abstraction enables testing with mock implementations and potential future enhancements
 * like caching or instrumentation.
 *
 * <p>Implementations must be thread-safe as the fetcher may be called from the prefetch thread
 * while the main thread is consuming data.
 */
public interface ThriftBatchFetcher {

  /**
   * Fetches the next batch of results from the server.
   *
   * <p>This is a blocking network call that uses the Thrift FETCH_NEXT orientation. Each call
   * returns the next sequential batch from the server's cursor.
   *
   * <p><b>Thread Safety:</b> This method should only be called from a single thread at a time, as
   * the server maintains a cursor that advances with each call.
   *
   * @return The fetch response containing the batch data and hasMoreRows flag
   * @throws SQLException if the fetch fails due to network or server errors
   */
  TFetchResultsResp fetchNextBatch() throws SQLException;

  /**
   * Closes the fetcher and releases any associated resources.
   *
   * <p>After calling close(), any subsequent calls to fetchNextBatch() should throw an exception.
   */
  void close();

  /**
   * Checks if the fetcher has been closed.
   *
   * @return true if close() has been called
   */
  boolean isClosed();
}
