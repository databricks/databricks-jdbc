package com.databricks.jdbc.api.impl.thrift;

import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;

/**
 * Default implementation of ThriftBatchFetcher that uses the session's Databricks client to fetch
 * batches.
 *
 * <p>This implementation delegates to {@code session.getDatabricksClient().getMoreResults()} which
 * uses Thrift's FETCH_NEXT orientation to retrieve sequential batches.
 */
public class ThriftBatchFetcherImpl implements ThriftBatchFetcher {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(ThriftBatchFetcherImpl.class);

  private final IDatabricksSession session;
  private final IDatabricksStatementInternal statement;
  private volatile boolean closed;

  /**
   * Creates a new ThriftBatchFetcherImpl.
   *
   * @param session The session to use for fetching
   * @param statement The statement that generated the result
   */
  public ThriftBatchFetcherImpl(
      IDatabricksSession session, IDatabricksStatementInternal statement) {
    this.session = session;
    this.statement = statement;
    this.closed = false;
    LOGGER.debug(
        "Created ThriftBatchFetcherImpl for statement {}",
        statement != null ? statement.getStatementId() : "null");
  }

  @Override
  public TFetchResultsResp fetchNextBatch() throws DatabricksSQLException {
    if (closed) {
      LOGGER.error("Attempted to fetch batch from closed ThriftBatchFetcher");
      throw new DatabricksSQLException(
          "ThriftBatchFetcher is closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }

    LOGGER.debug(
        "Fetching next batch for statement {}",
        statement != null ? statement.getStatementId() : "null");
    long startTime = System.currentTimeMillis();

    TFetchResultsResp response = session.getDatabricksClient().getMoreResults(statement);

    long duration = System.currentTimeMillis() - startTime;
    LOGGER.debug(
        "Fetched batch in {}ms, hasMoreRows: {}",
        duration,
        response != null && response.hasMoreRows);

    return response;
  }

  @Override
  public void close() {
    LOGGER.debug("Closing ThriftBatchFetcherImpl");
    this.closed = true;
  }

  @Override
  public boolean isClosed() {
    return closed;
  }
}
