package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Optional;

public interface IRetryStrategy {
  /* Tells whether a given HTTP status code is retriable or not */
  boolean isStatusCodeRetriable(int statusCode, IDatabricksConnectionContext connectionContext);

  /* Returns the delay in milliseconds after which a request should be retried, or empty if it shouldn't be retried */
  Optional<Integer> retryRequestAfter(
      int statusCode,
      Optional<Integer> retryAfterHeader,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext);

  boolean isExceptionRetryable(Exception e);
}
