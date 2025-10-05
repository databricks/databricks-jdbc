package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import java.util.Optional;

public interface IRetryStrategy {
  // Tells after how much time the request should be retried (and returns empty if it shouldn't) if
  // a response is received successfully.
  Optional<Integer> shouldRetryAfter(
      int statusCode,
      Optional<Integer> retryAfterHeader,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext,
      RetryTimeoutManager retryTimeoutManager);

  //  Tells after how much time the request should be retried (and returns empty if it shouldn't) if
  //  an exception is thrown while executing the request.
  Optional<Integer> shouldRetryAfter(
      Exception e, int executionAttempt, RetryTimeoutManager retryTimeoutManager);
}
