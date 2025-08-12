package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import org.apache.http.client.methods.CloseableHttpResponse;

public interface IRetryStrategy {
  /* Tells whether a given HTTP status code is retriable or not */
  boolean isStatusCodeRetriable(int statusCode, IDatabricksConnectionContext connectionContext);

  /* Tells after how much time (in milliseconds) should a request be retried and returns -1 if it shouldn't be retried*/
  int retryRequestAfter(
      CloseableHttpResponse response,
      int executionAttempt,
      IDatabricksConnectionContext connectionContext);
}
