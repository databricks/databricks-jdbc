package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.exception.DatabricksHttpException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * https://github.com/apache/httpcomponents-core/blob/5.2.x/httpcore5/src/test/java/org/apache/hc/core5/http/examples/AsyncRequestExecutionExample.java
 * can we follow this to make all http client call async
 */

/** Http client interface for executing http requests. */
public interface IDatabricksHttpClient {

  /**
   * Executes the given http request and returns the response TODO: add error handling
   *
   * @param request underlying http request
   * @return http response
   */
  CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException;

  void closeExpiredAndIdleConnections();
}
