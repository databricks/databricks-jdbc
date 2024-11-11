package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.exception.DatabricksHttpException;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/** Http client interface for executing http requests. */
public interface IDatabricksHttpClient {

  /**
   * Executes the given http request and returns the response
   *
   * @param request underlying http request
   * @return http response
   */
  CloseableHttpResponse execute(HttpUriRequest request) throws DatabricksHttpException;
}
