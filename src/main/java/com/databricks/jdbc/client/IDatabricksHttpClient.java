package com.databricks.jdbc.client;

import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpUriRequest;

/**
 * Http client interface for executing http requests.
 */
public interface IDatabricksHttpClient {

  /**
   * Executes the given http request and returns the response
   * TODO: add error handling
   *
   * @param request underlying http request
   * @return http response
   */
  HttpResponse execute(HttpUriRequest request) throws DatabricksHttpException;
}
