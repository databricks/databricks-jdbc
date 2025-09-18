package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.common.RequestType;
import com.databricks.jdbc.exception.DatabricksHttpException;
import java.util.concurrent.Future;
import org.apache.hc.core5.concurrent.FutureCallback;
import org.apache.hc.core5.http.nio.AsyncRequestProducer;
import org.apache.hc.core5.http.nio.AsyncResponseConsumer;
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

  /**
   * Executes the given http request and returns the response
   *
   * @param request underlying http request
   * @param supportGzipEncoding whether to support gzip encoding header
   * @return http response
   */
  CloseableHttpResponse execute(HttpUriRequest request, boolean supportGzipEncoding)
      throws DatabricksHttpException;

  /**
   * Executes the given http request with retry logic based on request type
   *
   * @param request underlying http request
   * @param requestType the type of request for retry strategy
   * @return http response
   */
  CloseableHttpResponse executeWithRetry(HttpUriRequest request, RequestType requestType)
      throws DatabricksHttpException;

  /**
   * Executes the given http request with retry logic based on request type
   *
   * @param request underlying http request
   * @param requestType the type of request for retry strategy
   * @param supportGzipEncoding whether to support gzip encoding header
   * @return http response
   */
  CloseableHttpResponse executeWithRetry(
      HttpUriRequest request, RequestType requestType, boolean supportGzipEncoding)
      throws DatabricksHttpException;

  /**
   * Executes the given http request asynchronously and returns the future
   *
   * @param requestProducer request producer
   * @param responseConsumer response consumer
   * @param callback future callback
   * @return future
   * @param <T> type of the response
   */
  <T> Future<T> executeAsync(
      AsyncRequestProducer requestProducer,
      AsyncResponseConsumer<T> responseConsumer,
      FutureCallback<T> callback);
}
