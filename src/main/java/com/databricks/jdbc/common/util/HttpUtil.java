package com.databricks.jdbc.common.util;

import org.apache.http.client.methods.CloseableHttpResponse;

public class HttpUtil {

  public static final String VOLUME_OPERATION_TYPE_GET = "get";
  public static final String VOLUME_OPERATION_TYPE_PUT = "put";
  public static final String VOLUME_OPERATION_TYPE_REMOVE = "remove";

  /** Check if the HTTP response is successful */
  public static boolean isSuccessfulHttpResponse(CloseableHttpResponse response) {
    return response.getStatusLine().getStatusCode() >= 200
        && response.getStatusLine().getStatusCode() < 300;
  }
}
