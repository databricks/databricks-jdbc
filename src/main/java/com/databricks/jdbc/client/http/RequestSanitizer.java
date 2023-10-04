package com.databricks.jdbc.client.http;

import org.apache.http.client.methods.HttpUriRequest;

public class RequestSanitizer {
  public static String sanitizeRequest(HttpUriRequest request) {
    // TODO: sanitize in better way
    return request.getURI().toString();
  }
}
