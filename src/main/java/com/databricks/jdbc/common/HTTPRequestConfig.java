package com.databricks.jdbc.common;

import java.util.Set;

public class HTTPRequestConfig {
  public final RequestIdempotency currentRequestIdempotency;
  public final int maxRetries;
  public final int requestTimeoutInMilis;
  public final Set<Integer> retryableHTTPCodes;

  public HTTPRequestConfig(
      RequestIdempotency currentRequestIdempotency,
      int maxRetries,
      int requestTimeoutInMilis,
      Set<Integer> retryableHTTPCodes) {
    this.currentRequestIdempotency = currentRequestIdempotency;
    this.maxRetries = maxRetries;
    this.requestTimeoutInMilis = requestTimeoutInMilis;
    this.retryableHTTPCodes = retryableHTTPCodes;
  }
}
