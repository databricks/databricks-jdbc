package com.databricks.jdbc.common;

import java.util.Collections;
import java.util.Set;
import org.apache.http.HttpStatus;

/** Utility class containing standard HTTPRequestConfig configurations. */
public final class HTTPRequestConfigList {

  // Standard retryable HTTP status codes for idempotent requests
  private static final Set<Integer> DEFAULT_IDEMPOTENT_RETRYABLE_CODES =
      Set.of(
          HttpStatus.SC_TOO_MANY_REQUESTS, // 429
          HttpStatus.SC_BAD_GATEWAY, // 502
          HttpStatus.SC_SERVICE_UNAVAILABLE // 503
          );

  // Retryable HTTP status codes for non-idempotent requests
  private static final Set<Integer> DEFAULT_NON_IDEMPOTENT_RETRYABLE_CODES =
      Set.of(
          HttpStatus.SC_TOO_MANY_REQUESTS, // 429
          HttpStatus.SC_SERVICE_UNAVAILABLE // 503
          );

  /**
   * Configuration for idempotent requests that can be safely retried - 5 retries maximum - 30
   * second timeout - Retryable on temporary server errors
   */
  public static final HTTPRequestConfig DEFAULT_IDEMPOTENT_CONFIG =
      new HTTPRequestConfig(
          RequestIdempotency.IDEMPOTENT,
          5,
          30000, // 30 seconds
          DEFAULT_IDEMPOTENT_RETRYABLE_CODES);

  /**
   * Configuration for non-idempotent requests with limited retry on clear temporary failures - 3
   * retries maximum - 30 second timeout - Retryable on rate limiting and service unavailable errors
   */
  public static final HTTPRequestConfig DEFAULT_NON_IDEMPOTENT_CONFIG =
      new HTTPRequestConfig(
          RequestIdempotency.NON_IDEMPOTENT,
          3,
          30000, // 30 seconds
          DEFAULT_NON_IDEMPOTENT_RETRYABLE_CODES);

  // When Config is not set (in general default config)
  public static final HTTPRequestConfig NOT_SET_CONFIG =
      new HTTPRequestConfig(RequestIdempotency.NOT_SET, 0, 0, Collections.emptySet());
}
