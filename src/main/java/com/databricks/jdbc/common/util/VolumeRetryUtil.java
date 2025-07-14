package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.VOLUME_OPERATION_MAX_RETRIES;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;

/**
 * Utility class for volume operation retry logic. Centralizes retry decision logic for both
 * presigned URL requests and upload operations.
 */
public class VolumeRetryUtil {

  /** Maximum number of retry attempts for volume operations. */
  public static final int MAX_RETRIES = VOLUME_OPERATION_MAX_RETRIES;

  /**
   * Check if the given HTTP status code is retryable.
   *
   * @param statusCode The HTTP status code to check
   * @param connectionContext The connection context (can be null for tests)
   * @return true if the status code is retryable, false otherwise
   */
  public static boolean isRetryableHttpCode(
      int statusCode, IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null) {
      return connectionContext.getUCIngestionRetriableHttpCodes().contains(statusCode);
    }
    return DatabricksJdbcConstants.DEFAULT_UC_INGESTION_RETRYABLE_HTTP_CODES.contains(statusCode);
  }

  /**
   * Check if we should retry based on attempt count and elapsed time.
   *
   * @param attempt Current attempt number (1-based)
   * @param retryStartTime Start time of the retry sequence in milliseconds
   * @param connectionContext The connection context (can be null for tests)
   * @return true if we should retry, false otherwise
   */
  public static boolean shouldRetry(
      int attempt, long retryStartTime, IDatabricksConnectionContext connectionContext) {
    if (attempt >= MAX_RETRIES) {
      return false;
    }

    // Check timeout before retrying
    long elapsedSeconds = (System.currentTimeMillis() - retryStartTime) / 1000;
    int timeoutSeconds =
        connectionContext != null
            ? connectionContext.getUCIngestionRetryTimeoutSeconds()
            : DatabricksJdbcConstants.DEFAULT_UC_INGESTION_RETRY_TIMEOUT_SECONDS;
    return elapsedSeconds < timeoutSeconds;
  }

  /**
   * Get the retry timeout in seconds.
   *
   * @param connectionContext The connection context (can be null for tests)
   * @return The retry timeout in seconds
   */
  public static int getRetryTimeoutSeconds(IDatabricksConnectionContext connectionContext) {
    return connectionContext != null
        ? connectionContext.getUCIngestionRetryTimeoutSeconds()
        : DatabricksJdbcConstants.DEFAULT_UC_INGESTION_RETRY_TIMEOUT_SECONDS;
  }
}
