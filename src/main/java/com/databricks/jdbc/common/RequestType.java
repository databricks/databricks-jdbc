package com.databricks.jdbc.common;

public enum RequestType {
  UNKNOWN(RetryPolicy.NON_IDEMPOTENT),
  FETCH_FEATURE_FLAGS(RetryPolicy.IDEMPOTENT),
  THRIFT_OPEN_SESSION(RetryPolicy.IDEMPOTENT),
  THRIFT_CLOSE_SESSION(RetryPolicy.IDEMPOTENT),
  THRIFT_METADATA(RetryPolicy.IDEMPOTENT),
  THRIFT_CLOSE_OPERATION(RetryPolicy.IDEMPOTENT),
  THRIFT_CANCEL_OPERATION(RetryPolicy.IDEMPOTENT),
  THRIFT_EXECUTE_STATEMENT(RetryPolicy.NON_IDEMPOTENT),
  THRIFT_FETCH_RESULTS(RetryPolicy.NON_IDEMPOTENT),
  CLOUD_FETCH(RetryPolicy.IDEMPOTENT),
  VOLUME_LIST(RetryPolicy.IDEMPOTENT),
  VOLUME_SHOW_VOLUMES(RetryPolicy.IDEMPOTENT),
  VOLUME_GET(RetryPolicy.IDEMPOTENT),
  VOLUME_PUT(RetryPolicy.NON_IDEMPOTENT),
  VOLUME_DELETE(RetryPolicy.IDEMPOTENT),
  AUTH(RetryPolicy.IDEMPOTENT),
  TELEMETRY_PUSH(RetryPolicy.IDEMPOTENT);

  private final RetryPolicy retryPolicy;

  RequestType(RetryPolicy retryPolicy) {
    this.retryPolicy = retryPolicy;
  }

  public RetryPolicy getRetryPolicy() {
    return retryPolicy;
  }
}
