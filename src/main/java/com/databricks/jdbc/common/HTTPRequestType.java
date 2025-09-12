package com.databricks.jdbc.common;

public enum HTTPRequestType {
  UNKNOWN(RequestRetryability.NON_IDEMPOTENT),
  FEATURE_FLAGS(RequestRetryability.IDEMPOTENT),
  THRIFT_OPEN_SESSION(RequestRetryability.IDEMPOTENT),
  THRIFT_CLOSE_SESSION(RequestRetryability.IDEMPOTENT),
  THRIFT_METADATA(RequestRetryability.IDEMPOTENT),
  THRIFT_CLOSE_OPERATION(RequestRetryability.IDEMPOTENT),
  THRIFT_CANCEL_OPERATION(RequestRetryability.IDEMPOTENT),
  THRIFT_EXECUTE_STATEMENT(RequestRetryability.NON_IDEMPOTENT),
  THRIFT_FETCH_RESULTS(RequestRetryability.NON_IDEMPOTENT),
  CLOUD_FETCH(RequestRetryability.IDEMPOTENT),
  VOLUME_LIST(RequestRetryability.IDEMPOTENT),
  VOLUME_SHOW_VOLUMES(RequestRetryability.IDEMPOTENT),
  VOLUME_GET(RequestRetryability.IDEMPOTENT),
  VOLUME_PUT(RequestRetryability.NON_IDEMPOTENT),
  VOLUME_DELETE(RequestRetryability.IDEMPOTENT),
  AUTH(RequestRetryability.IDEMPOTENT),
  TELEMETRY_PUSH(RequestRetryability.IDEMPOTENT);

  private final RequestRetryability requestRetryability;

  HTTPRequestType(RequestRetryability requestRetryability) {
    this.requestRetryability = requestRetryability;
  }

  public RequestRetryability getRequestRetryability() {
    return requestRetryability;
  }
}
