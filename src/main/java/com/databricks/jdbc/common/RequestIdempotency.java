package com.databricks.jdbc.common;

// enumerates the types of HTTP requests

public enum RequestIdempotency {
  // Safe to retry
  IDEMPOTENT,

  // Unsafe to retry
  NON_IDEMPOTENT,

  NOT_SET
}
