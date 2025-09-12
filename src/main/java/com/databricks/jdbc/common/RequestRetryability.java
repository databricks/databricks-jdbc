package com.databricks.jdbc.common;

public enum RequestRetryability {
  /**
   * Idempotent requests can be safely retried multiple times without side effects. Examples: GET
   * requests, metadata queries, fetch operations.
   */
  IDEMPOTENT,

  /**
   * Non-idempotent requests may have side effects and should be retried carefully. Examples: POST
   * requests, session creation, data modification operations.
   */
  NON_IDEMPOTENT
}
