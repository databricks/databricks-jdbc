package com.databricks.jdbc.common;

public enum RetryPolicy {
  /**
   * Idempotent requests can be safely retried multiple times without side effects. Examples:
   * Metadata Queries.
   */
  IDEMPOTENT,

  /**
   * Non-idempotent requests may have side effects and should be retried carefully. Example: Execute
   * Statement
   */
  NON_IDEMPOTENT
}
