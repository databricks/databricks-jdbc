package com.databricks.jdbc.api;

/**
 * Represents the possible states of a SQL statement execution in Databricks. This enum is used to
 * track the progress and status of SQL statements executed through the Databricks JDBC API.
 */
public enum ExecutionState {
  // The statement is in a pending state and has not yet started executing.
  PENDING,
  // The statement is currently executing.
  RUNNING,
  // The statement has completed successfully.
  SUCCEEDED,
  // The statement has completed with an error.
  FAILED,
  // The statement has been closed and is no longer available.
  CLOSED,
  // The statement has been cancelled by the user.
  ABORTED;
}
