package com.databricks.jdbc.api;

public interface IExecutionStatus {
  /**
   * Returns the error message if the statement execution failed.
   *
   * @return the error message, or null if there was no error
   */
  String getErrorMessage();

  /**
   * Returns the SQL state code if the statement execution failed.
   *
   * @return the SQL state code, or null if there was no error
   */
  String getSqlState();

  /**
   * Returns the current state of the statement execution.
   *
   * @return the current state of the statement execution
   */
  ExecutionState getExecutionState();
}
