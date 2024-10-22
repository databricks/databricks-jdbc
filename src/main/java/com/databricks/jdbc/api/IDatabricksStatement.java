package com.databricks.jdbc.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement extends Statement {

  /**
   * Executes the given SQL command in async mode, and returns a lightweight instance of result set
   *
   * @param sql SQL command to be executed
   * @return result set for given execution
   * @throws SQLException in case of error
   */
  ResultSet executeAsync(String sql) throws SQLException;

  /**
   * Returns result set response for the executed statement
   *
   * @return result set for underlying execution
   * @throws SQLException if statement was never executed
   */
  ResultSet getExecutionResult() throws SQLException;
}
