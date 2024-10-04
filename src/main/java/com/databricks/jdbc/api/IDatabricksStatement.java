package com.databricks.jdbc.api;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.ResultSet;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /**
   * Executes the given SQL command in async mode, and returns a lightweight instance of result set
   *
   * @param sql SQL command to be executed
   * @return result set for given execution
   * @throws DatabricksSQLException in case of error
   */
  ResultSet executeAsync(String sql) throws DatabricksSQLException;

  /**
   * Returns result set response for the executed statement
   *
   * @return result set for underlying execution
   * @throws DatabricksSQLException if statement was never executed
   */
  ResultSet getExecutionResult() throws DatabricksSQLException;
}
