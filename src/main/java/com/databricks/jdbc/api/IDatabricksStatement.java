package com.databricks.jdbc.api;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /**
   * Executes the given SQL command in async mode
   *
   * @param sql SQL command to be executed
   * @return result set handle
   * @throws DatabricksSQLException in case of error
   */
  IDatabricksResultSet executeAsync(String sql) throws SQLException;

  /**
   * Returns result set response for the executed statement
   *
   * @return result set handle
   * @throws DatabricksSQLException if statement was never executed
   */
  IDatabricksResultSet getExecutionResult() throws SQLException;
}
