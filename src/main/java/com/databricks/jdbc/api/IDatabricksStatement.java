package com.databricks.jdbc.api;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extends the standard JDBC {@link Statement} interface to provide Databricks-specific
 * functionality. This interface adds support for asynchronous query execution and result retrieval,
 * allowing for better handling of long-running queries and improved performance in distributed
 * environments.
 */
public interface IDatabricksStatement extends Statement {

  /**
   * Executes the given SQL command asynchronously and returns a lightweight result set handle. This
   * method initiates the execution but does not wait for it to complete, making it suitable for
   * long-running queries. The actual results can be retrieved later using {@link
   * #getExecutionResult()}.
   *
   * @param sql The SQL command to be executed
   * @return A {@link ResultSet} handle that can be used to track and retrieve the results
   * @throws SQLException if a database access error occurs, this method is called on a closed
   *     statement, or the SQL command is not valid
   */
  ResultSet executeAsync(String sql) throws SQLException;

  /**
   * Retrieves the result set for a previously executed statement. This method should be called
   * after executing a statement using {@link #executeAsync(String)} to get the actual results.
   *
   * @return A {@link ResultSet} containing the results of the statement execution in case of
   *     successful completion, else handle for the result status.
   * @throws SQLException if the statement was never executed, has been closed, or if a database
   *     access error occurs
   */
  ResultSet getExecutionResult() throws SQLException;
}
