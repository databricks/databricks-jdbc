package com.databricks.jdbc.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Extends the standard JDBC {@link Connection} interface to provide Databricks-specific
 * functionality. This interface adds methods to retrieve statement handles and connection
 * identifiers.
 */
public interface IDatabricksConnection extends Connection {

  /**
   * Retrieves a statement handle for a given statement ID.
   *
   * @param statementId The unique identifier of the statement to retrieve
   * @return A {@link Statement} object representing the statement
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *     connection
   */
  Statement getStatement(String statementId) throws SQLException;

  /**
   * Retrieves the unique identifier for this connection.
   *
   * @return A string representing the unique connection ID
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *     connection
   */
  String getConnectionId() throws SQLException;
}
