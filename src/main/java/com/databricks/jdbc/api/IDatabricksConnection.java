package com.databricks.jdbc.api;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/** Interface providing Databricks specific Connection APIs. */
public interface IDatabricksConnection {

  /** Returns the underlying session for the connection. */
  IDatabricksSession getSession();

  /**
   * Closes a statement from the connection's active set.
   *
   * @param statement {@link IDatabricksStatement} to be closed
   */
  void closeStatement(IDatabricksStatement statement);

  /** Returns the corresponding sql connection object */
  Connection getConnection();

  /** Returns a UC Volume client instance */
  IDatabricksUCVolumeClient getUCVolumeClient();

  /** Opens the connection and initiates the underlying session */
  void open() throws DatabricksSQLException;

  /** Returns the connection context associated with the connection. */
  IDatabricksConnectionContext getConnectionContext();

  /** Returns the statement handle for given statement-Id */
  Statement getStatement(String statementId) throws SQLException;
}
