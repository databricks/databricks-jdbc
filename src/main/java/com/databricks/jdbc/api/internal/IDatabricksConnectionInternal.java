package com.databricks.jdbc.api.internal;

import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.Connection;

/** Interface providing Databricks specific Connection APIs. */
public interface IDatabricksConnectionInternal extends Connection {

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

  /** Opens the connection and initiates the underlying session */
  void open() throws DatabricksSQLException;

  /** Returns the connection context associated with the connection. */
  IDatabricksConnectionContext getConnectionContext();
}
