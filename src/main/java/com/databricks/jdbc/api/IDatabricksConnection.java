package com.databricks.jdbc.api;

import java.sql.Connection;

/** Interface providing Databricks specific Connection APIs. */
public interface IDatabricksConnection {

  /** Returns the underlying session for the connection. */
  IDatabricksSession getSession();

  /**
   * Closes a statement from the connection's active set.
   *
   * @param statement
   */
  void closeStatement(IDatabricksStatement statement);

  /** Returns the corresponding sql connection object */
  Connection getConnection();

  /** Returns a UC Volume client instance */
  IDatabricksUCVolumeClient getUCVolumeClient();
}
