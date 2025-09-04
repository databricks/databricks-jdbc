package com.databricks.client.jdbc;

import java.sql.SQLException;
import java.util.Properties;

/** Extension interface for java.sql.Driver */
public interface IDatabricksDriver extends java.sql.Driver {

  /** Closes a connection for given connection-Id */
  void closeConnection(String url, Properties info, String connectionId) throws SQLException;
}
