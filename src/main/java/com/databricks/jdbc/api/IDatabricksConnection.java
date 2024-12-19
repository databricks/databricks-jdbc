package com.databricks.jdbc.api;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public interface IDatabricksConnection extends Connection {

  /** Returns the statement handle for given statement-Id */
  Statement getStatement(String statementId) throws SQLException;

  String getConnectionId() throws SQLException;

  void closeConnection(String connectionId) throws SQLException;
}
