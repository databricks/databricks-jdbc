package com.databricks.jdbc.core;

import java.sql.SQLException;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /** Returns the underlying session-Id for the statement. */
  String getSessionId();

  void close(boolean removeFromSession) throws SQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException;

  int getMaxRows() throws SQLException;
}
