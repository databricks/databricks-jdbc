package com.databricks.jdbc.core;

import java.sql.SQLException;
import java.sql.Statement;

/** Interface for Databricks specific statement. */
public interface IDatabricksStatement {

  /** Returns the underlying session-Id for the statement. */
  String getSessionId();

  void close(boolean removeFromSession) throws SQLException;

  void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException;

  int getMaxRows() throws SQLException;

  void setStatementId(String statementId);

  String getStatementId();

  default Statement getStatement() {
    return (Statement) this;
  }
}
