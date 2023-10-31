package com.databricks.jdbc.core;

import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.SQLException;

public interface IDatabricksResultSet {
  String statementId();

  StatementStatus getStatementStatus();

  long getUpdateCount() throws SQLException;

  boolean hasUpdateCount() throws SQLException;
}
