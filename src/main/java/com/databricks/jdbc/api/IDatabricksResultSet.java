package com.databricks.jdbc.api;

import com.databricks.sdk.service.sql.StatementStatus;
import java.sql.SQLException;

/** Extension to java.sql.ResultSet interface */
public interface IDatabricksResultSet {

  /**
   * Returns statement-Id of associated statement
   *
   * @return statement-Id
   */
  String getStatementId();

  /**
   * Fetches Statement status for underlying statement
   *
   * @return statement status
   */
  StatementStatus getStatementStatus();

  /**
   * Returns update count for underlying statement execution. Returns 0 for a query statement.
   *
   * @return update count
   * @throws SQLException
   */
  long getUpdateCount() throws SQLException;

  /**
   * Checks if there is an update count for underlying statement execution
   *
   * @return true for DML commands
   * @throws SQLException
   */
  boolean hasUpdateCount() throws SQLException;
}
