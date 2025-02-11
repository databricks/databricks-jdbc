package com.databricks.jdbc.api;

import com.databricks.jdbc.model.core.StatementStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

/** Extension to java.sql.ResultSet interface */
public interface IDatabricksResultSet extends ResultSet {

  Struct getStruct(String columnLabel) throws SQLException;

  Map<String, Object> getMap(String columbLabel) throws SQLException;

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

  /**
   * Retrieves the SQL `Map` from the specified column index in the result set.
   *
   * @param columnIndex the index of the column in the result set (1-based)
   * @return a `Map<String, Object>` if the column contains a map; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `MAP` type or if any SQL error occurs
   */
  Map<String, Object> getMap(int columnIndex) throws SQLException;

  /**
   * Retrieves the SQL `Struct` from the specified column index in the result set.
   *
   * @param columnIndex the index of the column in the result set (1-based)
   * @return a `Struct` object if the column contains a struct; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `STRUCT` type or if any SQL error occurs
   */
  Struct getStruct(int columnIndex) throws SQLException;
}
