package com.databricks.jdbc.api;

import com.databricks.jdbc.model.core.StatementStatus;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Struct;
import java.util.Map;

/**
 * Extends the standard JDBC {@link ResultSet} interface to provide Databricks-specific
 * functionality. This interface adds support for complex data types like Structs and Maps, as well
 * as methods to retrieve statement status and execution information.
 */
public interface IDatabricksResultSet extends ResultSet {

  /**
   * Retrieves the SQL `Struct` from the specified column using its label.
   *
   * @param columnLabel the label for the column specified in the SQL query
   * @return a `Struct` object if the column contains a struct; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `STRUCT` type or if any SQL error occurs
   */
  Struct getStruct(String columnLabel) throws SQLException;

  /**
   * Retrieves the SQL `Map` from the specified column using its label.
   *
   * @param columnLabel the label for the column specified in the SQL query
   * @return a `Map<String, Object>` if the column contains a map; `null` if the value is SQL `NULL`
   * @throws SQLException if the column is not of `MAP` type or if any SQL error occurs
   */
  Map<String, Object> getMap(String columnLabel) throws SQLException;

  /**
   * Retrieves the unique identifier of the statement associated with this result set.
   *
   * @return A string representing the statement ID
   */
  String getStatementId();

  /**
   * Retrieves the current status of the statement associated with this result set. This can be used
   * to monitor the execution progress and state of the statement.
   *
   * @return The current {@link StatementStatus} of the statement
   */
  StatementStatus getStatementStatus();

  /**
   * Retrieves the number of rows affected by the SQL statement. For SELECT statements or statements
   * that don't modify data, this will return 0.
   *
   * @return The number of rows affected by INSERT, UPDATE, or DELETE statements
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *     result set
   */
  long getUpdateCount() throws SQLException;

  /**
   * Checks whether the executed SQL statement has produced an update count. This is typically true
   * for DML (Data Manipulation Language) statements like INSERT, UPDATE, or DELETE.
   *
   * @return true if the statement has produced an update count, false otherwise
   * @throws SQLException if a database access error occurs or this method is called on a closed
   *     result set
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
