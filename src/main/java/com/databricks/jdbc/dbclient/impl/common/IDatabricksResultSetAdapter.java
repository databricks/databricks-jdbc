package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Interface for processing result sets with mapping and filtering capabilities to align with JDBC
 * spec.
 */
interface IDatabricksResultSetAdapter {
  /**
   * Maps a result column to a potentially different column name in the result set.
   *
   * @param column The original column definition
   * @return The mapped column to be used for retrieving data from the result set
   */
  ResultColumn mapColumn(ResultColumn column);

  /**
   * Determines whether a result set row should be included in the final results.
   *
   * @param resultSet The current result set positioned at the row to evaluate
   * @param columns The list of columns being processed
   * @return true if the row should be included, false if it should be skipped
   * @throws SQLException if there's an error accessing the result set
   */
  boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException;

  /**
   * Optional method to transform a value after it's retrieved from the result set.
   *
   * @param column The column definition
   * @param value The value retrieved from the result set
   * @return The potentially transformed value
   */
  default Object transformValue(ResultColumn column, Object value) {
    return value;
  }
}
