package com.databricks.jdbc.core;

import java.sql.SQLException;

/** Interface to provide methods over an underlying statement result */
interface IExecutionResult {

  /**
   * Get the object for given column index. Here index starts with 0.
   *
   * @param columnIndex index of column starting with 0
   * @return object at given index
   * @throws SQLException
   */
  Object getObject(int columnIndex) throws SQLException;

  /**
   * Gets the current row position, starting with 0.
   *
   * @return the current row position
   */
  long getCurrentRow();

  /**
   * Moves the cursor to next row and returns true if this can be done
   *
   * @return true if cursor is moved at next row
   */
  boolean next() throws DatabricksSQLException;

  /** Returns if there is next row in the result set */
  boolean hasNext();

  /** Closes the result set and releases any in-memory chunks or data */
  void close();
}
