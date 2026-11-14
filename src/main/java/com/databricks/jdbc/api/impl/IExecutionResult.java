package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;

/** Interface to provide methods over an underlying statement result */
public interface IExecutionResult {

  /**
   * Get the object for given column index. Here index starts with 0.
   *
   * @param columnIndex index of column starting with 0
   * @return object at given index
   * @throws DatabricksSQLException if there is any error in getting object
   */
  Object getObject(int columnIndex) throws DatabricksSQLException;

  /**
   * Get the object for given column index in a form that converts to {@code java.time} types
   * without loss. Here index starts with 0.
   *
   * <p>For DATE and TIMESTAMP columns, Arrow results read the epoch days and microseconds straight
   * from the vector and return a {@link java.time.LocalDate} (DATE), {@link
   * java.time.LocalDateTime} (TIMESTAMP_NTZ) or {@link java.time.OffsetDateTime} in the session
   * time zone (TIMESTAMP). In every other case this returns the same value as {@link
   * #getObject(int)}, which for text-based results is the server's text for the value.
   *
   * @param columnIndex index of column starting with 0
   * @return object at given index, or null for SQL NULL
   * @throws DatabricksSQLException if there is any error in getting object
   */
  default Object getTemporalObject(int columnIndex) throws DatabricksSQLException {
    return getObject(columnIndex);
  }

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
  boolean next() throws SQLException;

  /** Returns if there is next row in the result set */
  boolean hasNext();

  /** Closes the result set and releases any in-memory chunks or data */
  void close();

  long getRowCount();

  long getChunkCount();
}
