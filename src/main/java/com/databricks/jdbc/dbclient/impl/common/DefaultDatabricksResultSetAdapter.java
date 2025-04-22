package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * A default implementation of {@link IDatabricksResultSetAdapter} that performs identity mapping
 * and permits all rows to pass through without any modifications.
 */
public class DefaultDatabricksResultSetAdapter implements IDatabricksResultSetAdapter {
  @Override
  public ResultColumn mapColumn(ResultColumn column) {
    return column; // No mapping, return the column as-is
  }

  @Override
  public boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException {
    return true; // Include all rows
  }
}
