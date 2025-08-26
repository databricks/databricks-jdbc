package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.CATALOG_FULL_COLUMN;
import static com.databricks.jdbc.common.MetadataResultConstants.CATALOG_RESULT_COLUMN;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SchemasDatabricksResultSetAdapter implements IDatabricksResultSetAdapter {
  @Override
  public ResultColumn mapColumn(ResultColumn column) {
    String columnName = column.getResultSetColumnName();
    if (columnName.equals(CATALOG_FULL_COLUMN.getResultSetColumnName())) {
      // Map CATALOG_FULL_COLUMN to CATALOG_COLUMN_FOR_GET_CATALOGS of result set
      return CATALOG_RESULT_COLUMN;
    } else {
      return column;
    }
  }

  @Override
  public boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException {
    return true;
  }
}
