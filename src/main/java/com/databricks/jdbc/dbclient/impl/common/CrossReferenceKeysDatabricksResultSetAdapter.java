package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.PKTABLE_NAME;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class CrossReferenceKeysDatabricksResultSetAdapter
    extends ImportedKeysDatabricksResultSetAdapter {

  private final String targetParentTableName;

  public CrossReferenceKeysDatabricksResultSetAdapter(String targetParentTableName) {
    this.targetParentTableName = targetParentTableName;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns true if the row's parent table name matches the expected parent table name. This is
   * required because the SQL command returns all foreign keys.
   */
  @Override
  public boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException {
    // check if the row's parent table name matches the expected parent table name
    final ResultColumn parentTableNameColumn = mapColumn(PKTABLE_NAME);
    if (!resultSet
        .getString(parentTableNameColumn.getResultSetColumnName())
        .equals(targetParentTableName)) {
      return false;
    }

    return super.includeRow(resultSet, columns);
  }
}
