package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * Implementation of {@link IDatabricksResultSetAdapter} for processing the result set of the {@link
 * java.sql.DatabaseMetaData#getImportedKeys}.
 */
public class ImportedKeysDatabricksResultSetAdapter implements IDatabricksResultSetAdapter {

  public static final ResultColumn PARENT_CATALOG_NAME =
      new ResultColumn("PKTABLE_CAT", "parentCatalogName", Types.VARCHAR);
  public static final ResultColumn PARENT_NAMESPACE_NAME =
      new ResultColumn("PKTABLE_SCHEM", "parentNamespace", Types.VARCHAR);
  public static final ResultColumn PARENT_TABLE_NAME =
      new ResultColumn("PKTABLE_NAME", "parentTableName", Types.VARCHAR);
  private static final ResultColumn PARENT_COLUMN_NAME =
      new ResultColumn("PKCOLUMN_NAME", "parentColName", Types.VARCHAR);
  public static final ResultColumn FOREIGN_KEY_NAME_COLUMN =
      new ResultColumn("FK_NAME", "constraintName", Types.VARCHAR);

  /**
   * {@inheritDoc}
   *
   * <p>The SQL command SHOW FOREIGN KEYS returns column names that differ from those defined in the
   * JDBC specification, so we need to map them accordingly.
   */
  @Override
  public ResultColumn mapColumn(ResultColumn column) {
    String columnName = column.getResultSetColumnName();

    if (columnName.equals(PKTABLE_CAT.getResultSetColumnName())) {
      return PARENT_CATALOG_NAME;
    } else if (columnName.equals(PKTABLE_SCHEM.getResultSetColumnName())) {
      return PARENT_NAMESPACE_NAME;
    } else if (columnName.equals(PKTABLE_NAME.getResultSetColumnName())) {
      return PARENT_TABLE_NAME;
    } else if (columnName.equals(PKCOLUMN_NAME.getResultSetColumnName())) {
      return PARENT_COLUMN_NAME;
    } else if (columnName.equals(FKTABLE_CAT.getResultSetColumnName())) {
      return CATALOG_COLUMN;
    } else if (columnName.equals(FKTABLE_SCHEM.getResultSetColumnName())) {
      return SCHEMA_COLUMN;
    } else if (columnName.equals(FKTABLE_NAME.getResultSetColumnName())) {
      return TABLE_NAME_COLUMN;
    } else if (columnName.equals(FKCOLUMN_NAME.getResultSetColumnName())) {
      return COL_NAME_COLUMN;
    } else if (columnName.equals(FK_NAME.getResultSetColumnName())) {
      return FOREIGN_KEY_NAME_COLUMN;
    } else {
      return column; // No mapping, return the column as-is
    }
  }

  /** {@inheritDoc} */
  @Override
  public boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException {
    return true; // Include all rows
  }
}
