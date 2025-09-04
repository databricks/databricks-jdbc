package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.MetadataResultConstants.*;

import com.databricks.jdbc.model.core.ResultColumn;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Implementation of {@link IDatabricksResultSetAdapter} for processing the result set of the {@link
 * java.sql.DatabaseMetaData#getCrossReference}.
 */
public class CrossReferenceKeysDatabricksResultSetAdapter
    extends ImportedKeysDatabricksResultSetAdapter {

  private final String targetParentCatalogName;
  private final String targetParentNamespaceName;
  private final String targetParentTableName;

  public CrossReferenceKeysDatabricksResultSetAdapter(
      String targetParentCatalogName,
      String targetParentNamespaceName,
      String targetParentTableName) {
    this.targetParentCatalogName = targetParentCatalogName;
    this.targetParentNamespaceName = targetParentNamespaceName;
    this.targetParentTableName = targetParentTableName;
  }

  /**
   * {@inheritDoc}
   *
   * <p>Returns true if the row's parent catalog, schema, and table name matches the expected parent
   * values.
   */
  @Override
  public boolean includeRow(ResultSet resultSet, List<ResultColumn> columns) throws SQLException {
    // check if the row's parent catalog, schema, and table name matches the expected values
    final ResultColumn parentCatalogNameColumn = mapColumn(PKTABLE_CAT);
    final ResultColumn parentNamespaceColumn = mapColumn(PKTABLE_SCHEM);
    final ResultColumn parentTableNameColumn = mapColumn(PKTABLE_NAME);

    boolean isParentCatalogMatching =
        resultSet
            .getString(parentCatalogNameColumn.getResultSetColumnName())
            .equals(targetParentCatalogName);
    boolean isParentNamespaceMatching =
        resultSet
            .getString(parentNamespaceColumn.getResultSetColumnName())
            .equals(targetParentNamespaceName);
    boolean isParentTableMatching =
        resultSet
            .getString(parentTableNameColumn.getResultSetColumnName())
            .equals(targetParentTableName);

    if (!isParentTableMatching || !isParentCatalogMatching || !isParentNamespaceMatching) {
      return false;
    }

    return super.includeRow(resultSet, columns);
  }
}
