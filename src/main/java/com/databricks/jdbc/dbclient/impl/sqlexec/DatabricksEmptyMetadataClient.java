package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.dbclient.impl.sqlexec.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabricksEmptyMetadataClient implements IDatabricksMetadataClient {

  public static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksEmptyMetadataClient.class);

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) throws SQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return TYPE_INFO_RESULT;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listCatalogs.");
    return MetadataResultSetBuilder.getCatalogsResult((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listSchemas.");
    return MetadataResultSetBuilder.getSchemasResult(null);
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listTables.");
    return MetadataResultSetBuilder.getTablesResult(catalog, new ArrayList<>());
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    LOGGER.debug("public ResultSet listTableTypes()");
    return MetadataResultSetBuilder.getTableTypesResult();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listColumns.");
    return MetadataResultSetBuilder.getColumnsResult((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listFunctions.");
    return MetadataResultSetBuilder.getFunctionsResult(null);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listPrimaryKeys.");
    return MetadataResultSetBuilder.getPrimaryKeysResult((List<List<Object>>) null);
  }
}
