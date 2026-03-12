package com.databricks.jdbc.dbclient.impl.sqlexec;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DatabricksEmptyMetadataClient implements IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksEmptyMetadataClient.class);
  private final MetadataResultSetBuilder metadataResultSetBuilder;

  public DatabricksEmptyMetadataClient(IDatabricksConnectionContext ctx) {
    this.metadataResultSetBuilder = new MetadataResultSetBuilder(ctx);
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) throws SQLException {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return metadataResultSetBuilder.getTypeInfoResult();
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listCatalogs.");
    return metadataResultSetBuilder.getCatalogsResult((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listSchemas.");
    return metadataResultSetBuilder.getSchemasResult(null);
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
    return metadataResultSetBuilder.getTablesResult(catalog, tableTypes, new ArrayList<>());
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    LOGGER.debug("public ResultSet listTableTypes()");
    return metadataResultSetBuilder.getTableTypesResult();
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
    return metadataResultSetBuilder.getColumnsResult((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listFunctions.");
    return metadataResultSetBuilder.getFunctionsResult("", null);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listPrimaryKeys.");
    return metadataResultSetBuilder.getPrimaryKeysResult((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listImportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    return metadataResultSetBuilder.getImportedKeys((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listExportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    return metadataResultSetBuilder.getExportedKeys((List<List<Object>>) null);
  }

  @Override
  public DatabricksResultSet listCrossReferences(
      IDatabricksSession session,
      String parentCatalog,
      String parentSchema,
      String parentTable,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable)
      throws SQLException {
    return metadataResultSetBuilder.getCrossRefsResult((List<List<Object>>) null);
  }
}
