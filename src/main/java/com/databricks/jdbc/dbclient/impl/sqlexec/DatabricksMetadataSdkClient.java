package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.MetadataResultConstants.DEFAULT_TABLE_TYPES;
import static com.databricks.jdbc.dbclient.impl.common.CommandConstants.METADATA_STATEMENT_ID;
import static com.databricks.jdbc.dbclient.impl.sqlexec.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.MetadataResultConstants;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Optional;

/** Implementation for {@link IDatabricksMetadataClient} using {@link IDatabricksClient}. */
public class DatabricksMetadataSdkClient implements IDatabricksMetadataClient {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksMetadataSdkClient.class);
  private final IDatabricksClient sdkClient;

  public DatabricksMetadataSdkClient(IDatabricksClient sdkClient) {
    this.sdkClient = sdkClient;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return TYPE_INFO_RESULT;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LOGGER.debug(String.format("SQL command to fetch catalogs: {%s}", SQL));
    return MetadataResultSetBuilder.getCatalogsResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LOGGER.debug(String.format("SQL command to fetch schemas: {%s}", SQL));
    return MetadataResultSetBuilder.getSchemasResult(getResultSet(SQL, session), catalog);
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    String[] validatedTableTypes =
        Optional.ofNullable(tableTypes)
            .filter(types -> types.length > 0)
            .orElse(DEFAULT_TABLE_TYPES);
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_TABLES);
    LOGGER.debug(String.format("SQL command to fetch tables: {%s}", SQL));
    return MetadataResultSetBuilder.getTablesResult(
        getResultSet(SQL, session), validatedTableTypes);
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) throws SQLException {
    LOGGER.debug("Returning list of table types.");
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
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern)
            .setColumnPattern(columnNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_COLUMNS);
    LOGGER.debug(String.format("SQL command to fetch columns: {%s}", SQL));
    return MetadataResultSetBuilder.getColumnsResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setFunctionPattern(functionNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FUNCTIONS);
    LOGGER.debug(String.format("SQL command to fetch functions: {%s}", SQL));
    return MetadataResultSetBuilder.getFunctionsResult(getResultSet(SQL, session), catalog);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LOGGER.debug(String.format("SQL command to fetch primary keys: {%s}", SQL));
    return MetadataResultSetBuilder.getPrimaryKeysResult(getResultSet(SQL, session));
  }

  @Override
  public DatabricksResultSet listImportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.debug("public ResultSet listImportedKeys() using SDK");
    return MetadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        MetadataResultConstants.IMPORTED_KEYS_COLUMNS,
        new ArrayList<>(),
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.GET_IMPORTED_KEYS);
  }

  @Override
  public DatabricksResultSet listExportedKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.debug("public ResultSet listExportedKeys() using SDK");
    return MetadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        MetadataResultConstants.EXPORTED_KEYS_COLUMNS,
        new ArrayList<>(),
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.GET_EXPORTED_KEYS);
  }

  @Override
  public DatabricksResultSet listCrossReferences(
      IDatabricksSession session,
      String catalog,
      String schema,
      String table,
      String foreignCatalog,
      String foreignSchema,
      String foreignTable) {
    LOGGER.debug("public ResultSet listCrossReferences() using SDK");
    return MetadataResultSetBuilder.getResultSetWithGivenRowsAndColumns(
        MetadataResultConstants.CROSS_REFERENCE_COLUMNS,
        new ArrayList<>(),
        METADATA_STATEMENT_ID,
        com.databricks.jdbc.common.CommandName.GET_CROSS_REFERENCE);
  }

  private ResultSet getResultSet(String SQL, IDatabricksSession session) throws SQLException {
    return sdkClient.executeStatement(
        SQL,
        session.getComputeResource(),
        new HashMap<>(),
        StatementType.METADATA,
        session,
        null /* parentStatement */);
  }
}
