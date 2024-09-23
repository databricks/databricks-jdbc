package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.MetadataResultConstants.DEFAULT_TABLE_TYPES;
import static com.databricks.jdbc.dbclient.impl.sqlexec.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.common.MetadataResultSetBuilder;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
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
    return MetadataResultSetBuilder.getCatalogsResult(
        getResultSet(SQL, session, StatementType.METADATA));
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LOGGER.debug(String.format("SQL command to fetch schemas: {%s}", SQL));
    return MetadataResultSetBuilder.getSchemasResult(
        getResultSet(SQL, session, StatementType.METADATA), catalog);
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    tableTypes =
        Optional.ofNullable(tableTypes)
            .filter(types -> types.length > 0)
            .orElse(DEFAULT_TABLE_TYPES);
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_TABLES);
    return MetadataResultSetBuilder.getTablesResult(
        getResultSet(SQL, session, StatementType.METADATA), tableTypes);
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
    return MetadataResultSetBuilder.getColumnsResult(
        getResultSet(SQL, session, StatementType.QUERY));
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
    return MetadataResultSetBuilder.getFunctionsResult(
        getResultSet(SQL, session, StatementType.QUERY), catalog);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LOGGER.debug(String.format("SQL command to fetch primary keys: {%s}", SQL));
    return MetadataResultSetBuilder.getPrimaryKeysResult(
        getResultSet(SQL, session, StatementType.METADATA));
  }

  private ResultSet getResultSet(
      String SQL, IDatabricksSession session, StatementType statementType) throws SQLException {
    return sdkClient.executeStatement(
        SQL,
        session.getComputeResource(),
        new HashMap<>(),
        statementType,
        session,
        null /* parentStatement */);
  }
}
