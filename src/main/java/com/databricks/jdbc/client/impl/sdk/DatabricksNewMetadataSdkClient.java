package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.DEFAULT_TABLE_TYPES;
import static com.databricks.jdbc.client.impl.sdk.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.helper.CommandBuilder;
import com.databricks.jdbc.client.impl.helper.CommandName;
import com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.MetricsList;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.HashMap;
import java.util.Optional;

/**
 * This is for the new SQL commands added in runtime. Note that the DatabricksMetadataSdkClient will
 * be replaced by this class once runtime code is merged and this class is tested end to end.
 * https://docs.google.com/document/d/1E28o7jyPIp6_byZHGD5Eyc4uwGVSydX5o9PaiSY1V4s/edit#heading=h.681k0yimshae
 * Tracking bug for replacement: (PECO-1502)
 */
public class DatabricksNewMetadataSdkClient implements DatabricksMetadataClient {
  private final DatabricksSdkClient sdkClient;

  public DatabricksNewMetadataSdkClient(DatabricksSdkClient sdkClient) {
    this.sdkClient = sdkClient;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getTypeInfo()");
    return TYPE_INFO_RESULT;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch catalogs: {%s}", SQL));
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getCatalogsResult(
            getResultSet(SQL, session, StatementType.METADATA));
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_CATALOGS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch schemas: {%s}", SQL));
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getSchemasResult(
            getResultSet(SQL, session, StatementType.METADATA), catalog);
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_SCHEMAS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
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
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_TABLES);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getTablesResult(
            getResultSet(SQL, session, StatementType.METADATA), tableTypes);
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_TABLES_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) throws SQLException {

    LoggingUtil.log(LogLevel.DEBUG, "Returning list of table types.");
    long startTime = System.currentTimeMillis();
    DatabricksResultSet resultSet = MetadataResultSetBuilder.getTableTypesResult();
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_TABLE_TYPES_METADATA_SEA.name(),
            System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern)
            .setColumnPattern(columnNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_COLUMNS);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getColumnsResult(getResultSet(SQL, session, StatementType.QUERY));
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_COLUMNS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setFunctionPattern(functionNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_FUNCTIONS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch functions: {%s}", SQL));
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getFunctionsResult(
            getResultSet(SQL, session, StatementType.QUERY), catalog);
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_FUNCTIONS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch primary keys: {%s}", SQL));
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getPrimaryKeysResult(
            getResultSet(SQL, session, StatementType.METADATA));
    IDatabricksConnectionContext connectionContext = session.getConnectionContext();
    connectionContext
        .getMetricsExporter()
        .record(
            MetricsList.LIST_PRIMARY_KEYS_METADATA_SEA.name(),
            System.currentTimeMillis() - startTime);
    return resultSet;
  }

  private ResultSet getResultSet(
      String SQL, IDatabricksSession session, StatementType statementType) throws SQLException {
    return sdkClient.executeStatement(
        SQL,
        session.getComputeResource(),
        new HashMap<Integer, ImmutableSqlParameter>(),
        statementType,
        session,
        null /* parentStatement */);
  }
}
