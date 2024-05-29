package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.client.impl.sdk.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.helper.CommandBuilder;
import com.databricks.jdbc.client.impl.helper.CommandName;
import com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.commons.MetricName;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.telemetry.Metrics;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is for the new SQL commands added in runtime. Note that the DatabricksMetadataSdkClient will
 * be replaced by this class once runtime code is merged and this class is tested end to end.
 * https://docs.google.com/document/d/1E28o7jyPIp6_byZHGD5Eyc4uwGVSydX5o9PaiSY1V4s/edit#heading=h.681k0yimshae
 * Tracking bug for replacement: (PECO-1502)
 */
public class DatabricksNewMetadataSdkClient implements DatabricksMetadataClient {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DatabricksNewMetadataSdkClient.class);
  private final DatabricksSdkClient sdkClient;

  public DatabricksNewMetadataSdkClient(DatabricksSdkClient sdkClient) {
    this.sdkClient = sdkClient;
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    LOGGER.debug("public ResultSet getTypeInfo()");
    return TYPE_INFO_RESULT;
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LOGGER.debug("SQL command to fetch catalogs: {}", SQL);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getCatalogsResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_CATALOGS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LOGGER.debug("SQL command to fetch schemas: {}", SQL);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getSchemasResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_SCHEMAS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
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
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session)
            .setSchemaPattern(schemaNamePattern)
            .setTablePattern(tableNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_TABLES);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getTablesResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_TABLES_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) throws SQLException {
    long startTime = System.currentTimeMillis();
    LOGGER.debug("Returning list of table types.");
    DatabricksResultSet resultSet = MetadataResultSetBuilder.getTableTypesResult();
    Metrics.SetGaugeMetric(
        MetricName.LIST_TABLE_TYPES_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
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
        MetadataResultSetBuilder.getColumnsResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_COLUMNS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
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
    LOGGER.debug("SQL command to fetch functions: {}", SQL);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getFunctionsResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_FUNCTIONS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    long startTime = System.currentTimeMillis();
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LOGGER.debug("SQL command to fetch primary keys: {}", SQL);
    DatabricksResultSet resultSet =
        MetadataResultSetBuilder.getPrimaryKeysResult(getResultSet(SQL, session));
    Metrics.SetGaugeMetric(
        MetricName.LIST_PRIMARY_KEYS_METADATA_SEA.name(), System.currentTimeMillis() - startTime);
    return resultSet;
  }

  private ResultSet getResultSet(String SQL, IDatabricksSession session) throws SQLException {
    return sdkClient.executeStatement(
        SQL,
        session.getComputeResource(),
        new HashMap<Integer, ImmutableSqlParameter>(),
        StatementType.METADATA,
        session,
        null /* parentStatement */);
  }
}
