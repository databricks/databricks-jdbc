package com.databricks.jdbc.client.impl.sdk;

import static com.databricks.jdbc.client.impl.helper.MetadataResultConstants.DEFAULT_TABLE_TYPES;
import static com.databricks.jdbc.client.impl.sdk.ResultConstants.TYPE_INFO_RESULT;

import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.helper.CommandBuilder;
import com.databricks.jdbc.client.impl.helper.CommandName;
import com.databricks.jdbc.client.impl.helper.MetadataResultSetBuilder;
import com.databricks.jdbc.commons.CommandLatencyMetrics;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.jdbc.telemetry.annotation.DatabricksMetricsTimedClass;
import com.databricks.jdbc.telemetry.annotation.DatabricksMetricsTimedMethod;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Optional;

/**
 * This is for the new SQL commands added in runtime. Note that the DatabricksMetadataSdkClient will
 * be replaced by this class once runtime code is merged and this class is tested end to end.
 * https://docs.google.com/document/d/1E28o7jyPIp6_byZHGD5Eyc4uwGVSydX5o9PaiSY1V4s/edit#heading=h.681k0yimshae
 * Tracking bug for replacement: (PECO-1502)
 */
@DatabricksMetricsTimedClass(
    methods = {
      @DatabricksMetricsTimedMethod(
          methodName = "listCatalogs",
          metricName = CommandLatencyMetrics.LIST_CATALOGS_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listSchemas",
          metricName = CommandLatencyMetrics.LIST_SCHEMAS_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listTables",
          metricName = CommandLatencyMetrics.LIST_TABLES_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listTableTypes",
          metricName = CommandLatencyMetrics.LIST_TABLE_TYPES_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listColumns",
          metricName = CommandLatencyMetrics.LIST_COLUMNS_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listFunctions",
          metricName = CommandLatencyMetrics.LIST_FUNCTIONS_NEW_METADATA_SEA),
      @DatabricksMetricsTimedMethod(
          methodName = "listPrimaryKeys",
          metricName = CommandLatencyMetrics.LIST_PRIMARY_KEYS_NEW_METADATA_SEA)
    })
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
    CommandBuilder commandBuilder = new CommandBuilder(session);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_CATALOGS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch catalogs: {%s}", SQL));
    return MetadataResultSetBuilder.getCatalogsResult(
        getResultSet(SQL, session, StatementType.METADATA));
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) throws SQLException {
    System.out.println("public ResultSet getSchemas()");
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchemaPattern(schemaNamePattern);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_SCHEMAS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch schemas: {%s}", SQL));
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
    LoggingUtil.log(LogLevel.DEBUG, "Returning list of table types.");
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
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch functions: {%s}", SQL));
    return MetadataResultSetBuilder.getFunctionsResult(
        getResultSet(SQL, session, StatementType.QUERY), catalog);
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    CommandBuilder commandBuilder =
        new CommandBuilder(catalog, session).setSchema(schema).setTable(table);
    String SQL = commandBuilder.getSQLString(CommandName.LIST_PRIMARY_KEYS);
    LoggingUtil.log(LogLevel.DEBUG, String.format("SQL command to fetch primary keys: {%s}", SQL));
    return MetadataResultSetBuilder.getPrimaryKeysResult(
        getResultSet(SQL, session, StatementType.METADATA));
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
