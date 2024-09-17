package com.databricks.jdbc.dbclient.impl;

import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.fake.EmptyResultSet;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;

public class DatabricksEmptyMetadataClient implements IDatabricksMetadataClient {

  public static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksEmptyMetadataClient.class);

  @Override
  public ResultSet listTypeInfo(IDatabricksSession session) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listTypeInfo.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listCatalogs(IDatabricksSession session) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listCatalogs.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listSchemas(IDatabricksSession session, String catalog, String schemaNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listSchemas.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String[] tableTypes)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listTables.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listTableTypes(IDatabricksSession session) {
    LOGGER.warn("Empty metadata implementation for listTableTypes.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listColumns.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern)
      throws SQLException {
    LOGGER.warn("Empty metadata implementation for listFunctions.");
    return new EmptyResultSet();
  }

  @Override
  public ResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) throws SQLException {
    LOGGER.warn("Empty metadata implementation for listPrimaryKeys.");
    return new EmptyResultSet();
  }
}
