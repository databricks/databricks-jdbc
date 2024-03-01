package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftClient implements DatabricksClient, DatabricksMetadataClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftClient.class);
  private final IDatabricksConnectionContext connectionContext;

  @Override
  public ImmutableSessionInfo createSession(
      ComputeResource cluster, String catalog, String schema, Map<String, String> sessionConf) {
    throw new UnsupportedOperationException();
  }

  public DatabricksThriftClient(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  @Override
  public void deleteSession(String sessionId, ComputeResource cluster) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      ComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException {
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeStatement(String statementId) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTypeInfo(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listCatalogs(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listSchemas(
      IDatabricksSession session, String catalog, String schemaNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTables(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listTableTypes(IDatabricksSession session) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listColumns(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String tableNamePattern,
      String columnNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listFunctions(
      IDatabricksSession session,
      String catalog,
      String schemaNamePattern,
      String functionNamePattern) {
    throw new UnsupportedOperationException();
  }

  @Override
  public DatabricksResultSet listPrimaryKeys(
      IDatabricksSession session, String catalog, String schema, String table) {
    throw new UnsupportedOperationException();
  }
}
