package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*TODO : add all debug logs and implementations*/

public class DatabricksThriftClient implements DatabricksClient, DatabricksMetadataClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftClient.class);

  private final ThriftAccessor thriftAccessor;

  public DatabricksThriftClient(IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = new ThriftAccessor(connectionContext);
  }

  private TNamespace getNamespace(String catalog, String schema) {
    return new TNamespace().setCatalogName(catalog).setSchemaName(schema);
  }

  @Override
  public ImmutableSessionInfo createSession(
      ComputeResource cluster, String catalog, String schema, Map<String, String> sessionConf)
      throws DatabricksSQLException {
    LOGGER.debug(
        "public Session createSession(Compute cluster = {}, String catalog = {}, String schema = {}, Map<String, String> sessionConf = {})",
        cluster.toString(),
        catalog,
        schema,
        sessionConf);
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(catalog, schema))
            .setConfiguration(sessionConf)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10);
    TOpenSessionResp response =
        (TOpenSessionResp)
            thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION);
    String sessionId = byteBufferToId(response.sessionHandle.getSessionId().guid);
    LOGGER.info("Session created with ID {}", sessionId);
    return ImmutableSessionInfo.builder().sessionId(sessionId).computeResource(cluster).build();
  }

  private String byteBufferToId(ByteBuffer buffer) {
    long sigBits = buffer.getLong();
    return new UUID(sigBits, sigBits).toString();
  }

  @Override
  public void deleteSession(String sessionId, ComputeResource cluster) {
    LOGGER.debug(
        "public void deleteSession(Compute cluster = {}, String sessionId = {})",
        cluster.toString(),
        sessionId);
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
    LOGGER.debug(
        "public DatabricksResultSet executeStatement(String sql = {}, Compute cluster = {}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {}, IDatabricksSession session)",
        sql,
        computeResource.toString(),
        statementType);
    throw new UnsupportedOperationException();
  }

  @Override
  public void closeStatement(String statementId) {
    LOGGER.debug(
        "public void closeStatement(String statementId = {}) for all purpose cluster", statementId);
    throw new UnsupportedOperationException();
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    LOGGER.debug(
        "public Optional<ExternalLink> getResultChunk(String statementId = {}, long chunkIndex = {}) for all purpose cluster",
        statementId,
        chunkIndex);
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
