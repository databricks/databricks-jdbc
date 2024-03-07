package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.commons.ThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*TODO : add all debug logs and implementations*/

public class DatabricksThriftClient implements DatabricksClient {

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
    System.out.println("opening thrift session");
    TOpenSessionReq openSessionReq =
        new TOpenSessionReq()
            .setInitialNamespace(getNamespace(catalog, schema))
            .setConfiguration(sessionConf)
            .setCanUseMultipleCatalogs(true)
            .setClient_protocol(PROTOCOL);
    TOpenSessionResp response =
        (TOpenSessionResp)
            thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION);
    String sessionId = byteBufferToString(response.sessionHandle.getSessionId().guid);
    String secret = byteBufferToString(response.sessionHandle.getSessionId().secret);
    LOGGER.info("Session created with ID {}", sessionId);

    System.out.println("opened thrift session " + sessionId);
    return ImmutableSessionInfo.builder().sessionId(sessionId).computeResource(cluster).build();
  }

  @Override
  public void deleteSession(DatabricksSession session, ComputeResource cluster)
      throws DatabricksSQLException {
    LOGGER.debug(
        "public void deleteSession(Session session = {}, Compute cluster = {})",
        session.toString(),
        cluster.toString());
    TCloseSessionReq closeSessionReq =
        new TCloseSessionReq().setSessionHandle(getSessionHandle(session));
    TCloseSessionResp response =
        (TCloseSessionResp)
            thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION);
    System.out.println("deleted thrift session " + response.toString());
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
    // Does not require to perform anything in Thrift
  }

  @Override
  public Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex) {
    LOGGER.debug(
        "public Optional<ExternalLink> getResultChunk(String statementId = {}, long chunkIndex = {}) for all purpose cluster",
        statementId,
        chunkIndex);
    throw new UnsupportedOperationException();
  }
}
