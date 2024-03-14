package com.databricks.jdbc.client.impl.thrift;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.*;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.impl.thrift.commons.ThriftAccessor;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.client.sqlexec.ResultData;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

import org.apache.arrow.flatbuf.Int;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*TODO : add all debug logs and implementations*/

public class DatabricksThriftClient implements DatabricksClient {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftClient.class);

  private final ThriftAccessor thriftAccessor;

  public DatabricksThriftClient(IDatabricksConnectionContext connectionContext) {
    this.thriftAccessor = new ThriftAccessor(connectionContext);
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
            .setClient_protocol(PROTOCOL);
    TOpenSessionResp response =
        (TOpenSessionResp)
            thriftAccessor.getThriftResponse(openSessionReq, CommandName.OPEN_SESSION);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
    String sessionId = byteBufferToString(response.sessionHandle.sessionId.guid);
    return ImmutableSessionInfo.builder()
        .sessionId(sessionId)
        .sessionHandle(response.sessionHandle)
        .computeResource(cluster)
        .build();
  }

  @Override
  public void deleteSession(IDatabricksSession session, ComputeResource cluster)
      throws DatabricksSQLException {
    LOGGER.debug(
        "public void deleteSession(Session session = {}, Compute cluster = {})",
        session.toString(),
        cluster.toString());
    TCloseSessionReq closeSessionReq =
        new TCloseSessionReq().setSessionHandle(session.getSessionHandle());
    TCloseSessionResp response =
        (TCloseSessionResp)
            thriftAccessor.getThriftResponse(closeSessionReq, CommandName.CLOSE_SESSION);
    verifySuccessStatus(response.status.getStatusCode(), response.toString());
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
            "public DatabricksResultSet executeStatement(String sql = {}, compute resource = {}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {}, IDatabricksSession session)",
            sql,
            computeResource.toString(),
            statementType);
    TSparkGetDirectResults directResults = new TSparkGetDirectResults().setMaxRows(1000).setMaxBytes(100000);
    TExecuteStatementReq request = new TExecuteStatementReq()
    .setStatement(sql).setSessionHandle(session.getSessionHandle())
    .setGetDirectResults(directResults)
            .setPersistResultManifest(true)
            .setResultByteLimit(1000000);
    TExecuteStatementResp response = (TExecuteStatementResp) thriftAccessor.getThriftResponse(request,CommandName.EXECUTE_STATEMENT);
    TFetchResultsResp fetchResultsResp = thriftAccessor.getResultSetResp(response.getOperationHandle());
    TGetResultSetMetadataResp metadata = thriftAccessor.getMetadata(response.getOperationHandle());
    return new DatabricksResultSet(
            response.getStatus(),
            session.getSessionId(),
            fetchResultsResp.getResults() ,
            metadata,
            statementType,
            session,
            parentStatement);
  }

  @Override
  public void closeStatement(String statementId) {
    LOGGER.debug(
        "public void closeStatement(String statementId = {}) for all purpose cluster", statementId);
    // Does not require to perform anything in Thrift
  }

  private ExternalLink toExternalLink(TSparkArrowResultLink resultLink, long index){
    return new ExternalLink().setExternalLink(resultLink.getFileLink())
            .setChunkIndex(index)
            .setRowCount(resultLink.getRowCount())
            .setRowOffset(resultLink.getStartRowOffset())
            .setExpiration(String.valueOf(resultLink.getExpiryTime()))
            .setHttpHeaders(resultLink.getHttpHeaders());
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
