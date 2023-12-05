package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.hive.ThriftHandler;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.ExternalLink;
import java.nio.ByteBuffer;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftClient implements DatabricksClient, DatabricksMetadataClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);
  private final IDatabricksConnectionContext connectionContext;
  private final ThriftHandler thriftHandler;

  public DatabricksThriftClient(IDatabricksConnectionContext connectionContext) {
    // Todo : error handling
    this.connectionContext = connectionContext;
    try {
      this.thriftHandler = new ThriftHandler(connectionContext);
    } catch (TTransportException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ImmutableSessionInfo createSession(String warehouseId) {
    LOGGER.debug("Thrift createSession(String warehouseId = {})", warehouseId);
    TOpenSessionReq request = new TOpenSessionReq();
    Map<String, String> thriftConfig = new HashMap<>();
    request.setUsername(connectionContext.getUsername());
    request.setPassword(connectionContext.getToken());
    request.setConfiguration(thriftConfig);
    TOpenSessionResp response = thriftHandler.OpenSession(request);
    String sessionId = getSessionId(response.getSessionHandle());
    return ImmutableSessionInfo.builder().sessionId(sessionId).warehouseId(warehouseId).build();
  }

  @Override
  public void deleteSession(String sessionId, String warehouseId) {
    TCloseSessionReq request = new TCloseSessionReq();
    thriftHandler.CloseSession(request);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      String warehouseId,
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

  private String getSessionId(TSessionHandle sessionHandle) {
    byte[] guid = sessionHandle.getSessionId().getGuid();
    ByteBuffer byteBuffer = ByteBuffer.wrap(guid, 0, 16);
    return new UUID(byteBuffer.getLong(), byteBuffer.getLong()).toString();
  }
}
