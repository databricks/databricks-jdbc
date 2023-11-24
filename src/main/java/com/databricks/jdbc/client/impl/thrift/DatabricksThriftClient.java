package com.databricks.jdbc.client.impl.thrift;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.client.hive.ThriftHandler;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.service.sql.ExternalLink;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;
import org.apache.hive.service.rpc.thrift.TOpenSessionReq;
import org.apache.hive.service.rpc.thrift.TOpenSessionResp;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftClient implements DatabricksClient, DatabricksMetadataClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSdkClient.class);
  private final IDatabricksConnectionContext connectionContext;
  private final ThriftHandler thriftHandler;

  public DatabricksThriftClient(IDatabricksConnectionContext connectionContext)
      throws TTransportException {
    this.connectionContext = connectionContext;
    this.thriftHandler = new ThriftHandler(connectionContext);
  }

  @Override
  public ImmutableSessionInfo createSession(String warehouseId) throws TException {
    TOpenSessionReq request = new TOpenSessionReq();

    TOpenSessionResp response = thriftHandler.OpenSession(request);
    return ImmutableSessionInfo.builder()
        .sessionId(response.getSessionHandle().getSessionId().toString())
        .warehouseId(warehouseId)
        .build();
  }

  @Override
  public void deleteSession(String sessionId, String warehouseId) {
    throw new UnsupportedOperationException();
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
}
