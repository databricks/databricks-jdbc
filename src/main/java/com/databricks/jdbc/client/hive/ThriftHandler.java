package com.databricks.jdbc.client.hive;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.hive.service.rpc.thrift.*;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.THttpClient;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftHandler implements TCLIService.Iface {
  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftHandler.class);
  private final TCLIService.Iface client;
  private final IDatabricksConnectionContext connectionContext;
  private final TTransport transport;

  public ThriftHandler(IDatabricksConnectionContext connectionContext) throws TTransportException {
    String url = connectionContext.getHostUrl() + '/' + connectionContext.getHttpPath();
    LOGGER.debug("Using the interactive cluster URL " + url);
    System.out.println("Using the interactive cluster URL " + url);
    this.transport = new THttpClient(url);
    this.transport.open();
    // TODO : figure out if we need more protocols other than binary
    this.client = new TCLIService.Client(new TBinaryProtocol(transport));

    System.out.println("Client is " + client.toString());
    this.connectionContext = connectionContext;
  }

  @Override
  public TOpenSessionResp OpenSession(TOpenSessionReq tOpenSessionReq) {
    // TODO : add error handling
    System.out.println("open session request " + tOpenSessionReq.toString());
    try {
      return client.OpenSession(tOpenSessionReq);
    } catch (TException e) {
      // System.out.println(":((((((error
      // stacktrace"+e.getLocalizedMessage()+e.getMessage()+e.toString()+e.getCause());
      e.printStackTrace();
      LOGGER.error("Error occurred while opening session", e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public TCloseSessionResp CloseSession(TCloseSessionReq tCloseSessionReq) {
    try {
      return client.CloseSession(tCloseSessionReq);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public TGetInfoResp GetInfo(TGetInfoReq tGetInfoReq) throws TException {
    return null;
  }

  @Override
  public TExecuteStatementResp ExecuteStatement(TExecuteStatementReq tExecuteStatementReq)
      throws TException {
    return null;
  }

  @Override
  public TGetTypeInfoResp GetTypeInfo(TGetTypeInfoReq tGetTypeInfoReq) throws TException {
    return null;
  }

  @Override
  public TGetCatalogsResp GetCatalogs(TGetCatalogsReq tGetCatalogsReq) throws TException {
    return null;
  }

  @Override
  public TGetSchemasResp GetSchemas(TGetSchemasReq tGetSchemasReq) throws TException {
    return null;
  }

  @Override
  public TGetTablesResp GetTables(TGetTablesReq tGetTablesReq) throws TException {
    return null;
  }

  @Override
  public TGetTableTypesResp GetTableTypes(TGetTableTypesReq tGetTableTypesReq) throws TException {
    return null;
  }

  @Override
  public TGetColumnsResp GetColumns(TGetColumnsReq tGetColumnsReq) throws TException {
    return null;
  }

  @Override
  public TGetFunctionsResp GetFunctions(TGetFunctionsReq tGetFunctionsReq) throws TException {
    return null;
  }

  @Override
  public TGetPrimaryKeysResp GetPrimaryKeys(TGetPrimaryKeysReq tGetPrimaryKeysReq)
      throws TException {
    return null;
  }

  @Override
  public TGetCrossReferenceResp GetCrossReference(TGetCrossReferenceReq tGetCrossReferenceReq)
      throws TException {
    return null;
  }

  @Override
  public TGetOperationStatusResp GetOperationStatus(TGetOperationStatusReq tGetOperationStatusReq)
      throws TException {
    return null;
  }

  @Override
  public TCancelOperationResp CancelOperation(TCancelOperationReq tCancelOperationReq)
      throws TException {
    return null;
  }

  @Override
  public TCloseOperationResp CloseOperation(TCloseOperationReq tCloseOperationReq)
      throws TException {
    return null;
  }

  @Override
  public TGetResultSetMetadataResp GetResultSetMetadata(
      TGetResultSetMetadataReq tGetResultSetMetadataReq) throws TException {
    return null;
  }

  @Override
  public TFetchResultsResp FetchResults(TFetchResultsReq tFetchResultsReq) throws TException {
    return null;
  }

  @Override
  public TGetDelegationTokenResp GetDelegationToken(TGetDelegationTokenReq tGetDelegationTokenReq)
      throws TException {
    return null;
  }

  @Override
  public TCancelDelegationTokenResp CancelDelegationToken(
      TCancelDelegationTokenReq tCancelDelegationTokenReq) throws TException {
    return null;
  }

  @Override
  public TRenewDelegationTokenResp RenewDelegationToken(
      TRenewDelegationTokenReq tRenewDelegationTokenReq) throws TException {
    return null;
  }

  @Override
  public TGetQueryIdResp GetQueryId(TGetQueryIdReq tGetQueryIdReq) throws TException {
    return null;
  }

  @Override
  public TSetClientInfoResp SetClientInfo(TSetClientInfoReq tSetClientInfoReq) throws TException {
    return null;
  }
}
