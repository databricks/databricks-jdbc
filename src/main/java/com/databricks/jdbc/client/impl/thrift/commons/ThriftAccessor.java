package com.databricks.jdbc.client.impl.thrift.commons;

import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;
import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThriftAccessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ThriftAccessor.class);
  private DatabricksHttpTTransport transport;
  private final DatabricksConfig databricksConfig;

  public ThriftAccessor(IDatabricksConnectionContext connectionContext) {
    this.transport =
        new DatabricksHttpTTransport(
            DatabricksHttpClient.getInstance(connectionContext), getEndpointURL(connectionContext));
    // TODO : add other auth in followup PRs
    this.databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken());
  }
  private String getEndpointURL(IDatabricksConnectionContext connectionContext) {
    return String.format("%s/%s", connectionContext.getHostUrl(), connectionContext.getHttpPath());
  }

public void getResultSetResp(TOperationHandle operationHandle)  throws DatabricksSQLException{
  Map<String, String> authHeaders = databricksConfig.authenticate();
  transport.setCustomHeaders(authHeaders);
  TBinaryProtocol protocol = new TBinaryProtocol(transport);
  TCLIService.Client client = new TCLIService.Client(protocol);
  TFetchResultsReq request = new TFetchResultsReq().setOperationHandle(operationHandle).setIncludeResultSetMetadata(true).setFetchType((short) 0).setMaxRows(1000).setMaxBytes(1000000);
  try{
    TFetchResultsResp tFetchResultsResp = client.FetchResults(request);
    System.out.println("Here is fetch response : "+ tFetchResultsResp.toString());
  }
  catch (TException e){
    String errorMessage =
            String.format(
                    "Error while fetching results from Thrift server. Request {%s}, Error {%s}",
                    request.toString(), e.toString());
    LOGGER.error(errorMessage);
    throw new DatabricksSQLException(errorMessage, e);
  }
}
  public TBase getThriftResponse(TBase request, CommandName commandName)
      throws DatabricksSQLException {
    LOGGER.debug(
        "Fetching thrift response for request {}, CommandName {}",
        request.toString(),
        commandName.name());
    // TODO : find how we can create a client once rather than creating one for every execution
    Map<String, String> authHeaders = databricksConfig.authenticate();
    transport.setCustomHeaders(authHeaders);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    TCLIService.Client client = new TCLIService.Client(protocol);
    try {
      switch (commandName) {
        case OPEN_SESSION:
           return client.OpenSession((TOpenSessionReq) request);
        case CLOSE_SESSION:
          return client.CloseSession((TCloseSessionReq) request);
        case LIST_TABLE_TYPES:
          return client.GetTableTypes((TGetTableTypesReq) request);
        case LIST_PRIMARY_KEYS:
          return client.GetPrimaryKeys((TGetPrimaryKeysReq) request);
        case LIST_FUNCTIONS:
          return client.GetFunctions((TGetFunctionsReq) request);
        case LIST_SCHEMAS:
          return client.GetSchemas((TGetSchemasReq) request);
        case LIST_COLUMNS:
          return client.GetColumns((TGetColumnsReq) request);
        case LIST_CATALOGS:
          return client.GetCatalogs((TGetCatalogsReq) request);
        case LIST_TABLES:
          return client.GetTables((TGetTablesReq) request);
        case LIST_TYPE_INFO:
          return client.GetTypeInfo((TGetTypeInfoReq) request);
        case EXECUTE_STATEMENT:
          return client.ExecuteStatement((TExecuteStatementReq) request);
        default:
          String errorMessage =
              String.format(
                  "No implementation for fetching thrift response for CommandName {%s}.  Request {%s}",
                  commandName, request.toString());
          LOGGER.error(errorMessage);
          throw new DatabricksSQLFeatureNotSupportedException(errorMessage);
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    }
  }
}
