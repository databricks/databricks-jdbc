package com.databricks.jdbc.client.impl.thrift.commons;

import static com.databricks.jdbc.client.impl.thrift.commons.DatabricksThriftHelper.verifySuccessStatus;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_BYTE_LIMIT;
import static com.databricks.jdbc.commons.EnvironmentVariables.DEFAULT_ROW_LIMIT;

import com.databricks.jdbc.client.DatabricksHttpException;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.client.impl.thrift.generated.*;
import com.databricks.jdbc.commons.CommandName;
import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.core.IDatabricksStatement;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.Map;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksThriftAccessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksThriftAccessor.class);
  private final DatabricksConfig databricksConfig;
  private final TCLIService.Client thriftClient;
  private static final TSparkGetDirectResults DEFAULT_DIRECT_RESULTS =
      new TSparkGetDirectResults().setMaxRows(DEFAULT_ROW_LIMIT).setMaxBytes(DEFAULT_BYTE_LIMIT);

  public DatabricksThriftAccessor(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            DatabricksHttpClient.getInstance(connectionContext),
            connectionContext.getEndpointURL());
    // TODO : add other auth in followup PRs
    this.databricksConfig =
        new DatabricksConfig()
            .setHost(connectionContext.getHostUrl())
            .setToken(connectionContext.getToken());
    Map<String, String> authHeaders = databricksConfig.authenticate();
    transport.setCustomHeaders(authHeaders);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);
    this.thriftClient = new TCLIService.Client(protocol);
  }

  @VisibleForTesting
  public DatabricksThriftAccessor(TCLIService.Client client) {
    this.databricksConfig = null;
    this.thriftClient = client;
  }

  public TBase getThriftResponse(
      TBase request, CommandName commandName, IDatabricksStatement parentStatement)
      throws DatabricksSQLException {
    /*Todo list :
     *  1. Poll until we get a success status
     *  2. Test out metadata operations.
     *  3. Add token refresh
     *  4. Handle cloud-fetch
     *  5. Handle compression
     * */
    LOGGER.debug(
        "Fetching thrift response for request {}, CommandName {}",
        request.toString(),
        commandName.name());
    try {
      switch (commandName) {
        case OPEN_SESSION:
          return thriftClient.OpenSession((TOpenSessionReq) request);
        case CLOSE_SESSION:
          return thriftClient.CloseSession((TCloseSessionReq) request);
        case EXECUTE_STATEMENT:
          return execute((TExecuteStatementReq) request, parentStatement);
        case LIST_PRIMARY_KEYS:
          return listPrimaryKeys((TGetPrimaryKeysReq) request);
        case LIST_FUNCTIONS:
          return listFunctions((TGetFunctionsReq) request);
        case LIST_SCHEMAS:
          return listSchemas((TGetSchemasReq) request);
        case LIST_COLUMNS:
          return listColumns((TGetColumnsReq) request);
        case LIST_CATALOGS:
          return getCatalogs((TGetCatalogsReq) request);
        case LIST_TABLES:
          return getTables((TGetTablesReq) request);
        case LIST_TABLE_TYPES:
          return getTableTypes((TGetTableTypesReq) request);
        case LIST_TYPE_INFO:
          return getTypeInfo((TGetTypeInfoReq) request);
        default:
          String errorMessage =
              String.format(
                  "No implementation for fetching thrift response for CommandName {%s}.  Request {%s}",
                  commandName, request.toString());
          LOGGER.error(errorMessage);
          throw new DatabricksSQLFeatureNotSupportedException(errorMessage);
      }
    } catch (TException | SQLException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    }
  }

  public TFetchResultsResp getResultSetResp(
      TStatusCode responseCode,
      TOperationHandle operationHandle,
      String context,
      int maxRows,
      boolean fetchMetadata)
      throws DatabricksHttpException {
    verifySuccessStatus(responseCode, context);
    TFetchResultsReq request =
        new TFetchResultsReq()
            .setOperationHandle(operationHandle)
            .setIncludeResultSetMetadata(true)
            .setFetchType((short) 0) // 0 represents Query output. 1 represents Log
            .setMaxRows(maxRows)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    TFetchResultsResp response = null;
    try {
      response = thriftClient.FetchResults(request);
      if (fetchMetadata) {
        response.setResultSetMetadata(getResultSetMetadata(operationHandle));
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while fetching results from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LOGGER.error(errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
    verifySuccessStatus(
        response.getStatus().getStatusCode(),
        String.format(
            "Error while fetching results. TFetchResultsResp {%s}. ", response.toString()));
    return response;
  }

  private TFetchResultsResp execute(
      TExecuteStatementReq request, IDatabricksStatement parentStatement)
      throws TException, SQLException {
    int maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();
    TSparkGetDirectResults directResults =
        new TSparkGetDirectResults().setMaxBytes(DEFAULT_BYTE_LIMIT).setMaxRows(maxRows);
    request.setGetDirectResults(directResults);
    request.setCanReadArrowResult(true);
    request.setCanDownloadResult(true);
    TExecuteStatementResp response = thriftClient.ExecuteStatement(request);
    if (response.isSetDirectResults()) {
      TFetchResultsResp resultSet = response.getDirectResults().getResultSet();
      resultSet.setResultSetMetadata(response.getDirectResults().getResultSetMetadata());
      return resultSet;
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        maxRows,
        true);
  }

  private TFetchResultsResp listFunctions(TGetFunctionsReq request)
      throws DatabricksHttpException, TException {
    TGetFunctionsResp response = thriftClient.GetFunctions(request);
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp listPrimaryKeys(TGetPrimaryKeysReq request)
      throws DatabricksHttpException, TException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetPrimaryKeysResp response = thriftClient.GetPrimaryKeys(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTables(TGetTablesReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTablesResp response = thriftClient.GetTables(request);
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTableTypes(TGetTableTypesReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTableTypesResp response = thriftClient.GetTableTypes(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getCatalogs(TGetCatalogsReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCatalogsResp response = thriftClient.GetCatalogs(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp listSchemas(TGetSchemasReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetSchemasResp response = thriftClient.GetSchemas(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTypeInfo(TGetTypeInfoReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTypeInfoResp response = thriftClient.GetTypeInfo(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        true);
  }

  private TFetchResultsResp listColumns(TGetColumnsReq request)
      throws TException, DatabricksHttpException {
    request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetColumnsResp response = thriftClient.GetColumns(request);
    if (response.isSetDirectResults()) {
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus().getStatusCode(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TGetResultSetMetadataResp getResultSetMetadata(TOperationHandle operationHandle)
      throws TException {
    TGetResultSetMetadataReq resultSetMetadataReq =
        new TGetResultSetMetadataReq().setOperationHandle(operationHandle);
    return thriftClient.GetResultSetMetadata(resultSetMetadataReq);
  }
}
