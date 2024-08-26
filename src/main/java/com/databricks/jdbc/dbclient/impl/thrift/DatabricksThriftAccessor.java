package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;
import static com.databricks.jdbc.dbclient.impl.thrift.generated.TStatusCode.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.common.CommandName;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.dbclient.impl.common.ClientUtils;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.thrift.generated.*;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import org.apache.http.HttpException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

public class DatabricksThriftAccessor {
  private final DatabricksConfig databricksConfig;
  private final ThreadLocal<TCLIService.Client> thriftClient;
  private final Boolean enableDirectResults;
  private static final TSparkGetDirectResults DEFAULT_DIRECT_RESULTS =
      new TSparkGetDirectResults().setMaxRows(DEFAULT_ROW_LIMIT).setMaxBytes(DEFAULT_BYTE_LIMIT);

  public DatabricksThriftAccessor(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    enableDirectResults = connectionContext.getDirectResultMode();
    this.databricksConfig = ClientUtils.generateDatabricksConfig(connectionContext);
    OAuthAuthenticator authenticator = new OAuthAuthenticator(connectionContext);
    authenticator.setupDatabricksConfig(databricksConfig);
    this.databricksConfig.resolve();
    Map<String, String> authHeaders = databricksConfig.authenticate();
    String endPointUrl = connectionContext.getEndpointURL();
    // Create a new thrift client for each thread as client state is not thread safe. Note that the
    // underlying protocol uses the same http client which is thread safe
    this.thriftClient =
        ThreadLocal.withInitial(
            () -> {
              DatabricksHttpTTransport transport =
                  new DatabricksHttpTTransport(
                      DatabricksHttpClient.getInstance(connectionContext), endPointUrl);
              transport.setCustomHeaders(authHeaders);
              TBinaryProtocol protocol = new TBinaryProtocol(transport);
              return new TCLIService.Client(protocol);
            });
  }

  @VisibleForTesting
  DatabricksThriftAccessor(
      TCLIService.Client client,
      DatabricksConfig config,
      IDatabricksConnectionContext connectionContext) {
    this.databricksConfig = config;
    this.thriftClient = ThreadLocal.withInitial(() -> client);
    this.enableDirectResults = connectionContext.getDirectResultMode();
  }

  public TBase getThriftResponse(
      TBase request, CommandName commandName, IDatabricksStatement parentStatement)
      throws DatabricksSQLException {
    // TODO: Test out metadata operations.
    // TODO: Handle compression.
    refreshHeadersIfRequired();
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "Fetching thrift response for request {%s}, CommandName {%s}",
            request.toString(), commandName.name()));
    try {
      switch (commandName) {
        case OPEN_SESSION:
          return getThriftClient().OpenSession((TOpenSessionReq) request);
        case CLOSE_SESSION:
          return getThriftClient().CloseSession((TCloseSessionReq) request);
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
          LoggingUtil.log(LogLevel.ERROR, errorMessage);
          throw new DatabricksSQLFeatureNotSupportedException(errorMessage);
      }
    } catch (TException | SQLException e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof HttpException) {
          throw new DatabricksHttpException(cause.getMessage(), cause);
        }
        cause = cause.getCause();
      }
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksSQLException(errorMessage, e);
    } finally {
      // Ensure resources are closed after use
      transport.close();
    }
  }

  public TFetchResultsResp getResultSetResp(TOperationHandle operationHandle, String context)
      throws DatabricksHttpException {
    refreshHeadersIfRequired();
    return getResultSetResp(SUCCESS_STATUS, operationHandle, context, DEFAULT_ROW_LIMIT, false);
  }

  private TFetchResultsResp getResultSetResp(
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
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      response = getThriftClient().FetchResults(request);
      if (fetchMetadata) {
        response.setResultSetMetadata(getResultSetMetadata(operationHandle));
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while fetching results from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    } finally {
      transport.close();
    }
    verifySuccessStatus(
        response.getStatus().getStatusCode(),
        String.format(
            "Error while fetching results Request {%s}. TFetchResultsResp {%s}. ",
            request, response));
    return response;
  }

  void longPolling(TOperationHandle operationHandle)
      throws TException, InterruptedException, DatabricksHttpException {
    TGetOperationStatusReq request =
        new TGetOperationStatusReq()
            .setOperationHandle(operationHandle)
            .setGetProgressUpdate(false);
    TGetOperationStatusResp response;
    TStatusCode statusCode;
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      do {
        response = getThriftClient().GetOperationStatus(request);
        statusCode = response.getStatus().getStatusCode();
        if (statusCode == TStatusCode.STILL_EXECUTING_STATUS) {
          Thread.sleep(DEFAULT_SLEEP_DELAY);
        }
      } while (statusCode == TStatusCode.STILL_EXECUTING_STATUS);
      verifySuccessStatus(
          statusCode, String.format("Request {%s}, Response {%s}", request, response));
    } finally {
      transport.close();
    }
  }

  public DatabricksResultSet execute(
      TExecuteStatementReq request,
      IDatabricksStatement parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {
    refreshHeadersIfRequired();
    int maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();
    if (enableDirectResults) {
      TSparkGetDirectResults directResults =
          new TSparkGetDirectResults().setMaxBytes(DEFAULT_BYTE_LIMIT).setMaxRows(maxRows);
      request.setGetDirectResults(directResults);
    }
    TExecuteStatementResp response = null;
    TFetchResultsResp resultSet = null;
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      response = getThriftClient().ExecuteStatement(request);
      if (Arrays.asList(ERROR_STATUS, INVALID_HANDLE_STATUS).contains(response.status.statusCode)) {
        throw new DatabricksSQLException(response.status.errorMessage);
      }
      if (response.isSetDirectResults()) {
        if (enableDirectResults) {
          if (response.getDirectResults().isSetOperationStatus()
              && response.getDirectResults().operationStatus.operationState
                  == TOperationState.ERROR_STATE) {
            throw new DatabricksSQLException(
                response.getDirectResults().getOperationStatus().errorMessage);
          }
        }
        if (((response.status.statusCode == SUCCESS_STATUS)
            || (response.status.statusCode == SUCCESS_WITH_INFO_STATUS))) {
          checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
          resultSet = response.getDirectResults().getResultSet();
          resultSet.setResultSetMetadata(response.getDirectResults().getResultSetMetadata());
        }
      } else {
        longPolling(response.getOperationHandle());
        resultSet =
            getResultSetResp(
                response.getStatus().getStatusCode(),
                response.getOperationHandle(),
                response.toString(),
                maxRows,
                true);
      }
    } catch (TException | InterruptedException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.toString());
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    } finally {
      transport.close();
    }
    return new DatabricksResultSet(
        response.getStatus(),
        getStatementId(response.getOperationHandle()),
        resultSet.getResults(),
        resultSet.getResultSetMetadata(),
        statementType,
        parentStatement,
        session);
  }

  private TFetchResultsResp listFunctions(TGetFunctionsReq request)
      throws DatabricksHttpException, TException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetFunctionsResp response = getThriftClient().GetFunctions(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp listPrimaryKeys(TGetPrimaryKeysReq request)
      throws DatabricksHttpException, TException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetPrimaryKeysResp response = getThriftClient().GetPrimaryKeys(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp getTables(TGetTablesReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTablesResp response = getThriftClient().GetTables(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp getTableTypes(TGetTableTypesReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTableTypesResp response = getThriftClient().GetTableTypes(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp getCatalogs(TGetCatalogsReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCatalogsResp response = getThriftClient().GetCatalogs(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp listSchemas(TGetSchemasReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetSchemasResp response = getThriftClient().GetSchemas(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp getTypeInfo(TGetTypeInfoReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTypeInfoResp response = getThriftClient().GetTypeInfo(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          true);
    } finally {
      transport.close();
    }
  }

  private TFetchResultsResp listColumns(TGetColumnsReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetColumnsResp response = getThriftClient().GetColumns(request);
    DatabricksHttpTTransport transport =
        (DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport();
    try {
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        return response.getDirectResults().getResultSet();
      }
      return getResultSetResp(
          response.getStatus().getStatusCode(),
          response.getOperationHandle(),
          response.toString(),
          DEFAULT_ROW_LIMIT,
          false);
    } finally {
      transport.close();
    }
  }

  private TGetResultSetMetadataResp getResultSetMetadata(TOperationHandle operationHandle)
      throws TException {
    TGetResultSetMetadataReq resultSetMetadataReq =
        new TGetResultSetMetadataReq().setOperationHandle(operationHandle);
    return getThriftClient().GetResultSetMetadata(resultSetMetadataReq);
  }

  private void refreshHeadersIfRequired() {
    ((DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport())
        .setCustomHeaders(databricksConfig.authenticate());
  }

  private TCLIService.Client getThriftClient() {
    return thriftClient.get();
  }
}
