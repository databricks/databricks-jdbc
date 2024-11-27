package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Map;
import org.apache.http.HttpException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;

final class DatabricksThriftAccessor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksThriftAccessor.class);
  private final DatabricksConfig databricksConfig;
  private final ThreadLocal<TCLIService.Client> thriftClient;
  private final Boolean enableDirectResults;
  private static final TSparkGetDirectResults DEFAULT_DIRECT_RESULTS =
      new TSparkGetDirectResults().setMaxRows(DEFAULT_ROW_LIMIT).setMaxBytes(DEFAULT_BYTE_LIMIT);

  DatabricksThriftAccessor(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    enableDirectResults = connectionContext.getDirectResultMode();
    this.databricksConfig = new ClientConfigurator(connectionContext).getDatabricksConfig();
    Map<String, String> authHeaders = databricksConfig.authenticate();
    String endPointUrl = connectionContext.getEndpointURL();

    if (!DriverUtil.isRunningAgainstFake()) {
      // Create a new thrift client for each thread as client state is not thread safe. Note that
      // the underlying protocol uses the same http client which is thread safe
      this.thriftClient =
          ThreadLocal.withInitial(
              () -> createThriftClient(endPointUrl, authHeaders, connectionContext));
    } else {
      TCLIService.Client client = createThriftClient(endPointUrl, authHeaders, connectionContext);
      this.thriftClient = ThreadLocal.withInitial(() -> client);
    }
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

  TBase getThriftResponse(TBase request) throws DatabricksSQLException {
    refreshHeadersIfRequired();
    LOGGER.debug(String.format("Fetching thrift response for request {%s}", request.toString()));
    try {
      if (request instanceof TOpenSessionReq) {
        return getThriftClient().OpenSession((TOpenSessionReq) request);
      } else if (request instanceof TCloseSessionReq) {
        return getThriftClient().CloseSession((TCloseSessionReq) request);
      } else if (request instanceof TGetPrimaryKeysReq) {
        return listPrimaryKeys((TGetPrimaryKeysReq) request);
      } else if (request instanceof TGetFunctionsReq) {
        return listFunctions((TGetFunctionsReq) request);
      } else if (request instanceof TGetSchemasReq) {
        return listSchemas((TGetSchemasReq) request);
      } else if (request instanceof TGetColumnsReq) {
        return listColumns((TGetColumnsReq) request);
      } else if (request instanceof TGetCatalogsReq) {
        return getCatalogs((TGetCatalogsReq) request);
      } else if (request instanceof TGetTablesReq) {
        return getTables((TGetTablesReq) request);
      } else if (request instanceof TGetTableTypesReq) {
        return getTableTypes((TGetTableTypesReq) request);
      } else if (request instanceof TGetTypeInfoReq) {
        return getTypeInfo((TGetTypeInfoReq) request);
      } else {
        String errorMessage =
            String.format(
                "No implementation for fetching thrift response for Request {%s}", request);
        LOGGER.error(errorMessage);
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
              request, e.getMessage());
      LOGGER.error(e, errorMessage);
      if (e instanceof SQLException) {
        throw new DatabricksSQLException(errorMessage, ((SQLException) e).getSQLState());
      } else {
        throw new DatabricksSQLException(errorMessage);
      }
    }
  }

  TFetchResultsResp getResultSetResp(TOperationHandle operationHandle, String context)
      throws DatabricksHttpException {
    refreshHeadersIfRequired();
    return getResultSetResp(
        new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS),
        operationHandle,
        context,
        DEFAULT_ROW_LIMIT,
        false);
  }

  TCancelOperationResp cancelOperation(TCancelOperationReq req) throws DatabricksHttpException {
    refreshHeadersIfRequired();
    try {
      return getThriftClient().CancelOperation(req);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while canceling operation from Thrift server. Request {%s}, Error {%s}",
              req.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
  }

  TCloseOperationResp closeOperation(TCloseOperationReq req) throws DatabricksHttpException {
    refreshHeadersIfRequired();
    try {
      return getThriftClient().CloseOperation(req);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while closing operation from Thrift server. Request {%s}, Error {%s}",
              req.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
  }

  TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    String context =
        String.format(
            "Fetching more results as it has more rows %s",
            parentStatement.getStatementId().toSQLExecStatementId());
    int maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();
    return getResultSetResp(
        new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS),
        getOperationHandle(parentStatement.getStatementId()),
        context,
        maxRows,
        true);
  }

  private TFetchResultsResp getResultSetResp(
      TStatus responseStatus,
      TOperationHandle operationHandle,
      String context,
      int maxRows,
      boolean fetchMetadata)
      throws DatabricksHttpException {
    verifySuccessStatus(responseStatus, context);
    TFetchResultsReq request =
        new TFetchResultsReq()
            .setOperationHandle(operationHandle)
            .setFetchType((short) 0) // 0 represents Query output. 1 represents Log
            .setMaxRows(maxRows)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);
    if (fetchMetadata) {
      request.setIncludeResultSetMetadata(true);
    }
    TFetchResultsResp response;
    try {
      response = getThriftClient().FetchResults(request);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while fetching results from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
    verifySuccessStatus(
        response.getStatus(),
        String.format(
            "Error while fetching results Request {%s}. TFetchResultsResp {%s}. ",
            request, response));
    return response;
  }

  DatabricksResultSet execute(
      TExecuteStatementReq request,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {
    refreshHeadersIfRequired();

    // Set direct result configuration
    int maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();
    if (enableDirectResults) {
      TSparkGetDirectResults directResults =
          new TSparkGetDirectResults().setMaxBytes(DEFAULT_BYTE_LIMIT).setMaxRows(maxRows);
      request.setGetDirectResults(directResults);
    }

    TExecuteStatementResp response;
    TFetchResultsResp resultSet;

    try {
      response = getThriftClient().ExecuteStatement(request);
      checkResponseForErrors(response);

      TGetOperationStatusResp statusResp = null;
      if (response.isSetDirectResults()) {
        checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
        statusResp = response.getDirectResults().getOperationStatus();
        checkOperationStatusForErrors(statusResp);
      }

      // Polling until query operation state is finished
      TGetOperationStatusReq statusReq =
          new TGetOperationStatusReq()
              .setOperationHandle(response.getOperationHandle())
              .setGetProgressUpdate(false);
      while (shouldContinuePolling(statusResp)) {
        statusResp = getThriftClient().GetOperationStatus(statusReq);
        checkOperationStatusForErrors(statusResp);
      }

      if (hasResultDataInDirectResults(response)) {
        // The first response has result data
        // There is no polling in this case as status was already finished
        resultSet = response.getDirectResults().getResultSet();
        resultSet.setResultSetMetadata(response.getDirectResults().getResultSetMetadata());
      } else {
        // Fetch the result data after polling
        resultSet =
            getResultSetResp(
                response.getStatus(),
                response.getOperationHandle(),
                response.toString(),
                maxRows,
                true);
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
    StatementId statementId = new StatementId(response.getOperationHandle().operationId);
    if (parentStatement != null) {
      parentStatement.setStatementId(statementId);
    }
    return new DatabricksResultSet(
        response.getStatus(), statementId, resultSet, statementType, parentStatement, session);
  }

  DatabricksResultSet executeAsync(
      TExecuteStatementReq request,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {
    refreshHeadersIfRequired();
    TExecuteStatementResp response;
    try {
      response = getThriftClient().ExecuteStatement(request);
      if (Arrays.asList(TStatusCode.ERROR_STATUS, TStatusCode.INVALID_HANDLE_STATUS)
          .contains(response.status.statusCode)) {
        LOGGER.error(
            "Received error response {%s} from Thrift Server for request {%s}",
            response, request.toString());
        throw new DatabricksSQLException(response.status.errorMessage, response.status.sqlState);
      }
    } catch (DatabricksSQLException | TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      if (e instanceof DatabricksSQLException) {
        throw new DatabricksHttpException(errorMessage, ((DatabricksSQLException) e).getSQLState());
      } else {
        throw new DatabricksHttpException(errorMessage, e);
      }
    }
    StatementId statementId = new StatementId(response.getOperationHandle().operationId);
    if (parentStatement != null) {
      parentStatement.setStatementId(statementId);
    }
    return new DatabricksResultSet(
        response.getStatus(), statementId, null, statementType, parentStatement, session);
  }

  DatabricksResultSet getStatementResult(
      TOperationHandle operationHandle,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session)
      throws SQLException {
    LOGGER.debug("Operation handle {%s}", operationHandle);
    TGetOperationStatusReq request =
        new TGetOperationStatusReq()
            .setOperationHandle(operationHandle)
            .setGetProgressUpdate(false);
    TGetOperationStatusResp response;
    TStatusCode statusCode;
    TFetchResultsResp resultSet = null;
    try {
      response = getThriftClient().GetOperationStatus(request);
      statusCode = response.getStatus().getStatusCode();
      if (statusCode == TStatusCode.SUCCESS_STATUS
          || statusCode == TStatusCode.SUCCESS_WITH_INFO_STATUS) {
        resultSet =
            getResultSetResp(response.getStatus(), operationHandle, response.toString(), -1, true);
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e);
    }
    StatementId statementId = new StatementId(operationHandle.getOperationId());
    return new DatabricksResultSet(
        response.getStatus(), statementId, resultSet, StatementType.SQL, parentStatement, session);
  }

  void resetAccessToken(String newAccessToken) {
    this.databricksConfig.setToken(newAccessToken);
  }

  private TFetchResultsResp listFunctions(TGetFunctionsReq request)
      throws DatabricksHttpException, TException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetFunctionsResp response = getThriftClient().GetFunctions(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp listPrimaryKeys(TGetPrimaryKeysReq request)
      throws DatabricksHttpException, TException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetPrimaryKeysResp response = getThriftClient().GetPrimaryKeys(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTables(TGetTablesReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTablesResp response = getThriftClient().GetTables(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTableTypes(TGetTableTypesReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTableTypesResp response = getThriftClient().GetTableTypes(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getCatalogs(TGetCatalogsReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCatalogsResp response = getThriftClient().GetCatalogs(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp listSchemas(TGetSchemasReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetSchemasResp response = getThriftClient().GetSchemas(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp getTypeInfo(TGetTypeInfoReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTypeInfoResp response = getThriftClient().GetTypeInfo(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private TFetchResultsResp listColumns(TGetColumnsReq request)
      throws TException, DatabricksHttpException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetColumnsResp response = getThriftClient().GetColumns(request);
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(response.getDirectResults(), response.toString());
      return response.getDirectResults().getResultSet();
    }
    return getResultSetResp(
        response.getStatus(),
        response.getOperationHandle(),
        response.toString(),
        DEFAULT_ROW_LIMIT,
        false);
  }

  private void refreshHeadersIfRequired() {
    ((DatabricksHttpTTransport) getThriftClient().getInputProtocol().getTransport())
        .setCustomHeaders(databricksConfig.authenticate());
  }

  private TCLIService.Client getThriftClient() {
    return thriftClient.get();
  }

  /**
   * Creates a new thrift client for the given endpoint URL and authentication headers.
   *
   * @param endPointUrl endpoint URL
   * @param authHeaders authentication headers
   * @param connectionContext connection context
   */
  private TCLIService.Client createThriftClient(
      String endPointUrl,
      Map<String, String> authHeaders,
      IDatabricksConnectionContext connectionContext) {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            DatabricksHttpClientFactory.getInstance().getClient(connectionContext), endPointUrl);
    transport.setCustomHeaders(authHeaders);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    return new TCLIService.Client(protocol);
  }

  private void checkResponseForErrors(TExecuteStatementResp response) throws SQLException {
    if (!response.isSetOperationHandle()) {
      throw new DatabricksSQLException("Operation handle not set");
    }
    if (isErrorStatusCode(response.status.statusCode)) {
      throw new DatabricksSQLException(
          response.status.getErrorMessage(), response.status.getSqlState());
    }
  }

  private void checkOperationStatusForErrors(TGetOperationStatusResp statusResp)
      throws SQLException {
    if (statusResp != null
        && statusResp.isSetOperationState()
        && isErrorOperationState(statusResp.getOperationState())) {
      throw new DatabricksSQLException("Operation state erroneous");
    }
  }

  private boolean shouldContinuePolling(TGetOperationStatusResp statusResp) {
    return statusResp == null
        || !statusResp.isSetOperationState()
        || isPendingOperationState(statusResp.getOperationState());
  }

  private boolean hasResultDataInDirectResults(TExecuteStatementResp response) {
    return response.isSetDirectResults()
        && response.getDirectResults().isSetResultSet()
        && response.getDirectResults().isSetResultSetMetadata();
  }

  private boolean isErrorStatusCode(TStatusCode statusCode) {
    return statusCode == TStatusCode.ERROR_STATUS
        || statusCode == TStatusCode.INVALID_HANDLE_STATUS;
  }

  private boolean isErrorOperationState(TOperationState state) {
    return state == TOperationState.ERROR_STATE || state == TOperationState.CLOSED_STATE;
  }

  private boolean isPendingOperationState(TOperationState state) {
    return state == TOperationState.RUNNING_STATE || state == TOperationState.PENDING_STATE;
  }
}
