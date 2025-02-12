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
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.StatementState;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;

final class DatabricksThriftAccessor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksThriftAccessor.class);
  private static final TSparkGetDirectResults DEFAULT_DIRECT_RESULTS =
      new TSparkGetDirectResults().setMaxRows(DEFAULT_ROW_LIMIT).setMaxBytes(DEFAULT_BYTE_LIMIT);
  private static final short directResultsFieldId =
      TExecuteStatementResp._Fields.DIRECT_RESULTS.getThriftFieldId();
  private static final short operationHandleFieldId =
      TExecuteStatementResp._Fields.OPERATION_HANDLE.getThriftFieldId();
  private static final short statusFieldId =
      TExecuteStatementResp._Fields.STATUS.getThriftFieldId();
  private static final int POLLING_INTERVAL_SECONDS = 1;
  private final ThreadLocal<TCLIService.Client> thriftClient;
  private final boolean enableDirectResults;

  DatabricksThriftAccessor(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.enableDirectResults = connectionContext.getDirectResultMode();
    DatabricksConfig databricksConfig =
        new ClientConfigurator(connectionContext).getDatabricksConfig();
    String endPointUrl = connectionContext.getEndpointURL();

    if (!DriverUtil.isRunningAgainstFake()) {
      // Create a new thrift client for each thread as client state is not thread safe. Note that
      // the underlying protocol uses the same http client which is thread safe
      this.thriftClient =
          ThreadLocal.withInitial(
              () -> createThriftClient(endPointUrl, databricksConfig, connectionContext));
    } else {
      TCLIService.Client client =
          createThriftClient(endPointUrl, databricksConfig, connectionContext);
      this.thriftClient = ThreadLocal.withInitial(() -> client);
    }
  }

  @VisibleForTesting
  DatabricksThriftAccessor(
      TCLIService.Client client, IDatabricksConnectionContext connectionContext) {
    this.thriftClient = ThreadLocal.withInitial(() -> client);
    this.enableDirectResults = connectionContext.getDirectResultMode();
  }

  @SuppressWarnings("rawtypes")
  TBase getThriftResponse(TBase request) throws DatabricksSQLException {
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
          throw new DatabricksHttpException(
              cause.getMessage(), cause, DatabricksDriverErrorCode.INVALID_STATE);
        }
        cause = cause.getCause();
      }
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request, e.getMessage());
      LOGGER.error(e, errorMessage);
      if (e instanceof SQLException) {
        throw new DatabricksSQLException(errorMessage, e, ((SQLException) e).getSQLState());
      } else {
        throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }
  }

  TFetchResultsResp getResultSetResp(TOperationHandle operationHandle, String context)
      throws DatabricksHttpException {
    return getResultSetResp(
        new TStatus().setStatusCode(TStatusCode.SUCCESS_STATUS),
        operationHandle,
        context,
        DEFAULT_ROW_LIMIT,
        false);
  }

  TCancelOperationResp cancelOperation(TCancelOperationReq req) throws DatabricksHttpException {
    try {
      return getThriftClient().CancelOperation(req);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while canceling operation from Thrift server. Request {%s}, Error {%s}",
              req.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  TCloseOperationResp closeOperation(TCloseOperationReq req) throws DatabricksHttpException {
    try {
      return getThriftClient().CloseOperation(req);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while closing operation from Thrift server. Request {%s}, Error {%s}",
              req.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
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

  DatabricksResultSet execute(
      TExecuteStatementReq request,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {

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

      StatementId statementId = new StatementId(response.getOperationHandle().operationId);
      if (parentStatement != null) {
        parentStatement.setStatementId(statementId);
      }

      // Get the operation status from direct results if present
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
        // Polling for operation status
        statusResp = getThriftClient().GetOperationStatus(statusReq);
        checkOperationStatusForErrors(statusResp);
        try {
          TimeUnit.SECONDS.sleep(POLLING_INTERVAL_SECONDS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt(); // Restore interrupt flag
          cancelOperation(
              new TCancelOperationReq().setOperationHandle(response.getOperationHandle()));
          throw new DatabricksSQLException(
              "Query execution interrupted", e, DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
        }
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

      return new DatabricksResultSet(
          getStatementStatus(statusResp),
          statementId,
          resultSet,
          statementType,
          parentStatement,
          session);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  DatabricksResultSet executeAsync(
      TExecuteStatementReq request,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {
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
        throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
      }
    }
    StatementId statementId = new StatementId(response.getOperationHandle().operationId);
    if (parentStatement != null) {
      parentStatement.setStatementId(statementId);
    }
    StatementStatus statementStatus = getAsyncStatus(response.getStatus());
    return new DatabricksResultSet(
        statementStatus, statementId, null, statementType, parentStatement, session);
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
    TFetchResultsResp resultSet = null;
    StatementId statementId = new StatementId(operationHandle.getOperationId());
    try {
      response = getThriftClient().GetOperationStatus(request);
      TOperationState operationState = response.getOperationState();
      if (operationState == TOperationState.FINISHED_STATE) {
        resultSet =
            getResultSetResp(response.getStatus(), operationHandle, response.toString(), -1, true);
        return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            statementId,
            resultSet,
            StatementType.SQL,
            parentStatement,
            session);
      }
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
    StatementStatus executionStatus = getStatementStatus(response);
    return new DatabricksResultSet(
        executionStatus, statementId, resultSet, StatementType.SQL, parentStatement, session);
  }

  TCLIService.Client getThriftClient() {
    return thriftClient.get();
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
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
    verifySuccessStatus(
        response.getStatus(),
        String.format(
            "Error while fetching results Request {%s}. TFetchResultsResp {%s}. ",
            request, response));
    return response;
  }

  private TFetchResultsResp listFunctions(TGetFunctionsReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetFunctionsResp response = getThriftClient().GetFunctions(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listPrimaryKeys(TGetPrimaryKeysReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetPrimaryKeysResp response = getThriftClient().GetPrimaryKeys(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTables(TGetTablesReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTablesResp response = getThriftClient().GetTables(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTableTypes(TGetTableTypesReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTableTypesResp response = getThriftClient().GetTableTypes(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getCatalogs(TGetCatalogsReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCatalogsResp response = getThriftClient().GetCatalogs(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listSchemas(TGetSchemasReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetSchemasResp response = getThriftClient().GetSchemas(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTypeInfo(TGetTypeInfoReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTypeInfoResp response = getThriftClient().GetTypeInfo(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listColumns(TGetColumnsReq request)
      throws TException, DatabricksSQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetColumnsResp response = getThriftClient().GetColumns(request);
    return fetchMetadataResults(response, response.toString());
  }

  /**
   * Creates a new thrift client for the given endpoint URL and authentication headers.
   *
   * @param endPointUrl endpoint URL
   * @param databricksConfig SDK config object required for authentication headers
   */
  private TCLIService.Client createThriftClient(
      String endPointUrl,
      DatabricksConfig databricksConfig,
      IDatabricksConnectionContext connectionContext) {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            DatabricksHttpClientFactory.getInstance().getClient(connectionContext),
            endPointUrl,
            databricksConfig,
            connectionContext);
    TBinaryProtocol protocol = new TBinaryProtocol(transport);

    return new TCLIService.Client(protocol);
  }

  /**
   * Fetches the metadata results from the given response object. If the response object contains a
   * directResults field, then the metadata results are fetched from the directResults field.
   * Otherwise, the metadata results are fetched by polling the operation status.
   *
   * @param response Thrift response object
   * @param contextDescription description of the context in which the response was received
   * @return metadata results {@link TFetchResultsResp}
   * @param <TResp> Thrift response type
   * @param <FResp> Thrift response field type
   * @throws TException if an error occurs while fetching the operation status during polling
   * @throws DatabricksSQLException if an error occurs while fetching the metadata results
   */
  private <TResp extends TBase<TResp, FResp>, FResp extends TFieldIdEnum>
      TFetchResultsResp fetchMetadataResults(TResp response, String contextDescription)
          throws TException, DatabricksSQLException {
    checkResponseForErrors(response);

    // Get the operation status from direct results if present
    TGetOperationStatusResp statusResp = null;
    FResp directResultsField = response.fieldForId(directResultsFieldId);
    if (response.isSet(directResultsField)) {
      TSparkDirectResults directResults =
          (TSparkDirectResults) response.getFieldValue(directResultsField);
      checkDirectResultsForErrorStatus(directResults, contextDescription);
      statusResp = directResults.getOperationStatus();
      checkOperationStatusForErrors(statusResp);
    }

    // Get the operation handle from the response
    FResp operationHandleField = response.fieldForId(operationHandleFieldId);
    TOperationHandle operationHandle =
        (TOperationHandle) response.getFieldValue(operationHandleField);

    // Polling until query operation state is finished
    TGetOperationStatusReq statusReq =
        new TGetOperationStatusReq()
            .setOperationHandle(operationHandle)
            .setGetProgressUpdate(false);
    while (shouldContinuePolling(statusResp)) {
      statusResp = getThriftClient().GetOperationStatus(statusReq);
      checkOperationStatusForErrors(statusResp);
    }

    if (hasResultDataInDirectResults(response)) {
      // The first response has result data
      // There is no polling in this case as status was already finished
      TSparkDirectResults directResults =
          (TSparkDirectResults) response.getFieldValue(directResultsField);
      return directResults.getResultSet();
    } else {
      // Fetch the result data after polling
      FResp statusField = response.fieldForId(statusFieldId);
      TStatus status = (TStatus) response.getFieldValue(statusField);
      return getResultSetResp(
          status, operationHandle, contextDescription, DEFAULT_ROW_LIMIT, false);
    }
  }

  /**
   * Check the response for errors.
   *
   * @param response Thrift response object
   * @param <T> Thrift response type
   * @param <F> Thrift response field type
   * @throws DatabricksSQLException if the response contains an error status
   */
  private <T extends TBase<T, F>, F extends TFieldIdEnum> void checkResponseForErrors(
      TBase<T, F> response) throws DatabricksSQLException {
    F operationHandleField = response.fieldForId(operationHandleFieldId);
    if (!response.isSet(operationHandleField)) {
      throw new DatabricksSQLException(
          "Operation handle not set", DatabricksDriverErrorCode.INVALID_STATE);
    }
    F statusField = response.fieldForId(statusFieldId);
    TStatus status = (TStatus) response.getFieldValue(statusField);
    if (isErrorStatusCode(status.getStatusCode())) {
      throw new DatabricksSQLException(status.getErrorMessage(), status.getSqlState());
    }
  }

  private void checkOperationStatusForErrors(TGetOperationStatusResp statusResp)
      throws DatabricksSQLException {
    if (statusResp != null
        && statusResp.isSetOperationState()
        && isErrorOperationState(statusResp.getOperationState())) {
      String errorMsg =
          String.format("Operation failed with error: %s", statusResp.getErrorMessage());
      LOGGER.error(errorMsg);
      throw new DatabricksSQLException(errorMsg, statusResp.getSqlState());
    }
  }

  private boolean shouldContinuePolling(TGetOperationStatusResp statusResp) {
    return statusResp == null
        || !statusResp.isSetOperationState()
        || isPendingOperationState(statusResp.getOperationState());
  }

  private <T extends TBase<T, F>, F extends TFieldIdEnum> boolean hasResultDataInDirectResults(
      TBase<T, F> response) {
    F directResultsField = response.fieldForId(directResultsFieldId);
    if (!response.isSet(directResultsField)) {
      return false;
    }
    TSparkDirectResults directResults =
        (TSparkDirectResults) response.getFieldValue(directResultsField);
    return directResults.isSetResultSet() && directResults.isSetResultSetMetadata();
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
