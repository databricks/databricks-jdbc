package com.databricks.jdbc.dbclient.impl.thrift;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.COMMUNICATION_LINK_FAILURE_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_EXECUTION_TIMEOUT_SQLSTATE;
import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static com.databricks.jdbc.common.util.DatabricksThriftUtil.*;
import static com.databricks.jdbc.common.util.SqlStateClassifier.classifyTransientSqlState;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.ProtocolFeatureUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.common.TimeoutHandler;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.service.sql.StatementState;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.http.HttpException;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransportException;

final class DatabricksThriftAccessor {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksThriftAccessor.class);
  private static final TSparkGetDirectResults DEFAULT_DIRECT_RESULTS =
      new TSparkGetDirectResults()
          .setMaxRows(DEFAULT_ROW_LIMIT_PER_BLOCK)
          .setMaxBytes(DEFAULT_BYTE_LIMIT);
  private static final short directResultsFieldId =
      TExecuteStatementResp._Fields.DIRECT_RESULTS.getThriftFieldId();
  private static final short operationHandleFieldId =
      TExecuteStatementResp._Fields.OPERATION_HANDLE.getThriftFieldId();
  private static final short statusFieldId =
      TExecuteStatementResp._Fields.STATUS.getThriftFieldId();
  // Bounded, jittered retry for transient transport-level failures (stale pooled connection,
  // connection reset, load-balancer idle drop) on idempotent status / close / cancel RPCs. A
  // status poll is read-only, and closing or cancelling an operation is idempotent, so repeating
  // any of them produces no additional server-side side effects. This is why statement submission
  // is deliberately NOT routed through the retry path: re-sending an ExecuteStatement could run the
  // query twice.
  private static final int MAX_POLL_TRANSPORT_RETRIES = 5;
  // Cleanup RPCs (CloseOperation / CancelOperation) run on close / cancel / timeout paths that must
  // not hang: one transient blip is worth reconnecting past, but a sustained outage should surface
  // fast rather than stall shutdown. Hence a far smaller budget than the status-poll path.
  private static final int MAX_CLEANUP_TRANSPORT_RETRIES = 2;
  private static final long TRANSPORT_RETRY_MIN_BACKOFF_MILLIS = 1_000L;
  private static final long TRANSPORT_RETRY_MAX_BACKOFF_MILLIS = 16_000L;
  // Transient HTTP gateway codes that the shared DatabricksHttpRetryHandler does NOT itself retry
  // (it only retries 429/503 + configured custom codes). A poll that hits one of these received a
  // real HTTP response but from a transiently-unhealthy hop, so re-polling on a fresh connection is
  // safe and worthwhile. 429/503 are deliberately excluded here: they are owned by the HTTP layer,
  // and re-retrying them would multiply load on a recovering endpoint.
  private static final Set<Integer> RETRYABLE_TRANSPORT_HTTP_CODES = Set.of(408, 500, 502, 504);

  private DatabricksConfig databricksConfig;
  private final boolean enableDirectResults;
  private final int asyncPollIntervalMillis;
  private final int maxRowsPerBlock;
  private final String connectionUuid;
  private final String endpointUrl;
  private final IDatabricksConnectionContext connectionContext;
  private TProtocolVersion serverProtocolVersion = JDBC_THRIFT_VERSION;
  private ThreadLocal<TCLIService.Client> FAKE_SHARED_CLIENT;

  DatabricksThriftAccessor(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException, DatabricksValidationException {
    this.enableDirectResults = connectionContext.getDirectResultMode();
    this.databricksConfig =
        DatabricksClientConfiguratorManager.getInstance()
            .getConfigurator(connectionContext)
            .getDatabricksConfig();
    this.endpointUrl = connectionContext.getEndpointURL();
    this.asyncPollIntervalMillis = connectionContext.getAsyncExecPollInterval();
    this.maxRowsPerBlock = connectionContext.getRowsFetchedPerBlock();
    this.connectionUuid = connectionContext.getConnectionUuid();
    this.connectionContext = connectionContext;
    if (DriverUtil.isRunningAgainstFake()) {
      TCLIService.Client client = newThriftClient();
      this.FAKE_SHARED_CLIENT = ThreadLocal.withInitial(() -> client);
    }
  }

  @SuppressWarnings("rawtypes")
  TBase getThriftResponse(TBase request) throws DatabricksSQLException {
    LOGGER.debug("Fetching thrift response for request {}", request.toString());
    try {
      if (request instanceof TOpenSessionReq) {
        return getThriftClient().OpenSession((TOpenSessionReq) request);
      } else if (request instanceof TCloseSessionReq) {
        return getThriftClient().CloseSession((TCloseSessionReq) request);
      } else if (request instanceof TGetFunctionsReq) {
        return listFunctions((TGetFunctionsReq) request);
      } else if (request instanceof TGetPrimaryKeysReq) {
        return listPrimaryKeys((TGetPrimaryKeysReq) request);
      } else if (request instanceof TGetCrossReferenceReq) {
        return listCrossReferences((TGetCrossReferenceReq) request);
      } else if (request instanceof TGetCatalogsReq) {
        return getCatalogs((TGetCatalogsReq) request);
      } else if (request instanceof TGetTablesReq) {
        return getTables((TGetTablesReq) request);
      } else if (request instanceof TGetTableTypesReq) {
        return getTableTypes((TGetTableTypesReq) request);
      } else if (request instanceof TGetSchemasReq) {
        return listSchemas((TGetSchemasReq) request);
      } else if (request instanceof TGetTypeInfoReq) {
        return getTypeInfo((TGetTypeInfoReq) request);
      } else if (request instanceof TGetColumnsReq) {
        return listColumns((TGetColumnsReq) request);
      }
      String errorMessage =
          String.format("No implementation for fetching thrift response for Request {%s}", request);
      LOGGER.error(errorMessage);
      throw new DatabricksSQLFeatureNotSupportedException(errorMessage);
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

  /**
   * Fetch the next set of results for the given operation handle with default settings.
   *
   * @param operationHandle the operation handle
   * @return TFetchResultsResp containing the results
   * @throws DatabricksHttpException if fetch fails
   */
  TFetchResultsResp getResultSetResp(TOperationHandle operationHandle) throws SQLException {
    TFetchResultsReq req = createFetchResultsReqWithDefaults(operationHandle);
    return executeFetchRequest(req);
  }

  /**
   * Fetches results starting from a specific row offset.
   *
   * @param operationHandle the operation handle
   * @param startRowOffset the row offset to start fetching from
   * @return TFetchResultsResp containing the results
   * @throws DatabricksHttpException if fetch fails
   */
  TFetchResultsResp getResultSetResp(TOperationHandle operationHandle, long startRowOffset)
      throws SQLException {
    TFetchResultsReq req = createFetchResultsReqWithDefaults(operationHandle);
    req.setStartRowOffset(startRowOffset);
    return executeFetchRequest(req);
  }

  TCancelOperationResp cancelOperation(TCancelOperationReq req) throws DatabricksHttpException {
    try {
      return withCleanupTransportRetry(
          "CancelOperation",
          loggableOperationHandle(req.getOperationHandle()),
          () -> getThriftClient().CancelOperation(req));
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
      return withCleanupTransportRetry(
          "CloseOperation",
          loggableOperationHandle(req.getOperationHandle()),
          () -> getThriftClient().CloseOperation(req));
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
      throws SQLException {
    TFetchResultsReq req =
        createFetchResultsReqWithDefaults(getOperationHandle(parentStatement.getStatementId()));
    setFetchMetadata(req);
    return executeFetchRequest(req);
  }

  DatabricksResultSet execute(
      TExecuteStatementReq request,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementType statementType)
      throws SQLException {

    try {
      // Set direct result configuration
      if (enableDirectResults) {
        // if getDirectResults.maxRows > 0, the server will immediately call FetchResults. Fetch
        // initial rows limited by maxRows.
        // if = 0, server does not call FetchResults.
        TSparkGetDirectResults directResults =
            new TSparkGetDirectResults()
                .setMaxBytes(DEFAULT_BYTE_LIMIT)
                .setMaxRows(maxRowsPerBlock);
        request.setGetDirectResults(directResults);
      }
      TExecuteStatementResp response;
      TFetchResultsResp resultSet;
      response = getThriftClient().ExecuteStatement(request);
      checkResponseForErrors(response);

      StatementId statementId = new StatementId(response.getOperationHandle().operationId);
      LOGGER.debug(
          "Executed statement for statementId {} in session {}",
          statementId.toSQLExecStatementId(),
          session.getSessionId());

      DatabricksThreadContextHolder.setStatementId(statementId);
      if (parentStatement != null) {
        parentStatement.setStatementId(statementId);
      }

      // Get the operation status from direct results if present
      String sessionDebugInfo =
          String.format(
              "Session [%s] with (%s)", session.getSessionId(), session.getComputeResource());

      TGetOperationStatusResp statusResp =
          pollTillOperationFinished(
              response, parentStatement, session, statementId, sessionDebugInfo, statementType);
      boolean isDirectResults = hasResultDataInDirectResults(response);
      if (isDirectResults) {
        // The first response has result data
        // There is no polling in this case as status was already finished
        resultSet = response.getDirectResults().getResultSet();
        resultSet.setResultSetMetadata(response.getDirectResults().getResultSetMetadata());
      } else {
        verifySuccessStatus(
            response.getStatus(), "executeStatement", statementId.toSQLExecStatementId());

        // Fetch the result data after polling
        TFetchResultsReq resultsReq =
            createFetchResultsReqWithDefaults(response.getOperationHandle());
        setFetchMetadata(resultsReq);
        long fetchStartTime = System.nanoTime();
        resultSet = executeFetchRequest(resultsReq);

        long fetchEndTime = System.nanoTime();
        long fetchLatencyNanos = fetchEndTime - fetchStartTime;
        long fetchLatencyMillis = fetchLatencyNanos / 1_000_000;
        LOGGER.debug(
            String.format(
                "Connection [%s] Statement [%s] Session [%s] Thrift fetch latency: %dms",
                connectionUuid, statementId, sessionDebugInfo, fetchLatencyMillis));
      }

      DatabricksResultSet databricksResultSet =
          new DatabricksResultSet(
              getStatementStatus(statusResp),
              statementId,
              resultSet,
              statementType,
              parentStatement,
              session);

      // Mark direct results only if the server confirmed it closed the operation.
      // TSparkDirectResults.closeOperation is optional — a server can return inline
      // data without closing the op (older protocol versions, interactive flows).
      // Without this guard, close() would skip the server RPC and leak the handle.
      if (isDirectResults
          && parentStatement != null
          && response.getDirectResults().isSetCloseOperation()) {
        LOGGER.debug(
            "Statement {} received direct results via Thrift with close confirmation, "
                + "marking as direct results received",
            statementId);
        parentStatement.markDirectResultsReceived();
      }

      return databricksResultSet;
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request, e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
  }

  private TGetOperationStatusResp pollTillOperationFinished(
      TExecuteStatementResp response,
      IDatabricksStatementInternal parentStatement,
      IDatabricksSession session,
      StatementId statementId,
      String sessionDebugInfo,
      StatementType statementType)
      throws SQLException, TException {
    int timeoutInSeconds;
    if (parentStatement != null) {
      timeoutInSeconds = parentStatement.getStatement().getQueryTimeout();
    } else if (statementType == StatementType.METADATA) {
      timeoutInSeconds = connectionContext.getMetadataOperationTimeout();
    } else {
      timeoutInSeconds = 0;
    }

    TGetOperationStatusResp statusResp = null;
    if (response.isSetDirectResults()) {
      checkDirectResultsForErrorStatus(
          response.getDirectResults(),
          "executeStatement DirectResults",
          statementId.toSQLExecStatementId());
      statusResp = response.getDirectResults().getOperationStatus();
      checkOperationStatusForErrors(
          statusResp, StatementId.loggableStatementId(response.getOperationHandle()));
    }

    TimeoutHandler timeoutHandler =
        getTimeoutHandler(
            response, timeoutInSeconds, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);

    // Polling until query operation state is finished
    long pollingStartTime = System.nanoTime();
    TGetOperationStatusReq statusReq =
        new TGetOperationStatusReq()
            .setOperationHandle(response.getOperationHandle())
            .setGetProgressUpdate(false);
    while (shouldContinuePolling(statusResp)) {
      // Check for timeout before continuing
      timeoutHandler.checkTimeout();

      // TTransportException means a transport-level failure (e.g. HTTP 502 Bad Gateway)
      // after retries were exhausted. Other TException subtypes propagate unchanged. The timeout
      // handler is threaded in so the retry backoff cannot overshoot the statement's queryTimeout.
      try {
        statusResp = getOperationStatus(statusReq, statementId, timeoutHandler);
      } catch (TTransportException e) {
        throw buildTransportFailureException(statementId.toSQLExecStatementId(), e);
      }
      checkOperationStatusForErrors(statusResp, statementId.toSQLExecStatementId());
      // Save some time if sleep isn't required by breaking.
      if (!shouldContinuePolling(statusResp)) {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(asyncPollIntervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt(); // Restore interrupt flag
        cancelOperation(
            new TCancelOperationReq().setOperationHandle(response.getOperationHandle()));
        throw new DatabricksSQLException(
            "Query execution interrupted", e, DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
      }
    }
    long pollingEndTime = System.nanoTime();
    long pollingLatencyNanos = pollingEndTime - pollingStartTime;
    long pollingLatencyMillis = pollingLatencyNanos / 1_000_000;
    LOGGER.debug(
        String.format(
            "Connection [%s] Statement [%s] Session [%s] Thrift polling latency: %dms",
            connectionUuid, statementId, sessionDebugInfo, pollingLatencyMillis));
    return statusResp;
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
            "Received error response {} from Thrift Server for request {}",
            response,
            request.toString());
        String originalSqlState = response.status.sqlState;
        String remappedSqlState =
            classifyTransientSqlState(response.status.errorMessage, originalSqlState);
        if (!Objects.equals(remappedSqlState, originalSqlState)) {
          LOGGER.info(
              "Remapped SQL state [{}] -> [{}] for transient error pattern in async execute response",
              originalSqlState,
              remappedSqlState);
        }
        throw new DatabricksSQLException(response.status.errorMessage, remappedSqlState);
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
    LOGGER.debug(
        String.format(
            "Executed statement in async for statementId [%s] in session [%s]",
            statementId.toSQLExecStatementId(), session.getSessionId()));
    DatabricksThreadContextHolder.setStatementId(statementId);
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
    LOGGER.debug(
        "getStatementResult for StatementId {}", StatementId.loggableStatementId(operationHandle));

    long getStatementResultStartTime = System.nanoTime();
    StatementId statementId = new StatementId(operationHandle.getOperationId());
    String sessionInfo = session.getSessionId() + " (" + session.getComputeResource() + ")";

    TGetOperationStatusReq request =
        new TGetOperationStatusReq()
            .setOperationHandle(operationHandle)
            .setGetProgressUpdate(false);
    TGetOperationStatusResp response;
    TFetchResultsResp resultSet = null;
    try {
      response = getOperationStatus(request, statementId);
      TOperationState operationState = response.getOperationState();
      if (operationState == TOperationState.CANCELED_STATE) {
        throw cancelledStatementException(statementId.toSQLExecStatementId());
      }
      if (operationState == TOperationState.FINISHED_STATE) {
        verifySuccessStatus(
            response.getStatus(), "getStatementResult", statementId.toSQLExecStatementId());

        long fetchStartTime = System.nanoTime();

        TFetchResultsReq resultsReq = createFetchResultsReqWithDefaults(operationHandle);
        resultsReq.setMaxRows(-1);
        setFetchMetadata(resultsReq);
        resultSet = executeFetchRequest(resultsReq);

        long fetchEndTime = System.nanoTime();
        long fetchLatencyNanos = fetchEndTime - fetchStartTime;
        long fetchLatencyMillis = fetchLatencyNanos / 1_000_000;
        LOGGER.debug(
            "Connection ["
                + connectionUuid
                + "] Statement ["
                + statementId
                + "] Session ["
                + sessionInfo
                + "] Thrift getStatementResult fetch latency: "
                + fetchLatencyMillis
                + "ms");

        long getStatementResultEndTime = System.nanoTime();
        long getStatementResultLatencyNanos =
            getStatementResultEndTime - getStatementResultStartTime;
        long getStatementResultLatencyMillis = getStatementResultLatencyNanos / 1_000_000;
        LOGGER.debug(
            "Connection ["
                + connectionUuid
                + "] Statement ["
                + statementId
                + "] Session ["
                + sessionInfo
                + "] Thrift getStatementResult latency: "
                + getStatementResultLatencyMillis
                + "ms");

        return new DatabricksResultSet(
            new StatementStatus().setState(StatementState.SUCCEEDED),
            statementId,
            resultSet,
            StatementType.SQL,
            parentStatement,
            session);
      }
    } catch (TException e) {
      long getStatementResultEndTime = System.nanoTime();
      long getStatementResultLatencyNanos = getStatementResultEndTime - getStatementResultStartTime;
      long getStatementResultLatencyMillis = getStatementResultLatencyNanos / 1_000_000;
      LOGGER.debug(
          "Connection ["
              + connectionUuid
              + "] Statement ["
              + statementId
              + "] Session ["
              + sessionInfo
              + "] Thrift getStatementResult latency (with error): "
              + getStatementResultLatencyMillis
              + "ms");

      String errorMessage =
          String.format(
              "Error while receiving response from Thrift server. Request {%s}, Error {%s}",
              request.toString(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }
    StatementStatus executionStatus = getStatementStatus(response);

    long getStatementResultEndTime = System.nanoTime();
    long getStatementResultLatencyNanos = getStatementResultEndTime - getStatementResultStartTime;
    long getStatementResultLatencyMillis = getStatementResultLatencyNanos / 1_000_000;
    LOGGER.debug(
        "Connection ["
            + connectionUuid
            + "] Statement ["
            + statementId
            + "] Session ["
            + sessionInfo
            + "] Thrift getStatementResult latency: "
            + getStatementResultLatencyMillis
            + "ms");

    return new DatabricksResultSet(
        executionStatus, statementId, resultSet, StatementType.SQL, parentStatement, session);
  }

  DatabricksConfig getDatabricksConfig() {
    return databricksConfig;
  }

  void updateConfig(DatabricksConfig newConfig) {
    this.databricksConfig = newConfig;
  }

  private TFetchResultsResp executeFetchRequest(TFetchResultsReq request) throws SQLException {
    TFetchResultsResp response;
    try {
      response = getThriftClient().FetchResults(request);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while fetching results from Thrift server. Request maxRows=%d, "
                  + "maxBytes=%d, Error {%s}",
              request.getMaxRows(), request.getMaxBytes(), e.getMessage());
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }

    String statementId = StatementId.loggableStatementId(request.getOperationHandle());
    verifySuccessStatus(
        response.getStatus(),
        String.format(
            "Error while fetching results Request maxRows=%d, maxBytes=%d. "
                + "Response hasMoreRows=%s",
            request.getMaxRows(), request.getMaxBytes(), response.hasMoreRows),
        statementId);

    return response;
  }

  private TFetchResultsReq createFetchResultsReqWithDefaults(TOperationHandle operationHandle) {
    return new TFetchResultsReq()
        .setOperationHandle(operationHandle)
        .setFetchType((short) 0) // 0 represents Query output. 1 represents Log
        .setMaxRows(maxRowsPerBlock) // Max number of rows that should be returned in the rowset.
        .setMaxBytes(DEFAULT_BYTE_LIMIT);
  }

  private void setFetchMetadata(TFetchResultsReq request) {
    if (ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(serverProtocolVersion)) {
      request.setIncludeResultSetMetadata(true);
    }
  }

  /**
   * Fetches results using FETCH_ABSOLUTE orientation starting from the given row offset.
   *
   * <p>This method is used by the streaming chunk provider to seek to a specific row position and
   * fetch a batch of results.
   *
   * @param operationHandle The operation handle for the statement
   * @param startRowOffset The row offset to start fetching from (0-indexed)
   * @return The fetch results response
   * @throws DatabricksHttpException if the fetch fails
   */
  TFetchResultsResp fetchResultsWithAbsoluteOffset(
      TOperationHandle operationHandle, long startRowOffset) throws SQLException {
    String statementId = StatementId.loggableStatementId(operationHandle);
    LOGGER.debug(
        "Fetching results with FETCH_ABSOLUTE at offset {} for statement {}",
        startRowOffset,
        statementId);

    TFetchResultsReq request =
        new TFetchResultsReq()
            .setOperationHandle(operationHandle)
            .setStartRowOffset(startRowOffset)
            .setFetchType((short) 0) // 0 represents Query output
            .setMaxRows(maxRowsPerBlock)
            .setMaxBytes(DEFAULT_BYTE_LIMIT);

    TFetchResultsResp response;
    try {
      response = getThriftClient().FetchResults(request);
    } catch (TException e) {
      String errorMessage =
          String.format(
              "Error while fetching results from Thrift server with FETCH_ABSOLUTE. "
                  + "startRowOffset=%d, maxRows=%d, Error {%s}",
              startRowOffset, request.getMaxRows(), e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksHttpException(errorMessage, e, DatabricksDriverErrorCode.INVALID_STATE);
    }

    verifySuccessStatus(
        response.getStatus(),
        String.format(
            "Error while fetching results with FETCH_ABSOLUTE. startRowOffset=%d, hasMoreRows=%s",
            startRowOffset, response.hasMoreRows),
        statementId);

    return response;
  }

  private TFetchResultsResp listFunctions(TGetFunctionsReq request)
      throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetFunctionsResp response = getThriftClient().GetFunctions(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listPrimaryKeys(TGetPrimaryKeysReq request)
      throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetPrimaryKeysResp response = getThriftClient().GetPrimaryKeys(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listCrossReferences(TGetCrossReferenceReq request)
      throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCrossReferenceResp response = getThriftClient().GetCrossReference(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTables(TGetTablesReq request) throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTablesResp response = getThriftClient().GetTables(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTableTypes(TGetTableTypesReq request)
      throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTableTypesResp response = getThriftClient().GetTableTypes(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getCatalogs(TGetCatalogsReq request) throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetCatalogsResp response = getThriftClient().GetCatalogs(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listSchemas(TGetSchemasReq request) throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetSchemasResp response = getThriftClient().GetSchemas(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp getTypeInfo(TGetTypeInfoReq request) throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetTypeInfoResp response = getThriftClient().GetTypeInfo(request);
    return fetchMetadataResults(response, response.toString());
  }

  private TFetchResultsResp listColumns(TGetColumnsReq request) throws TException, SQLException {
    if (enableDirectResults) request.setGetDirectResults(DEFAULT_DIRECT_RESULTS);
    TGetColumnsResp response = getThriftClient().GetColumns(request);
    return fetchMetadataResults(response, response.toString());
  }

  /** Creates a new thrift client for the given endpoint URL and authentication headers. */
  TCLIService.Client getThriftClient() {
    if (DriverUtil.isRunningAgainstFake()) {
      return FAKE_SHARED_CLIENT.get();
    }
    return newThriftClient();
  }

  private TCLIService.Client newThriftClient() {
    DatabricksHttpTTransport transport =
        new DatabricksHttpTTransport(
            DatabricksHttpClientFactory.getInstance().getClient(connectionContext),
            endpointUrl,
            databricksConfig,
            connectionContext);
    return new TCLIService.Client(new TBinaryProtocol(transport));
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
          throws TException, SQLException {
    checkResponseForErrors(response);

    // Get the operation status from direct results if present
    TGetOperationStatusResp statusResp = null;
    FResp directResultsField = response.fieldForId(directResultsFieldId);

    // Get the operation handle from the response
    FResp operationHandleField = response.fieldForId(operationHandleFieldId);
    TOperationHandle operationHandle =
        response.isSet(operationHandleField)
            ? (TOperationHandle) response.getFieldValue(operationHandleField)
            : null;
    String statementId =
        (operationHandle != null) ? StatementId.loggableStatementId(operationHandle) : "null";

    if (response.isSet(directResultsField)) {
      TSparkDirectResults directResults =
          (TSparkDirectResults) response.getFieldValue(directResultsField);
      checkDirectResultsForErrorStatus(directResults, contextDescription, statementId);
      statusResp = directResults.getOperationStatus();
      checkOperationStatusForErrors(statusResp, statementId);
    }

    LOGGER.debug("Poll for operation status for statementId: {}", statementId);

    // Polling until query operation state is finished
    TGetOperationStatusReq statusReq =
        new TGetOperationStatusReq()
            .setOperationHandle(operationHandle)
            .setGetProgressUpdate(false);
    TimeoutHandler metadataTimeoutHandler =
        new TimeoutHandler(
            connectionContext.getMetadataOperationTimeout(),
            "Metadata operation for statement: " + statementId,
            () -> {
              try {
                if (operationHandle != null) {
                  LOGGER.debug("Canceling metadata operation due to timeout: {}", operationHandle);
                  cancelOperation(new TCancelOperationReq().setOperationHandle(operationHandle));
                }
              } catch (Exception e) {
                LOGGER.warn("Failed to cancel metadata operation on timeout: {}", e.getMessage());
              }
            },
            DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR);
    while (shouldContinuePolling(statusResp)) {
      metadataTimeoutHandler.checkTimeout();
      try {
        statusResp =
            withTransportRetry(
                "GetOperationStatus",
                statementId,
                metadataTimeoutHandler,
                () -> getThriftClient().GetOperationStatus(statusReq));
      } catch (TTransportException e) {
        throw buildTransportFailureException(statementId, e);
      }
      checkOperationStatusForErrors(statusResp, statementId);
      if (!shouldContinuePolling(statusResp)) {
        break;
      }
      try {
        TimeUnit.MILLISECONDS.sleep(asyncPollIntervalMillis);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOGGER.error(
            "Metadata operation interrupted for statement [{}], canceling operation", statementId);
        if (operationHandle != null) {
          cancelOperation(new TCancelOperationReq().setOperationHandle(operationHandle));
        }
        throw new DatabricksSQLException(
            "Metadata operation interrupted",
            e,
            DatabricksDriverErrorCode.THREAD_INTERRUPTED_ERROR);
      }
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
      verifySuccessStatus(status, contextDescription, statementId);

      TFetchResultsReq resultsReq = createFetchResultsReqWithDefaults(operationHandle);
      resultsReq.setMaxRows(DEFAULT_ROW_LIMIT_PER_BLOCK);
      return executeFetchRequest(resultsReq);
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
    F statusField = response.fieldForId(statusFieldId);
    TStatus status = (TStatus) response.getFieldValue(statusField);

    if (!response.isSet(operationHandleField) || isErrorStatusCode(status)) {
      // if the operationHandle has not been set, it is an error from the server.
      LOGGER.error("Error thrift response {}", response);
      String originalSqlState = status.getSqlState();
      String remappedSqlState =
          classifyTransientSqlState(status.getErrorMessage(), originalSqlState);
      if (!Objects.equals(remappedSqlState, originalSqlState)) {
        LOGGER.info(
            "Remapped SQL state [{}] -> [{}] for transient error pattern in thrift response",
            originalSqlState,
            remappedSqlState);
      }
      throw new DatabricksSQLException(
          status.getErrorMessage(),
          remappedSqlState,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }
  }

  private void checkOperationStatusForErrors(TGetOperationStatusResp statusResp, String statementId)
      throws SQLException {
    if (statusResp == null) {
      return;
    }

    // Check TStatus for INVALID_HANDLE_STATUS — this can happen when the server restarts
    // and the operation handle becomes invalid. Without this check, the polling loop would
    // continue indefinitely since operationState may not be set in the response.
    if (statusResp.isSetStatus() && isErrorStatusCode(statusResp.getStatus())) {
      String serverError = enrichErrorMessage(statusResp.getStatus());
      String errorMsg =
          String.format(
              "Operation status check failed with status code: [%s] for statement [%s], "
                  + "error: [%s]",
              statusResp.getStatus().getStatusCode(), statementId, serverError);
      LOGGER.error(errorMsg);
      String originalSqlState = statusResp.isSetSqlState() ? statusResp.getSqlState() : null;
      String remappedSqlState = classifyTransientSqlState(serverError, originalSqlState);
      if (!Objects.equals(remappedSqlState, originalSqlState)) {
        LOGGER.info(
            "Remapped SQL state [{}] -> [{}] for transient error pattern in statement [{}]",
            originalSqlState,
            remappedSqlState,
            statementId);
      }
      throw new DatabricksSQLException(
          errorMsg, remappedSqlState, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }

    if (statusResp.isSetOperationState()
        && statusResp.getOperationState() == TOperationState.CANCELED_STATE) {
      throw cancelledStatementException(statementId);
    }

    if (statusResp.isSetOperationState() && isErrorOperationState(statusResp.getOperationState())) {
      String serverError = enrichErrorMessage(statusResp.getStatus());
      String errorMsg =
          String.format(
              "Operation failed with error: [%s] for statement [%s], with response [%s]",
              serverError, statementId, statusResp);
      LOGGER.error(errorMsg);

      String sqlState = statusResp.getSqlState();
      if (QUERY_EXECUTION_TIMEOUT_SQLSTATE.equals(sqlState)
          || statusResp.getOperationState() == TOperationState.TIMEDOUT_STATE) {
        throw new DatabricksTimeoutException(
            errorMsg, null, DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR);
      }

      String remappedSqlState = classifyTransientSqlState(serverError, sqlState);
      if (!Objects.equals(remappedSqlState, sqlState)) {
        LOGGER.info(
            "Remapped SQL state [{}] -> [{}] for transient error pattern in statement [{}]",
            sqlState,
            remappedSqlState,
            statementId);
      }
      throw new DatabricksSQLException(
          errorMsg, remappedSqlState, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }
  }

  /**
   * Enriches a null or empty error message from TStatus by including errorCode, errorDetailsJson,
   * and infoMessages. Returns the original errorMessage if it is already present.
   */
  private String enrichErrorMessage(TStatus status) {
    if (status == null) {
      return "no error details from server";
    }
    String errorMessage = status.getErrorMessage();
    if (errorMessage != null && !errorMessage.isEmpty()) {
      return errorMessage;
    }
    StringBuilder detail = new StringBuilder();
    if (status.isSetErrorCode()) {
      detail.append("errorCode=").append(status.getErrorCode());
    }
    if (status.isSetErrorDetailsJson()
        && status.getErrorDetailsJson() != null
        && !status.getErrorDetailsJson().isEmpty()) {
      if (detail.length() > 0) detail.append(", ");
      detail.append("details=").append(status.getErrorDetailsJson());
    }
    if (status.isSetInfoMessages() && status.getInfoMessages() != null) {
      if (detail.length() > 0) detail.append(", ");
      detail.append("infoMessages=").append(status.getInfoMessages());
    }
    return detail.length() > 0 ? detail.toString() : "no error details from server";
  }

  /**
   * Builds a DatabricksSQLException for transport-level failures (e.g. HTTP 502 Bad Gateway) during
   * polling. Uses SQL state 08S01 (communication link failure) so callers can identify retryable
   * errors.
   */
  private DatabricksSQLException buildTransportFailureException(
      String statementId, TTransportException e) {
    String errorMsg =
        String.format(
            "Lost connection to server while polling statement [%s] (%s). "
                + "This is typically a transient error (e.g. HTTP 502 Bad Gateway) "
                + "indicating the cluster was temporarily unavailable. Cause: %s",
            statementId, e.getClass().getSimpleName(), e.getMessage());
    LOGGER.error(errorMsg, e);
    return new DatabricksSQLException(errorMsg, e, COMMUNICATION_LINK_FAILURE_SQLSTATE);
  }

  /** A Thrift RPC that is safe to repeat after a transport-level failure. */
  @FunctionalInterface
  private interface TransportSafeRpc<T> {
    T call() throws TException;
  }

  /** Null-safe operation-handle rendering for log lines (handles may be absent). */
  private static String loggableOperationHandle(TOperationHandle operationHandle) {
    return operationHandle != null
        ? StatementId.loggableStatementId(operationHandle)
        : "unknown";
  }

  /**
   * Executes an idempotent status-poll RPC with no query-timeout budget (used by the Thrift
   * heartbeat / metadata-less status checks). Retries are bounded by {@link
   * #MAX_POLL_TRANSPORT_RETRIES}. See {@link #withTransportRetry(String, String, TimeoutHandler,
   * int, TransportSafeRpc)} for the full contract.
   */
  private <T> T withTransportRetry(String rpcName, String statementId, TransportSafeRpc<T> rpc)
      throws TException {
    return withoutTimeout(rpcName, statementId, MAX_POLL_TRANSPORT_RETRIES, rpc);
  }

  /**
   * Executes a cleanup RPC ({@code CloseOperation} / {@code CancelOperation}) with a deliberately
   * small retry budget ({@link #MAX_CLEANUP_TRANSPORT_RETRIES}). Cleanup runs on close / cancel /
   * timeout paths that must not hang: a single transient blip is worth reconnecting past, but during
   * a sustained outage extra retries only delay shutdown, so the budget is far tighter than the
   * status-poll path.
   */
  private <T> T withCleanupTransportRetry(
      String rpcName, String statementId, TransportSafeRpc<T> rpc) throws TException {
    return withoutTimeout(rpcName, statementId, MAX_CLEANUP_TRANSPORT_RETRIES, rpc);
  }

  /** Shared no-timeout entry point; adapts the deadline-aware core for callers with no deadline. */
  private <T> T withoutTimeout(
      String rpcName, String statementId, int maxRetries, TransportSafeRpc<T> rpc)
      throws TException {
    try {
      return withTransportRetry(
          rpcName, statementId, /* timeoutHandler= */ null, maxRetries, rpc);
    } catch (DatabricksTimeoutException e) {
      // Unreachable: a null timeout handler never enforces a deadline. Guard defensively so the
      // checked timeout type cannot silently widen this method's contract.
      throw new IllegalStateException("Unexpected timeout without an active timeout handler", e);
    }
  }

  /**
   * Deadline-aware status-poll retry with the default poll budget ({@link
   * #MAX_POLL_TRANSPORT_RETRIES}). See {@link #withTransportRetry(String, String, TimeoutHandler,
   * int, TransportSafeRpc)}.
   */
  private <T> T withTransportRetry(
      String rpcName, String statementId, TimeoutHandler timeoutHandler, TransportSafeRpc<T> rpc)
      throws TException, DatabricksTimeoutException {
    return withTransportRetry(
        rpcName, statementId, timeoutHandler, MAX_POLL_TRANSPORT_RETRIES, rpc);
  }

  /**
   * Executes an idempotent Thrift RPC, transparently retrying <em>transient</em> transport-level
   * failures on a fresh connection with jittered exponential backoff.
   *
   * <p>Every invocation of {@code rpc} builds a new transport, so a retry naturally leases a
   * different pooled connection while the broken one is discarded. This lets a still-running
   * server-side operation be re-polled — or a completed one be re-closed / re-cancelled — instead
   * of being abandoned after a single stale-connection blip and left to expire on the server's
   * inactivity timeout. Only RPCs that are safe to repeat may use this path (status polling,
   * operation close, cancel); statement submission must not.
   *
   * <p>Only failures classified as transient by {@link #isRetryableTransportFailure} are retried:
   * genuine connection-level errors (stale pooled connection, reset, socket timeout) and transient
   * HTTP gateway codes ({@link #RETRYABLE_TRANSPORT_HTTP_CODES}). Permanent HTTP errors (401/403/404
   * …) and anything the shared {@link
   * com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler} already retried and
   * exhausted (429/503/custom, which surface with a {@link DatabricksRetryHandlerException} in their
   * cause chain) are rethrown on the first attempt — the latter avoids stacking a second retry
   * storm on top of the HTTP layer's.
   *
   * <p>When {@code timeoutHandler} is non-null the operation's deadline is enforced before each
   * backoff sleep and the sleep is capped to the remaining budget, so a failing RPC cannot overshoot
   * the statement's {@code queryTimeout}. Retries are bounded by {@code maxRetries}; once exhausted
   * the original {@link TTransportException} is rethrown so existing caller-side handling still
   * applies. A thread interrupt during a backoff sleep restores the interrupt flag and aborts the
   * retry loop.
   */
  private <T> T withTransportRetry(
      String rpcName,
      String statementId,
      TimeoutHandler timeoutHandler,
      int maxRetries,
      TransportSafeRpc<T> rpc)
      throws TException, DatabricksTimeoutException {
    int attempt = 0;
    long backoffMillis = TRANSPORT_RETRY_MIN_BACKOFF_MILLIS;
    while (true) {
      try {
        return rpc.call();
      } catch (TTransportException e) {
        if (!isRetryableTransportFailure(e)) {
          // Permanent error (e.g. 401/403/404) or one the HTTP layer already retried (429/503):
          // surface immediately instead of hanging through the backoff schedule.
          throw e;
        }
        if (++attempt > maxRetries) {
          LOGGER.error(
              "Transport failure on {} for statement [{}] still failing after {} retries; giving"
                  + " up. Cause: {}",
              rpcName,
              statementId,
              maxRetries,
              e.getMessage());
          throw e;
        }
        // Enforce the query deadline before sleeping so a failing RPC cannot overshoot the
        // statement's queryTimeout by the backoff schedule (may run the timeout action and throw).
        if (timeoutHandler != null) {
          timeoutHandler.checkTimeout();
        }
        // Full-jitter backoff around the current exponential ceiling, spreading concurrent
        // reconnect attempts so they do not thunder against a recovering endpoint.
        long sleepMillis =
            ThreadLocalRandom.current().nextLong(backoffMillis / 2 + 1, backoffMillis + 1);
        if (timeoutHandler != null) {
          // Cap to the time left before the deadline is actually enforced so we neither overshoot
          // nor collapse to a zero-length (busy-spin) sleep in the sub-second window before it.
          long remainingMillis = timeoutHandler.getRemainingMillis();
          if (remainingMillis < sleepMillis) {
            sleepMillis = Math.max(0L, remainingMillis);
          }
        }
        LOGGER.warn(
            "Transport failure on {} for statement [{}] (attempt {}/{}); reconnecting and retrying"
                + " in {} ms. Cause: {}",
            rpcName,
            statementId,
            attempt,
            maxRetries,
            sleepMillis,
            e.getMessage());
        try {
          backoffSleep(sleepMillis);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
        backoffMillis = Math.min(backoffMillis * 2, TRANSPORT_RETRY_MAX_BACKOFF_MILLIS);
      }
    }
  }

  /**
   * Classifies a transport failure as transient (safe to retry on a fresh connection) or not.
   *
   * <p>Because the Thrift transport routes through {@link
   * com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient}, every failure arrives wrapped as a
   * {@link TTransportException} whose cause is normally a {@link DatabricksHttpException}. The
   * decision:
   *
   * <ul>
   *   <li>Any {@link DatabricksRetryHandlerException} in the cause chain → <b>not</b> retryable: the
   *       HTTP layer already retried and exhausted this (429/503/custom); retrying again would
   *       amplify load.
   *   <li>{@link DatabricksHttpException} carrying a concrete HTTP status → retryable only if the
   *       status is a transient gateway code ({@link #RETRYABLE_TRANSPORT_HTTP_CODES}); permanent
   *       statuses (401/403/404 …) are not.
   *   <li>{@link DatabricksHttpException} with no status (status {@code 0}) → a genuine
   *       connection-level failure (stale pooled connection, reset, socket timeout) surfaced as an
   *       {@link IOException} cause; retryable.
   *   <li>A direct {@link IOException} cause (e.g. a response-body read error) → retryable.
   *   <li>Anything else (unknown or absent cause) → not retryable.
   * </ul>
   */
  private static boolean isRetryableTransportFailure(TTransportException e) {
    Throwable cause = e.getCause();
    if (chainContains(cause, DatabricksRetryHandlerException.class)) {
      return false;
    }
    if (cause instanceof DatabricksHttpException) {
      int statusCode = ((DatabricksHttpException) cause).getStatusCode();
      if (statusCode != 0) {
        return RETRYABLE_TRANSPORT_HTTP_CODES.contains(statusCode);
      }
      // No HTTP response was received: retry only when a real connection-level IOException is the
      // underlying cause (guards against unrelated status-less DatabricksHttpExceptions).
      return chainContains(cause.getCause(), IOException.class);
    }
    return cause instanceof IOException;
  }

  /** Sleeps for the transport-retry backoff. Extracted as a test seam to keep retry tests fast. */
  @VisibleForTesting
  void backoffSleep(long millis) throws InterruptedException {
    TimeUnit.MILLISECONDS.sleep(millis);
  }

  /** Returns true if {@code throwable} or any exception in its cause chain is of {@code type}. */
  private static boolean chainContains(Throwable throwable, Class<? extends Throwable> type) {
    for (Throwable current = throwable; current != null; current = current.getCause()) {
      if (type.isInstance(current)) {
        return true;
      }
    }
    return false;
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

  private DatabricksSQLException cancelledStatementException(String statementId) {
    String msg = String.format("Statement [%s] was cancelled", statementId);
    LOGGER.info(msg);
    // silentExceptions=true: cancellations are common in BI tools and should not
    // emit ERROR-level telemetry
    return new DatabricksSQLException(
        msg,
        OPERATION_CANCELLED_SQLSTATE,
        DatabricksDriverErrorCode.EXECUTE_STATEMENT_CANCELLED,
        true);
  }

  private boolean isErrorStatusCode(TStatus status) {
    if (status == null || !status.isSetStatusCode()) {
      LOGGER.error("Status code is not set, marking the response as failed");
      return true;
    }
    TStatusCode statusCode = status.getStatusCode();
    return statusCode == TStatusCode.ERROR_STATUS
        || statusCode == TStatusCode.INVALID_HANDLE_STATUS;
  }

  private boolean isErrorOperationState(TOperationState state) {
    return state == TOperationState.ERROR_STATE
        || state == TOperationState.CLOSED_STATE
        || state == TOperationState.TIMEDOUT_STATE;
  }

  private boolean isPendingOperationState(TOperationState state) {
    return state == TOperationState.RUNNING_STATE || state == TOperationState.PENDING_STATE;
  }

  void setServerProtocolVersion(TProtocolVersion protocolVersion) {
    serverProtocolVersion = protocolVersion;
  }

  private TimeoutHandler getTimeoutHandler(
      TExecuteStatementResp response,
      int timeoutInSeconds,
      DatabricksDriverErrorCode internalErrorCode) {
    final TOperationHandle operationHandle = response.getOperationHandle();

    return new TimeoutHandler(
        timeoutInSeconds,
        "Thrift Operation Handle: " + operationHandle.toString(),
        () -> {
          try {
            LOGGER.debug("Canceling operation due to timeout: {}", operationHandle);
            cancelOperation(new TCancelOperationReq().setOperationHandle(operationHandle));
          } catch (Exception e) {
            LOGGER.warn("Failed to cancel operation on timeout: {}", e.getMessage());
          }
        },
        internalErrorCode);
  }

  /**
   * Gets the operation status for the given statement. Package-visible to allow heartbeat polling
   * from {@link DatabricksThriftServiceClient#checkStatementAlive}.
   */
  TGetOperationStatusResp getOperationStatus(
      TGetOperationStatusReq statusReq, StatementId statementId) throws TException {
    long operationStatusStartTime = System.nanoTime();
    TGetOperationStatusResp operationStatus =
        withTransportRetry(
            "GetOperationStatus",
            statementId.toSQLExecStatementId(),
            () -> getThriftClient().GetOperationStatus(statusReq));
    return recordOperationStatusLatency(statementId, operationStatusStartTime, operationStatus);
  }

  /**
   * Timeout-aware variant used by the execution polling loop: the retry backoff is bounded by the
   * statement's {@code queryTimeout} via {@code timeoutHandler} so a transient transport failure
   * cannot overshoot the deadline.
   */
  TGetOperationStatusResp getOperationStatus(
      TGetOperationStatusReq statusReq, StatementId statementId, TimeoutHandler timeoutHandler)
      throws TException, DatabricksTimeoutException {
    long operationStatusStartTime = System.nanoTime();
    TGetOperationStatusResp operationStatus =
        withTransportRetry(
            "GetOperationStatus",
            statementId.toSQLExecStatementId(),
            timeoutHandler,
            () -> getThriftClient().GetOperationStatus(statusReq));
    return recordOperationStatusLatency(statementId, operationStatusStartTime, operationStatus);
  }

  private TGetOperationStatusResp recordOperationStatusLatency(
      StatementId statementId, long startTimeNanos, TGetOperationStatusResp operationStatus) {
    long operationStatusLatencyMillis = (System.nanoTime() - startTimeNanos) / 1_000_000;
    LOGGER.debug(
        "Statement [{}] Thrift operation status latency: {}ms",
        statementId,
        operationStatusLatencyMillis);
    TelemetryHelper.recordGetOperationStatus(
        connectionContext, statementId.toSQLExecStatementId(), operationStatusLatencyMillis);
    return operationStatus;
  }
}
