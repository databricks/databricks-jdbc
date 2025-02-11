package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.TEMPORARY_REDIRECT_STATUS_CODE;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_ROW_LIMIT;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.TEMPORARY_REDIRECT_EXCEPTION;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.client.sqlexec.GetStatementResponse;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.service.sql.*;
import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/** Implementation of IDatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements IDatabricksClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksSdkClient.class);
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private final IDatabricksConnectionContext connectionContext;
  private final ClientConfigurator clientConfigurator;
  private volatile WorkspaceClient workspaceClient;

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.clientConfigurator = new ClientConfigurator(connectionContext);
    this.workspaceClient = clientConfigurator.getWorkspaceClient();
  }

  @VisibleForTesting
  public DatabricksSdkClient(
      IDatabricksConnectionContext connectionContext,
      StatementExecutionService statementExecutionService,
      ApiClient apiClient)
      throws DatabricksParsingException {
    this.connectionContext = connectionContext;
    this.clientConfigurator = new ClientConfigurator(connectionContext);
    this.workspaceClient =
        new WorkspaceClient(true /* mock */, apiClient)
            .withStatementExecutionImpl(statementExecutionService);
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  @Override
  public ImmutableSessionInfo createSession(
      IDatabricksComputeResource warehouse,
      String catalog,
      String schema,
      Map<String, String> sessionConf)
      throws DatabricksTemporaryRedirectException {
    // TODO (PECO-1460): Handle sessionConf in public session API
    LOGGER.debug(
        String.format(
            "public Session createSession(String warehouseId = {%s}, String catalog = {%s}, String schema = {%s}, Map<String, String> sessionConf = {%s})",
            ((Warehouse) warehouse).getWarehouseId(), catalog, schema, sessionConf));
    CreateSessionRequest request =
        new CreateSessionRequest().setWarehouseId(((Warehouse) warehouse).getWarehouseId());
    if (catalog != null) {
      request.setCatalog(catalog);
    }
    if (schema != null) {
      request.setSchema(schema);
    }
    if (sessionConf != null && !sessionConf.isEmpty()) {
      request.setSessionConfigs(sessionConf);
    }
    CreateSessionResponse createSessionResponse = null;
    try {
      createSessionResponse =
          workspaceClient
              .apiClient()
              .POST(SESSION_PATH, request, CreateSessionResponse.class, JSON_HTTP_HEADERS);
    } catch (DatabricksError e) {
      if (e.getStatusCode() == TEMPORARY_REDIRECT_STATUS_CODE) {
        throw new DatabricksTemporaryRedirectException(TEMPORARY_REDIRECT_EXCEPTION);
      }
    }

    return ImmutableSessionInfo.builder()
        .computeResource(warehouse)
        .sessionId(createSessionResponse.getSessionId())
        .build();
  }

  @Override
  public void deleteSession(ImmutableSessionInfo sessionInfo) {
    LOGGER.debug(
        String.format(
            "public void deleteSession(String sessionId = {%s})", sessionInfo.sessionId()));
    DeleteSessionRequest request =
        new DeleteSessionRequest()
            .setSessionId(sessionInfo.sessionId())
            .setWarehouseId(((Warehouse) sessionInfo.computeResource()).getWarehouseId());
    String path = String.format(SESSION_PATH_WITH_ID, request.getSessionId());
    workspaceClient.apiClient().DELETE(path, request, Void.class, JSON_HTTP_HEADERS);
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    LOGGER.debug(
        String.format(
            "public DatabricksResultSet executeStatement(String sql = {%s}, compute resource = {%s}, Map<Integer, ImmutableSqlParameter> parameters, StatementType statementType = {%s}, IDatabricksSession session)",
            sql, computeResource.toString(), statementType));
    long pollCount = 0;
    long executionStartTime = Instant.now().toEpochMilli();
    DatabricksThreadContextHolder.setStatementType(statementType);
    ExecuteStatementRequest request =
        getRequest(
            statementType,
            sql,
            ((Warehouse) computeResource).getWarehouseId(),
            session,
            parameters,
            parentStatement,
            false);
    ExecuteStatementResponse response =
        workspaceClient
            .apiClient()
            .POST(STATEMENT_PATH, request, ExecuteStatementResponse.class, JSON_HTTP_HEADERS);
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error(
          "Empty Statement ID for sql %s, statementType %s, compute %s",
          sql, statementType, computeResource);
      handleFailedExecution(response, "", sql);
    }
    LOGGER.debug(
        String.format(
            "Executing sql %s, statementType %s, compute %s, StatementID %s",
            sql, statementType, computeResource, statementId));
    StatementId typedStatementId = new StatementId(statementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }
    StatementState responseState = response.getStatus().getState();
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      if (pollCount > 0) { // First poll happens without a delay
        try {
          Thread.sleep(connectionContext.getAsyncExecPollInterval());
        } catch (InterruptedException e) {
          String timeoutErrorMessage =
              String.format(
                  "Thread interrupted due to statement timeout. StatementID %s", statementId);
          LOGGER.error(timeoutErrorMessage);
          throw new DatabricksTimeoutException(timeoutErrorMessage);
        }
      }
      String getStatusPath = String.format(STATEMENT_PATH_WITH_ID, statementId);
      response =
          wrapGetStatementResponse(
              workspaceClient
                  .apiClient()
                  .GET(getStatusPath, request, GetStatementResponse.class, JSON_HTTP_HEADERS));
      responseState = response.getStatus().getState();
      LOGGER.debug(
          String.format(
              "Executed sql [%s] with status [%s] with retry count [%d]",
              sql, responseState, pollCount));
      pollCount++;
    }
    long executionEndTime = Instant.now().toEpochMilli();
    LOGGER.debug(
        String.format(
            "Executed sql [%s] with status [%s], total time taken [%s] and pollCount [%s]",
            sql, responseState, (executionEndTime - executionStartTime), pollCount));
    if (responseState != StatementState.SUCCEEDED) {
      handleFailedExecution(response, statementId, sql);
    }
    return new DatabricksResultSet(
        response.getStatus(),
        typedStatementId,
        response.getResult(),
        response.getManifest(),
        statementType,
        session,
        parentStatement);
  }

  @Override
  public DatabricksResultSet executeStatementAsync(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    LOGGER.debug(
        "public DatabricksResultSet executeStatementAsync(String sql = {%s}, compute resource = {%s}, Map<Integer, ImmutableSqlParameter> parameters, IDatabricksSession session)",
        sql, computeResource.toString());
    ExecuteStatementRequest request =
        getRequest(
            StatementType.SQL,
            sql,
            ((Warehouse) computeResource).getWarehouseId(),
            session,
            parameters,
            parentStatement,
            true);
    ExecuteStatementResponse response =
        workspaceClient
            .apiClient()
            .POST(STATEMENT_PATH, request, ExecuteStatementResponse.class, JSON_HTTP_HEADERS);
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error("Empty Statement ID for sql %s, compute %s", sql, computeResource.toString());
      handleFailedExecution(response, "", sql);
    }
    StatementId typedStatementId = new StatementId(statementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }
    LOGGER.debug("Executed sql [%s] with status [%s]", sql, response.getStatus().getState());

    return new DatabricksResultSet(
        response.getStatus(),
        typedStatementId,
        response.getResult(),
        response.getManifest(),
        StatementType.SQL,
        session,
        parentStatement);
  }

  @Override
  public DatabricksResultSet getStatementResult(
      StatementId typedStatementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    String statementId = typedStatementId.toSQLExecStatementId();
    GetStatementRequest request = new GetStatementRequest().setStatementId(statementId);
    String getStatusPath = String.format(STATEMENT_PATH_WITH_ID, statementId);
    GetStatementResponse response =
        workspaceClient
            .apiClient()
            .GET(getStatusPath, request, GetStatementResponse.class, JSON_HTTP_HEADERS);
    return new DatabricksResultSet(
        response.getStatus(),
        typedStatementId,
        response.getResult(),
        response.getManifest(),
        StatementType.SQL,
        session,
        parentStatement);
  }

  @Override
  public void closeStatement(StatementId typedStatementId) {
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        String.format("public void closeStatement(String statementId = {%s})", statementId));
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format(STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().DELETE(path, request, Void.class, JSON_HTTP_HEADERS);
  }

  @Override
  public void cancelStatement(StatementId typedStatementId) {
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        String.format("public void cancelStatement(String statementId = {%s})", statementId));
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(statementId);
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, request.getStatementId());
    workspaceClient.apiClient().POST(path, request, Void.class, JSON_HTTP_HEADERS);
  }

  @Override
  public Collection<ExternalLink> getResultChunks(StatementId typedStatementId, long chunkIndex) {
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        String.format(
            "public Optional<ExternalLink> getResultChunk(String statementId = {%s}, long chunkIndex = {%s})",
            statementId, chunkIndex));
    GetStatementResultChunkNRequest request =
        new GetStatementResultChunkNRequest().setStatementId(statementId).setChunkIndex(chunkIndex);
    String path = String.format(RESULT_CHUNK_PATH, statementId, chunkIndex);
    return workspaceClient
        .apiClient()
        .GET(path, request, ResultData.class, JSON_HTTP_HEADERS)
        .getExternalLinks();
  }

  @Override
  public synchronized void resetAccessToken(String newAccessToken) {
    this.clientConfigurator.resetAccessTokenInConfig(newAccessToken);
    this.workspaceClient = clientConfigurator.getWorkspaceClient();
  }

  @Override
  public TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    throw new DatabricksValidationException("Get more results cannot be called for SEA flow");
  }

  private boolean useCloudFetchForResult(StatementType statementType) {
    return this.connectionContext.shouldEnableArrow()
        && (statementType == StatementType.QUERY
            || statementType == StatementType.SQL
            || statementType == StatementType.METADATA);
  }

  private ExecuteStatementRequest getRequest(
      StatementType statementType,
      String sql,
      String warehouseId,
      IDatabricksSession session,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksStatementInternal parentStatement,
      boolean executeAsync)
      throws SQLException {
    Format format = useCloudFetchForResult(statementType) ? Format.ARROW_STREAM : Format.JSON_ARRAY;
    Disposition defaultDisposition =
        connectionContext.isSqlExecHybridResultsEnabled()
            ? Disposition.INLINE_OR_EXTERNAL_LINKS
            : Disposition.EXTERNAL_LINKS;
    Disposition disposition =
        useCloudFetchForResult(statementType) ? defaultDisposition : Disposition.INLINE;
    long maxRows = (parentStatement == null) ? DEFAULT_ROW_LIMIT : parentStatement.getMaxRows();
    CompressionCodec compressionCodec = session.getCompressionCodec();
    if (disposition.equals(Disposition.INLINE)) {
      // TODO: Evaluate if inline results need compression based on performance.
      LOGGER.debug("Results are inline, skipping compression.");
      compressionCodec = CompressionCodec.NONE;
    }
    List<StatementParameterListItem> parameterListItems =
        parameters.values().stream().map(this::mapToParameterListItem).collect(Collectors.toList());
    ExecuteStatementRequest request =
        new ExecuteStatementRequest()
            .setSessionId(session.getSessionId())
            .setStatement(sql)
            .setWarehouseId(warehouseId)
            .setDisposition(disposition)
            .setFormat(format)
            .setResultCompression(compressionCodec)
            .setParameters(parameterListItems);
    if (executeAsync) {
      request.setWaitTimeout(ASYNC_TIMEOUT_VALUE);
    } else {
      request
          .setWaitTimeout(SYNC_TIMEOUT_VALUE)
          .setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    if (maxRows != DEFAULT_ROW_LIMIT) {
      request.setRowLimit(maxRows);
    }
    return request;
  }

  private StatementParameterListItem mapToParameterListItem(ImmutableSqlParameter parameter) {
    return new PositionalStatementParameterListItem()
        .setOrdinal(parameter.cardinal())
        .setType(parameter.type().name())
        .setValue(parameter.value() != null ? parameter.value().toString() : null);
  }

  /** Handles a failed execution and throws appropriate exception */
  void handleFailedExecution(
      ExecuteStatementResponse response, String statementId, String statement) throws SQLException {
    StatementState statementState = response.getStatus().getState();
    ServiceError error = response.getStatus().getError();
    String errorMessage =
        String.format(
            "Statement execution failed %s -> %s\n%s.", statementId, statement, statementState);
    if (error != null) {
      errorMessage +=
          String.format(
              " Error Message: %s, Error code: %s", error.getMessage(), error.getErrorCode());
    }
    LOGGER.debug(errorMessage);
    throw new DatabricksSQLException(
        errorMessage, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
  }

  private ExecuteStatementResponse wrapGetStatementResponse(
      GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse()
        .setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }
}
