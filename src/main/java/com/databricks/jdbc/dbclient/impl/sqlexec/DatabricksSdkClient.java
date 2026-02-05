package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_EXECUTION_TIMEOUT_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.TEMPORARY_REDIRECT_STATUS_CODE;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.DECIMAL;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.getDecimalTypeString;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;
import static com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode.TEMPORARY_REDIRECT_EXCEPTION;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.common.TimeoutHandler;
import com.databricks.jdbc.dbclient.impl.common.TracingUtil;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.sqlexec.*;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementResponse;
import com.databricks.jdbc.model.client.sqlexec.GetStatementResponse;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.service.sql.*;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.SQLException;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpStatus;

/** Implementation of IDatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements IDatabricksClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksSdkClient.class);
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private final IDatabricksConnectionContext connectionContext;
  private final ClientConfigurator clientConfigurator;
  private volatile WorkspaceClient workspaceClient;
  private volatile ApiClient apiClient;

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException, DatabricksHttpException {
    this.connectionContext = connectionContext;
    this.clientConfigurator =
        DatabricksClientConfiguratorManager.getInstance().getConfigurator(connectionContext);
    this.workspaceClient = clientConfigurator.getWorkspaceClient();
    this.apiClient = workspaceClient.apiClient();
  }

  @VisibleForTesting
  public DatabricksSdkClient(
      IDatabricksConnectionContext connectionContext,
      StatementExecutionService statementExecutionService,
      ApiClient apiClient)
      throws DatabricksParsingException, DatabricksHttpException {
    this.connectionContext = connectionContext;
    this.clientConfigurator =
        DatabricksClientConfiguratorManager.getInstance().getConfigurator(connectionContext);
    this.workspaceClient =
        new WorkspaceClient(true /* mock */, apiClient)
            .withStatementExecutionImpl(statementExecutionService);
    this.apiClient = apiClient;
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
      throws DatabricksSQLException {
    // TODO (PECO-1460): Handle sessionConf in public session API
    LOGGER.debug(
        "public Session createSession(String warehouseId = {}, String catalog = {}, String schema = {}, Map<String, String> sessionConf = {})",
        ((Warehouse) warehouse).getWarehouseId(),
        catalog,
        schema,
        sessionConf);
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
      Request req = new Request(Request.POST, SESSION_PATH, apiClient.serialize(request));
      req.withHeaders(getHeaders("createSession"));
      createSessionResponse = apiClient.execute(req, CreateSessionResponse.class);
    } catch (DatabricksError e) {
      if (e.getStatusCode() == TEMPORARY_REDIRECT_STATUS_CODE) {
        throw new DatabricksTemporaryRedirectException(TEMPORARY_REDIRECT_EXCEPTION);
      }
      if (e.getStatusCode() == HttpStatus.SC_TOO_MANY_REQUESTS) {
        String errorMessage =
            String.format(
                "createSession failed with HTTP %d (rate limit exceeded) after retries. "
                    + "Warehouse id: %s, Error: %s",
                HttpStatus.SC_TOO_MANY_REQUESTS,
                ((Warehouse) warehouse).getWarehouseId(),
                e.getMessage());
        LOGGER.warn(errorMessage, e);
        throw new DatabricksRateLimitException(errorMessage, e, HttpStatus.SC_TOO_MANY_REQUESTS);
      }
      String errorReason = buildErrorMessage(e);
      throw new DatabricksSQLException(errorReason, e, DatabricksDriverErrorCode.CONNECTION_ERROR);
    } catch (IOException e) {
      String errorMessage = "Error while processing the request via the sdk client";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    DatabricksThreadContextHolder.setSessionId(createSessionResponse.getSessionId());
    return ImmutableSessionInfo.builder()
        .computeResource(warehouse)
        .sessionId(createSessionResponse.getSessionId())
        .build();
  }

  @Override
  public void deleteSession(ImmutableSessionInfo sessionInfo) throws DatabricksSQLException {
    LOGGER.debug("public void deleteSession(String sessionId = {})", sessionInfo.sessionId());
    DatabricksThreadContextHolder.setSessionId(sessionInfo.sessionId());
    DeleteSessionRequest request =
        new DeleteSessionRequest()
            .setSessionId(sessionInfo.sessionId())
            .setWarehouseId(((Warehouse) sessionInfo.computeResource()).getWarehouseId());
    String path = String.format(SESSION_PATH_WITH_ID, request.getSessionId());
    try {
      Request req = new Request(Request.DELETE, path);
      req.withHeaders(getHeaders("deleteSession"));
      ApiClient.setQuery(req, request);
      apiClient.execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while performing the deleting session operation";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
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
        "public DatabricksResultSet executeStatement(String sql = {}, compute resource = {}, Map<Integer, ImmutableSqlParameter> parameters = {}, StatementType statementType = {}, IDatabricksSession session = {}, parentStatement = {})",
        sql,
        computeResource.toString(),
        parameters,
        statementType,
        session,
        parentStatement);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
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
    ExecuteStatementResponse response;
    try {
      Request req = new Request(Request.POST, STATEMENT_PATH, apiClient.serialize(request));
      req.withHeaders(getHeaders("executeStatement", statementType, false));
      response = apiClient.execute(req, ExecuteStatementResponse.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the execute statement request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error(
          "Empty Statement ID for sql {}, statementType {}, compute {}",
          sql,
          statementType,
          computeResource);
      handleFailedExecution(response, "", sql);
    }
    LOGGER.debug(
        "Executing sql = {}, statementType = {}, compute = {}, StatementID = {}",
        sql,
        statementType,
        computeResource,
        statementId);
    StatementId typedStatementId = new StatementId(statementId);
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }

    int timeoutInSeconds =
        parentStatement != null ? parentStatement.getStatement().getQueryTimeout() : 0;

    // Create timeout handler
    TimeoutHandler timeoutHandler =
        TimeoutHandler.forStatement(
            timeoutInSeconds,
            typedStatementId,
            this,
            DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);

    StatementState responseState = response.getStatus().getState();
    while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
      // Check for timeout
      timeoutHandler.checkTimeout();

      if (pollCount > 0) { // First poll happens without a delay
        try {
          Thread.sleep(connectionContext.getAsyncExecPollInterval());
        } catch (InterruptedException e) {
          String timeoutErrorMessage =
              String.format(
                  "Thread interrupted due to statement timeout. StatementID %s", statementId);
          LOGGER.error(timeoutErrorMessage);
          throw new DatabricksTimeoutException(
              timeoutErrorMessage, null, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
        }
      }
      String getStatusPath = String.format(STATEMENT_PATH_WITH_ID, statementId);
      try {
        Request req = new Request(Request.GET, getStatusPath, apiClient.serialize(request));
        req.withHeaders(getHeaders("getStatement"));
        response = wrapGetStatementResponse(apiClient.execute(req, GetStatementResponse.class));
      } catch (IOException e) {
        String errorMessage = "Error while processing the get statement response";
        LOGGER.error(errorMessage, e);
        throw new DatabricksSQLException(
            errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      responseState = response.getStatus().getState();
      LOGGER.debug(
          "Executed sql {} with status {} with retry count {}", sql, responseState, pollCount);
      pollCount++;
    }
    long executionEndTime = Instant.now().toEpochMilli();
    LOGGER.debug(
        "Executed sql {} with status {}, total time taken {} and pollCount {}",
        sql,
        responseState,
        (executionEndTime - executionStartTime),
        pollCount);
    if (responseState != StatementState.SUCCEEDED && responseState != StatementState.CLOSED) {
      handleFailedExecution(response, statementId, sql);
    }

    if (responseState == StatementState.CLOSED && parentStatement != null) {
      LOGGER.debug("Statement {} returned CLOSED status, marking statement as closed", statementId);
      ((DatabricksStatement) parentStatement.getStatement()).markAsClosed();
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
        "public DatabricksResultSet executeStatementAsync(String sql = {}, compute resource = {}, Map<Integer, ImmutableSqlParameter> parameters, IDatabricksSession session = {}, IDatabricksStatementInternal parentStatement = {})",
        sql,
        computeResource.toString(),
        session,
        parentStatement);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
    StatementType statementType = StatementType.SQL;
    ExecuteStatementRequest request =
        getRequest(
            statementType,
            sql,
            ((Warehouse) computeResource).getWarehouseId(),
            session,
            parameters,
            parentStatement,
            true);
    ExecuteStatementResponse response;
    try {
      Request req = new Request(Request.POST, STATEMENT_PATH, apiClient.serialize(request));
      req.withHeaders(getHeaders("executeStatement", statementType, true));
      response = apiClient.execute(req, ExecuteStatementResponse.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the execute statement async request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error("Empty Statement ID for sql {}, compute {}", sql, computeResource.toString());
      handleFailedExecution(response, "", sql);
    }
    StatementId typedStatementId = new StatementId(statementId);
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }
    LOGGER.debug("Executed sql [{}] with status [{}]", sql, response.getStatus().getState());

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
  public DatabricksResultSet getStatementResult(
      StatementId typedStatementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
    String statementId = typedStatementId.toSQLExecStatementId();
    GetStatementRequest request = new GetStatementRequest().setStatementId(statementId);
    String getStatusPath = String.format(STATEMENT_PATH_WITH_ID, statementId);
    GetStatementResponse response;
    try {
      Request req = new Request(Request.GET, getStatusPath, apiClient.serialize(request));
      req.withHeaders(getHeaders("getStatement"));
      response = apiClient.execute(req, GetStatementResponse.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the get statement result request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
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
  public void closeStatement(StatementId typedStatementId) throws DatabricksSQLException {
    String statementId = typedStatementId.toSQLExecStatementId();
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    LOGGER.debug(String.format("public void closeStatement(String statementId = {})", statementId));
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = String.format(STATEMENT_PATH_WITH_ID, request.getStatementId());
    try {
      Request req = new Request(Request.DELETE, path, apiClient.serialize(request));
      req.withHeaders(getHeaders("closeStatement"));
      apiClient.execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the close statement request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public void cancelStatement(StatementId typedStatementId) throws DatabricksSQLException {
    String statementId = typedStatementId.toSQLExecStatementId();
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    LOGGER.debug("public void cancelStatement(String statementId = {})", statementId);
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(statementId);
    String path = String.format(CANCEL_STATEMENT_PATH_WITH_ID, request.getStatementId());
    try {
      Request req = new Request(Request.POST, path, apiClient.serialize(request));
      req.withHeaders(getHeaders("cancelStatement"));
      apiClient.execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the cancel statement request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public ChunkLinkFetchResult getResultChunks(
      StatementId typedStatementId, long chunkIndex, long chunkStartRowOffset)
      throws DatabricksSQLException {
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        "getResultChunks(statementId={}, chunkIndex={}) using SEA client", statementId, chunkIndex);
    GetStatementResultChunkNRequest request =
        new GetStatementResultChunkNRequest().setStatementId(statementId).setChunkIndex(chunkIndex);
    String path = String.format(RESULT_CHUNK_PATH, statementId, chunkIndex);
    try {
      Request req = new Request(Request.GET, path, apiClient.serialize(request));
      req.withHeaders(getHeaders("getStatementResultN"));
      ResultData resultData = apiClient.execute(req, ResultData.class);
      return buildChunkLinkFetchResult(resultData.getExternalLinks());
    } catch (IOException e) {
      String errorMessage = "Error while processing the get result chunk request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  /**
   * Builds a ChunkLinkFetchResult from SEA external links.
   *
   * @param links The external links from the SEA response
   * @return ChunkLinkFetchResult with links and continuation info
   */
  private ChunkLinkFetchResult buildChunkLinkFetchResult(Collection<ExternalLink> links) {
    if (links == null || links.isEmpty()) {
      return ChunkLinkFetchResult.endOfStream();
    }

    List<ExternalLink> linkList =
        links instanceof List ? (List<ExternalLink>) links : new ArrayList<>(links);

    // Derive continuation info from last link
    ExternalLink lastLink = linkList.get(linkList.size() - 1);
    boolean hasMore = lastLink.getNextChunkIndex() != null;
    long nextFetchIndex = hasMore ? lastLink.getNextChunkIndex() : -1;
    long nextRowOffset = lastLink.getRowOffset() + lastLink.getRowCount();

    LOGGER.debug(
        "Built ChunkLinkFetchResult with {} links, hasMore={}, nextFetchIndex={}, nextRowOffset={}",
        linkList.size(),
        hasMore,
        nextFetchIndex,
        nextRowOffset);

    return ChunkLinkFetchResult.of(linkList, hasMore, nextFetchIndex, nextRowOffset);
  }

  @Override
  public ResultData getResultChunksData(StatementId typedStatementId, long chunkIndex)
      throws DatabricksSQLException {
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        "public ResultData getResultChunksData(String statementId = {}, long chunkIndex = {})",
        statementId,
        chunkIndex);
    GetStatementResultChunkNRequest request =
        new GetStatementResultChunkNRequest().setStatementId(statementId).setChunkIndex(chunkIndex);
    String path = String.format(RESULT_CHUNK_PATH, statementId, chunkIndex);
    try {
      Request req = new Request(Request.GET, path, apiClient.serialize(request));
      req.withHeaders(getHeaders("getStatementResultN"));
      return apiClient.execute(req, ResultData.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the get result chunk request";
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public synchronized void resetAccessToken(String newAccessToken) {
    this.clientConfigurator.resetAccessTokenInConfig(newAccessToken);
    this.workspaceClient = clientConfigurator.getWorkspaceClient();
    this.apiClient = workspaceClient.apiClient();
  }

  @Override
  public TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException {
    throw new DatabricksValidationException("Get more results cannot be called for SEA flow");
  }

  @Override
  public DatabricksConfig getDatabricksConfig() {
    return clientConfigurator.getDatabricksConfig();
  }

  private boolean useCloudFetchForResult(StatementType statementType) {
    return this.connectionContext.shouldEnableArrow()
        && (statementType == StatementType.QUERY
            || statementType == StatementType.SQL
            || statementType == StatementType.METADATA);
  }

  private Map<String, String> getHeaders(String method) {
    return getHeaders(method, null, false);
  }

  private Map<String, String> getHeaders(
      String method, StatementType statementType, boolean isAsync) {
    Map<String, String> headers = new HashMap<>(JSON_HTTP_HEADERS);
    if (connectionContext.isRequestTracingEnabled()) {
      String traceHeader = TracingUtil.getTraceHeader();
      LOGGER.debug("Tracing header for method {}: [{}]", method, traceHeader);
      headers.put(TracingUtil.TRACE_HEADER, traceHeader);
    }

    // Add SEA sync metadata header when appropriate
    if (shouldAddSeaSyncMetadataHeader(statementType, isAsync)) {
      headers.put("x-databricks-sea-can-run-fully-sync", "true");
      LOGGER.debug(
          "Adding x-databricks-sea-can-run-fully-sync header for synchronous metadata request");
    }

    // Overriding with URL defined headers
    headers.putAll(this.connectionContext.getCustomHeaders());
    return headers;
  }

  /**
   * Determines whether the x-databricks-sea-can-run-fully-sync header should be added to the
   * request.
   *
   * <p>This header is only added when all of the following conditions are met:
   *
   * <ul>
   *   <li>The EnableSeaSyncMetadata URL parameter is enabled (default: true)
   *   <li>The statement type is METADATA
   *   <li>The execution mode is synchronous (not async)
   * </ul>
   *
   * <p>The header signals to the server that the metadata operation can be executed fully
   * synchronously in SEA mode, enabling optimized execution paths.
   *
   * <p>Note: This client is only used for SEA mode, so no client type check is needed.
   *
   * @param statementType the type of statement being executed (e.g., METADATA, QUERY, SQL)
   * @param isAsync true if the statement is being executed asynchronously, false for synchronous
   *     execution
   * @return true if the header should be added, false otherwise
   */
  private boolean shouldAddSeaSyncMetadataHeader(StatementType statementType, boolean isAsync) {
    return connectionContext.isSeaSyncMetadataEnabled()
        && statementType == StatementType.METADATA
        && !isAsync;
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
    long maxRows =
        (parentStatement == null) ? DEFAULT_RESULT_ROW_LIMIT : parentStatement.getMaxRows();
    CompressionCodec compressionCodec = session.getCompressionCodec();
    if (disposition.equals(Disposition.INLINE)) {
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
      // Only set timeout if direct results mode is not enabled
      if (!connectionContext.isSqlExecDirectResultsEnabled()) {
        request.setWaitTimeout(SYNC_TIMEOUT_VALUE);
      }
      request.setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    if (maxRows > 0) {
      request.setRowLimit(maxRows);
    }
    return request;
  }

  @VisibleForTesting
  StatementParameterListItem mapToParameterListItem(ImmutableSqlParameter parameter) {
    Object value = parameter.value();
    String typeString = parameter.type().name();
    if (typeString.equals(DECIMAL) && value instanceof BigDecimal) {
      typeString = getDecimalTypeString((BigDecimal) value);
    }
    return new PositionalStatementParameterListItem()
        .setOrdinal(parameter.cardinal())
        .setType(typeString)
        .setValue(value != null ? value.toString() : null);
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

    String sqlState = response.getStatus().getSqlState();
    if (QUERY_EXECUTION_TIMEOUT_SQLSTATE.equals(sqlState)) {
      throw new DatabricksTimeoutException(
          errorMessage, null, DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR);
    }
    throw new DatabricksSQLException(
        errorMessage, sqlState, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
  }

  private ExecuteStatementResponse wrapGetStatementResponse(
      GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse()
        .setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }

  /**
   * Builds actionable error messages for SSL handshake failures. Returns a generic message if the
   * error is not SSL-related.
   */
  private String buildErrorMessage(DatabricksError e) {

    boolean isSSLException =
        Stream.iterate(e.getCause(), Objects::nonNull, Throwable::getCause)
            .anyMatch(cause -> cause instanceof SSLHandshakeException);

    boolean isCertificatePathError =
        e.getMessage().contains("PKIX path building failed")
            || e.getMessage().contains("unable to find valid certification path");

    if (isSSLException && isCertificatePathError) {
      return buildSSLCertificatePathErrorMessage(e);
    }

    return "Error while establishing a connection in databricks";
  }

  /** Builds the SSL certificate path error message with actionable steps. */
  private String buildSSLCertificatePathErrorMessage(DatabricksError e) {

    String customTruststorePathMessage = "";
    if (connectionContext != null && connectionContext.getSSLTrustStore() != null) {
      customTruststorePathMessage = " in truststore: " + connectionContext.getSSLTrustStore();
    }

    // Get the actual workspace hostname for the openssl command
    // by removing protocol and port from host url
    String workspaceHostname = "<workspace>";
    try {
      if (connectionContext != null && connectionContext.getHostUrl() != null) {
        workspaceHostname = new URL(connectionContext.getHostUrl()).getHost();
      }
    } catch (Exception ex) {
      LOGGER.debug("Could not retrieve workspace hostname for error message", ex);
    }

    return String.format(
        "Unable to find certification path to requested target%s\n\n"
            + "SSL Error: %s\n\n"
            + "Details: TLS handshake failure due to TLS Certificate of server being connected is not in the configured truststore.\n\n"
            + "Next steps:\n"
            + "- Make sure that the connection string has the appropriate Databricks workspace FQDN.\n\n"
            + "- Verify the configured truststore path and make sure the required certificates are imported.\n"
            + "  .   PEM certificate chain of the warehouse endpoint can be fetched using \"openssl s_client -connect %s:443 -showcerts\"\n"
            + "  .   Reference KB article with troubleshooting steps.\n",
        customTruststorePathMessage, e.getMessage(), workspaceHostname);
  }
}
