package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.JSON_HTTP_HEADERS;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.OPERATION_CANCELLED_SQLSTATE;
import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_EXECUTION_TIMEOUT_SQLSTATE;
import static com.databricks.jdbc.common.EnvironmentVariables.DEFAULT_RESULT_ROW_LIMIT;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.DECIMAL;
import static com.databricks.jdbc.common.util.DatabricksTypeUtil.getDecimalTypeString;
import static com.databricks.jdbc.common.util.SqlStateClassifier.classifyTransientSqlState;
import static com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants.*;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.*;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.SQLInterpolator;
import com.databricks.jdbc.common.util.SqlCommentParser;
import com.databricks.jdbc.common.util.UserAgentManager;
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
import com.databricks.jdbc.model.core.ColumnInfoTypeName;
import com.databricks.jdbc.model.core.Disposition;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.model.core.ResultManifest;
import com.databricks.jdbc.model.core.SessionVersion;
import com.databricks.jdbc.model.core.SqlExecResultFormat;
import com.databricks.jdbc.model.core.StatementStatus;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import org.apache.http.HttpStatus;

/** Implementation of IDatabricksClient interface using Databricks Java SDK. */
public class DatabricksSdkClient implements IDatabricksClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksSdkClient.class);
  private static final ObjectMapper ERROR_MAPPER = new ObjectMapper();
  private static final String SYNC_TIMEOUT_VALUE = "10s";
  private static final String ASYNC_TIMEOUT_VALUE = "0s";
  private static final String FAST_SESSION_EXECUTION_MODE = "FAST";
  private static final String REALTIME_SESSION_HEADER = "x-databricks-rt-session";
  private static final String HEADER_METADATA_OPERATION_TYPE =
      "X-Databricks-Metadata-Operation-Type";
  private static final String HEADER_REQUIRE_THRIFT_NATIVE_METADATA =
      "X-Databricks-Require-Thrift-Native-Metadata";

  private final IDatabricksConnectionContext connectionContext;
  private final ClientConfigurator clientConfigurator;
  private final String customerUserAgentSegment;
  // Non-prepared statements would otherwise WARN and emit telemetry on every call.
  private final AtomicBoolean apcDowngradeReported = new AtomicBoolean(false);
  private volatile ApiClient apiClient;

  static final int APC_NATIVE_BATCH_SUBMIT_MAX_THREADS = 16;
  // A timed-out submit stays blocked in HTTP, so the pool rejects instead of growing or queueing.
  private final java.util.concurrent.ExecutorService apcNativeBatchSubmitExecutor =
      new java.util.concurrent.ThreadPoolExecutor(
          0,
          APC_NATIVE_BATCH_SUBMIT_MAX_THREADS,
          60,
          java.util.concurrent.TimeUnit.SECONDS,
          new java.util.concurrent.SynchronousQueue<>(),
          runnable -> {
            Thread thread = new Thread(runnable, "apc-native-batch-submit");
            thread.setDaemon(true);
            return thread;
          });

  private static final class CreateSessionHttpRequest extends RedactingRequest {
    boolean realtimeSession;

    CreateSessionHttpRequest(String path, String body) {
      super(Request.POST, path, body);
    }
  }

  private <T> T execute(Request request, Class<T> responseType) throws IOException {
    if (!(request instanceof RedactingRequest)
        && (request.getUrl().contains("backend_compute.all_purpose_cluster_compute.cluster_id=")
            || (request.getBodyString() != null
                && request.getBodyString().contains("\"backend_compute\"")))) {
      request = new RedactingRequest(request);
    }
    return apiClient.execute(request, responseType);
  }

  public DatabricksSdkClient(IDatabricksConnectionContext connectionContext)
      throws DatabricksParsingException, DatabricksHttpException {
    this.connectionContext = connectionContext;
    this.clientConfigurator =
        DatabricksClientConfiguratorManager.getInstance().getConfigurator(connectionContext);
    this.customerUserAgentSegment =
        UserAgentManager.customerUserAgentSegment(connectionContext.getCustomerUserAgent());
    this.apiClient = newApiClient();
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
    this.customerUserAgentSegment =
        UserAgentManager.customerUserAgentSegment(connectionContext.getCustomerUserAgent());
    this.apiClient = apiClient;
  }

  private ApiClient newApiClient() {
    return buildApiClient(
        clientConfigurator.getDatabricksConfig(),
        customerUserAgentSegment,
        connectionContext.isSeaResponseCompressionEnabled());
  }

  @VisibleForTesting
  static ApiClient buildApiClient(
      DatabricksConfig config,
      String customerUserAgentSegment,
      boolean responseCompressionEnabled) {
    HttpClient transport = config.getHttpClient();
    HttpClient delegate = transport;
    // Always wrap the transport so CreateSession response headers can determine follow-up routing.
    transport =
        request -> {
          if (customerUserAgentSegment != null) {
            String userAgent = request.getHeaders().get("User-Agent");
            if (userAgent != null) {
              request.withHeader(
                  "User-Agent",
                  UserAgentManager.orderSeaUserAgent(userAgent, customerUserAgentSegment));
            }
          }
          if (!responseCompressionEnabled) {
            request.withHeader("Accept-Encoding", "identity");
          }
          if (request instanceof RedactingRequest) {
            ((RedactingRequest) request).showWireBody();
          }
          Response response;
          try {
            response = delegate.execute(request);
          } catch (IOException e) {
            if (request instanceof RedactingRequest) {
              // ApiClient logs transport failures before the driver can sanitize the exception.
              throw sanitizeApcTransportFailure(e);
            }
            throw e;
          } finally {
            if (request instanceof RedactingRequest) {
              ((RedactingRequest) request).hideWireBody();
            }
          }
          response = redactApcResponseDebugBody(request, response);
          if (request instanceof CreateSessionHttpRequest) {
            String header = response.getFirstHeader(REALTIME_SESSION_HEADER);
            ((CreateSessionHttpRequest) request).realtimeSession =
                header != null && "true".equalsIgnoreCase(header.trim());
          }
          return response;
        };
    return new ApiClient.Builder().withDatabricksConfig(config).withHttpClient(transport).build();
  }

  /**
   * Returns a message-free exception of the same retry-relevant class. The SDK's non-idempotent
   * strategy retries POSTs only when the cause (or a nested cause) is one of these connect-phase
   * types, so a plain IOException would silently disable retries for
   * CreateSession/ExecuteStatement. The original is not chained because ApiClient logs the full
   * cause chain.
   */
  @VisibleForTesting
  static IOException sanitizeApcTransportFailure(IOException original) {
    String message = "APC SEA transport request failed";
    for (Throwable t = original; t != null; t = t.getCause()) {
      if (t instanceof ConnectException) {
        return new ConnectException(message);
      } else if (t instanceof UnknownHostException) {
        return new UnknownHostException(message);
      } else if (t instanceof NoRouteToHostException) {
        return new NoRouteToHostException(message);
      } else if (t instanceof PortUnreachableException) {
        return new PortUnreachableException(message);
      }
    }
    if (original instanceof SocketTimeoutException) {
      return new SocketTimeoutException(message);
    }
    return new IOException(message);
  }

  @VisibleForTesting
  static Response redactApcResponseDebugBody(Request request, Response response) {
    if (!(request instanceof RedactingRequest)) {
      return response;
    }
    if (response.getStatusCode() >= 400) {
      // ApiClient logs the parsed DatabricksError as a throwable on every retry. Scrub the
      // response before the SDK parses it, including non-JSON errors and nested details.
      String code = "SERVER_ERROR";
      String detail = "APC SEA request failed";
      ArrayNode safeDetails = ERROR_MAPPER.createArrayNode();
      try {
        if (response.getBody() != null) {
          JsonNode error = ERROR_MAPPER.readTree(response.getBody());
          if (error != null) {
            code = safeServerErrorCode(error.path("error_code").asText());
            // Keep only the enum-like ErrorInfo identifiers; message and metadata may echo SQL.
            for (JsonNode item : error.path("details")) {
              String type = item.path("@type").asText("");
              String reason = item.path("reason").asText("");
              String domain = item.path("domain").asText("");
              if (type.endsWith("google.rpc.ErrorInfo")
                  && reason.matches("[A-Z0-9_]{1,128}")
                  && domain.matches("[A-Za-z0-9_.-]{0,128}")) {
                safeDetails
                    .addObject()
                    .put("@type", "type.googleapis.com/google.rpc.ErrorInfo")
                    .put("reason", reason)
                    .put("domain", domain);
              }
            }
            String message = error.path("message").asText("").toLowerCase(Locale.ROOT);
            if (message.contains("all-purpose clusters are not supported")) {
              detail = "all-purpose clusters are not supported";
            } else if (message.contains("unsupported region")
                || message.contains("region is not supported")) {
              detail = "unsupported region";
            }
          }
        }
      } catch (IOException ignored) {
        // Unknown response formats still get a safe, structured error.
      }
      ObjectNode safeError = ERROR_MAPPER.createObjectNode();
      safeError.put("error_code", code).put("message", detail);
      if (safeDetails.size() > 0) {
        safeError.set("details", safeDetails);
      }
      String safeBody = safeError.toString();
      return new Response(
          request,
          response.getUrl(),
          response.getStatusCode(),
          "APC request failed",
          response.getAllHeaders(),
          safeBody);
    }
    // Successful APC responses still need their full body for result decoding. Hide the body
    // from the SDK debug view without changing the parser's input stream.
    return new Response(
        request,
        response.getUrl(),
        response.getStatusCode(),
        response.getStatus(),
        response.getAllHeaders(),
        response.getBody());
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  @Override
  public ImmutableSessionInfo createSession(
      IDatabricksComputeResource computeResource,
      String catalog,
      String schema,
      Map<String, String> sessionConf)
      throws DatabricksSQLException {
    // TODO (PECO-1460): Handle sessionConf in public session API
    SeaComputeTarget target = SeaComputeTarget.of(computeResource);
    LOGGER.debug(
        "public Session createSession(compute = {}, catalog = {}, schema = {}, sessionConf = {})",
        computeLabel(computeResource),
        target.isApc() ? "[REDACTED]" : catalog,
        target.isApc() ? "[REDACTED]" : schema,
        target.isApc() ? "[REDACTED]" : sessionConf);
    CreateSessionRequest request =
        new CreateSessionRequest().setExecutionMode(FAST_SESSION_EXECUTION_MODE);
    if (target.isApc()) {
      request.setBackendCompute(target.backendCompute());
    }
    if (catalog != null) {
      request.setCatalog(catalog);
    }
    if (schema != null) {
      request.setSchema(schema);
    }
    // Inline-only SEA results must not be served from cloud storage.
    if ((target.isApc() || connectionContext.isBoundedSeaApiEnabled())
        && !connectionContext.isCloudFetchEnabled()) {
      if (sessionConf == null) {
        sessionConf = new java.util.HashMap<>();
      } else {
        sessionConf = new java.util.HashMap<>(sessionConf);
      }
      sessionConf.put("can_cloud_download", "false");
    }
    if (sessionConf != null && !sessionConf.isEmpty()) {
      request.setSessionConfigs(sessionConf);
    }
    CreateSessionResponse createSessionResponse;
    String createPath = target.createSessionPath();
    boolean realtimeSession;
    try {
      CreateSessionHttpRequest req =
          new CreateSessionHttpRequest(createPath, apiClient.serialize(request));
      req.withHeaders(getHeaders("createSession"));
      createSessionResponse = execute(req, CreateSessionResponse.class);
      realtimeSession = req.realtimeSession;
    } catch (DatabricksError e) {
      if (target.isApc() && isSdkTransportError(e)) {
        throw new DatabricksSQLException(
            "APC SEA CreateSession request failed",
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      if (e.getStatusCode() == HttpStatus.SC_TOO_MANY_REQUESTS) {
        if (target.isApc()) {
          String code = safeServerErrorCode(e.getErrorCode());
          String safeMessage =
              "APC SEA CreateSession rate limited (server error " + code + ", HTTP 429)";
          throw new DatabricksRateLimitException(
              safeMessage,
              new DatabricksError(code, safeMessage, HttpStatus.SC_TOO_MANY_REQUESTS),
              HttpStatus.SC_TOO_MANY_REQUESTS,
              DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED);
        }
        String errorMessage =
            String.format(
                "createSession failed with HTTP %d (rate limit exceeded) after retries. "
                    + "Compute: %s, Error: %s",
                HttpStatus.SC_TOO_MANY_REQUESTS,
                target.isApc() ? "all-purpose cluster" : target.warehouseId(),
                e.getMessage());
        LOGGER.warn(errorMessage, e);
        throw new DatabricksRateLimitException(errorMessage, e, HttpStatus.SC_TOO_MANY_REQUESTS);
      }
      if (target.isApc()) {
        throw apcServerFailure("CreateSession", e, DatabricksDriverErrorCode.CONNECTION_ERROR);
      }
      String errorReason = buildErrorMessage(e);
      throw new DatabricksSQLException(errorReason, e, DatabricksDriverErrorCode.CONNECTION_ERROR);
    } catch (IOException e) {
      String errorMessage = "Error while processing the request via the sdk client";
      if (target.isApc()) {
        LOGGER.error("APC SEA CreateSession request failed");
        throw new DatabricksSQLException(
            "APC SEA CreateSession request failed",
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (!target.isApc()) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        if (serverError.getStatusCode() == HttpStatus.SC_TOO_MANY_REQUESTS) {
          String code = safeServerErrorCode(serverError.getErrorCode());
          String safeMessage =
              "APC SEA CreateSession rate limited (server error " + code + ", HTTP 429)";
          throw new DatabricksRateLimitException(
              safeMessage,
              new DatabricksError(code, safeMessage, HttpStatus.SC_TOO_MANY_REQUESTS),
              HttpStatus.SC_TOO_MANY_REQUESTS,
              DatabricksDriverErrorCode.RATE_LIMIT_EXCEEDED);
        }
        throw apcServerFailure(
            "CreateSession", serverError, DatabricksDriverErrorCode.CONNECTION_ERROR);
      }
      LOGGER.error("APC SEA CreateSession request failed");
      throw new DatabricksSQLException(
          "APC SEA CreateSession request failed",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    String sessionId = createSessionResponse.getSessionId();
    DatabricksThreadContextHolder.setSessionId(sessionId);
    ImmutableSessionInfo.Builder sessionInfo =
        ImmutableSessionInfo.builder()
            .computeResource(computeResource)
            .clientType(DatabricksClientType.SEA)
            .sessionId(sessionId)
            .realtimeSession(!target.isApc() && realtimeSession)
            .negotiatedProtocolVersion(createSessionResponse.getNegotiatedProtocolVersion())
            .liveProtocolVersion(true);
    SessionVersion initialVersion = createSessionResponse.getSessionVersion();
    if (initialVersion != null && initialVersion.getVersionId() != null) {
      sessionInfo.sessionVersionId(initialVersion.getVersionId());
    }
    return sessionInfo.build();
  }

  private String sessionPath(ImmutableSessionInfo sessionInfo) {
    if (sessionInfo.realtimeSession()
        && !SeaComputeTarget.of(sessionInfo.computeResource()).isApc()) {
      return String.format(
          WAREHOUSE_SESSION_PATH_WITH_ID,
          SeaComputeTarget.of(sessionInfo.computeResource()).warehouseId(),
          sessionInfo.sessionId());
    }
    return String.format(SESSION_PATH_WITH_ID, sessionInfo.sessionId());
  }

  private String statementCollectionPath(ImmutableSessionInfo sessionInfo) {
    if (sessionInfo.realtimeSession()
        && !SeaComputeTarget.of(sessionInfo.computeResource()).isApc()) {
      return String.format(
          WAREHOUSE_STATEMENT_PATH,
          SeaComputeTarget.of(sessionInfo.computeResource()).warehouseId(),
          sessionInfo.sessionId());
    }
    return STATEMENT_PATH;
  }

  private String statementPath(StatementId statementId) {
    if (statementId.sessionId() != null && statementId.computeResource() == null) {
      return String.format(
          WAREHOUSE_STATEMENT_PATH_WITH_ID,
          statementId.warehouseId(),
          statementId.sessionId(),
          statementId.toSQLExecStatementId());
    }
    return String.format(STATEMENT_PATH_WITH_ID, statementId.toSQLExecStatementId());
  }

  private String serializeStatementRequest(StatementId statementId, Object request)
      throws IOException {
    return statementId.computeResource() != null || statementId.sessionId() != null
        ? null
        : apiClient.serialize(request);
  }

  private String targetStatementPath(StatementId statementId, String path) {
    return statementId.computeResource() == null
        ? path
        : SeaComputeTarget.of(statementId.computeResource())
            .withQuery(path, Collections.emptyMap());
  }

  private String targetChunkPath(StatementId statementId, String path, Long rowOffset) {
    Map<String, String> query = new LinkedHashMap<>();
    if (rowOffset != null) {
      query.put("row_offset", Long.toString(rowOffset));
    }
    if (statementId.computeResource() != null) {
      return SeaComputeTarget.of(statementId.computeResource()).withQuery(path, query);
    }
    return query.isEmpty() ? path : path + "?row_offset=" + rowOffset;
  }

  private static String computeLabel(IDatabricksComputeResource resource) {
    return resource instanceof AllPurposeCluster ? "all-purpose cluster" : resource.toString();
  }

  private static DatabricksError findServerError(Throwable failure) {
    for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
      if (cause instanceof DatabricksError) {
        DatabricksError error = (DatabricksError) cause;
        return isSdkTransportError(error) ? null : error;
      }
    }
    return null;
  }

  private static boolean isSdkTransportError(DatabricksError error) {
    // ApiClient wraps an IOException as IO_ERROR/523, including after exhausting retries.
    // Require the IOException cause so a server response with those fields remains a server error.
    return "IO_ERROR".equals(error.getErrorCode())
        && error.getStatusCode() == 523
        && error.getCause() instanceof IOException;
  }

  private static DatabricksSQLException apcServerFailure(
      String operation, DatabricksError error, DatabricksDriverErrorCode driverCode) {
    // Server messages can echo the request URL, SQL, and bound values. Keep only structured
    // server fields and recognized error categories in the exception, its cause, and telemetry.
    String code = safeServerErrorCode(error.getErrorCode());
    int status = error.getStatusCode();
    String rawMessage = error.getMessage();
    String normalized = rawMessage == null ? "" : rawMessage.toLowerCase(Locale.ROOT);
    String detail = "";
    if (normalized.contains("all-purpose clusters are not supported")) {
      detail = ": all-purpose clusters are not supported";
    } else if (normalized.contains("unsupported region")
        || normalized.contains("region is not supported")
        || code.contains("UNSUPPORTED_REGION")) {
      detail = ": unsupported region";
    }
    String safeMessage =
        String.format(
            "APC SEA %s failed%s (server error %s, HTTP %d)", operation, detail, code, status);
    DatabricksError safeCause = new DatabricksError(code, safeMessage, status);
    LOGGER.error(safeMessage);
    return driverCode == null
        ? new DatabricksSQLException(safeMessage, code, status, safeCause)
        : new DatabricksSQLException(
            safeMessage, driverCode.name(), driverCode.getCode(), safeCause);
  }

  private static String safeServerErrorCode(String rawCode) {
    return rawCode != null && rawCode.matches("[A-Z0-9_]{1,128}") ? rawCode : "SERVER_ERROR";
  }

  @Override
  public void deleteSession(ImmutableSessionInfo sessionInfo) throws DatabricksSQLException {
    LOGGER.debug(
        "public void deleteSession(String sessionId = {})",
        SeaComputeTarget.of(sessionInfo.computeResource()).isApc()
            ? "[REDACTED]"
            : sessionInfo.sessionId());
    DatabricksThreadContextHolder.setSessionId(sessionInfo.sessionId());
    DeleteSessionRequest request = new DeleteSessionRequest().setSessionId(sessionInfo.sessionId());
    SeaComputeTarget target = SeaComputeTarget.of(sessionInfo.computeResource());
    if (target.isApc()) {
      request.setBackendCompute(target.backendCompute());
    } else {
      request.setWarehouseId(target.warehouseId());
    }
    String path = sessionPath(sessionInfo);
    try {
      Request req =
          target.isApc()
              ? new Request(Request.DELETE, path, apiClient.serialize(request))
              : new Request(Request.DELETE, path);
      req.withHeaders(getHeaders("deleteSession"));
      if (!target.isApc() && !sessionInfo.realtimeSession()) {
        ApiClient.setQuery(req, request);
      }
      execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while performing the deleting session operation";
      if (target.isApc()) {
        LOGGER.error("APC SEA TerminateSession request failed");
        throw new DatabricksSQLException(
            "APC SEA TerminateSession request failed",
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (!target.isApc()) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("TerminateSession", serverError, null);
      }
      LOGGER.error("APC SEA TerminateSession request failed");
      throw new DatabricksSQLException(
          "APC SEA TerminateSession request failed",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      MetadataOperationType metadataOperationType)
      throws SQLException {
    return executeStatementInternal(
        sql,
        computeResource,
        parameters,
        statementType,
        session,
        parentStatement,
        metadataOperationType,
        null,
        null);
  }

  private DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      MetadataOperationType metadataOperationType,
      ExecuteStatementRequest suppliedRequest)
      throws SQLException {
    return executeStatementInternal(
        sql,
        computeResource,
        parameters,
        statementType,
        session,
        parentStatement,
        metadataOperationType,
        null,
        suppliedRequest);
  }

  @Override
  public boolean supportsNativeBatchExecution() {
    return true;
  }

  @Override
  public long[] executeNativeBatch(
      String sql,
      List<BatchParameterSet> parameterSets,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    if (!ApcParameterCapabilities.supportsNativeBatch(session)) {
      throw new java.sql.SQLFeatureNotSupportedException(
          "APC parameter_sets require a live V10 session",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode());
    }
    int placeholderCount = SqlCommentParser.findApcPlaceholderPositions(sql).length;
    for (BatchParameterSet set : parameterSets) {
      ApcParameterCapabilities.rejectComplexBindings(set.getParameterBindings());
      SQLInterpolator.validateApcBindings(placeholderCount, set.getParameterBindings());
    }
    if (!ApcParameterCapabilities.hasConsistentBinaryPositions(parameterSets)) {
      // Bytes are sent as hex under a statement-wide unhex(?), so sets cannot mix binary and
      // non-binary values at one position.
      throw new java.sql.SQLFeatureNotSupportedException(
          "APC parameter_sets cannot mix binary and non-binary values at one position",
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.name(),
          DatabricksDriverErrorCode.UNSUPPORTED_OPERATION.getCode());
    }
    List<StatementParameterSet> wireSets =
        parameterSets.stream()
            .map(
                set ->
                    new StatementParameterSet(
                        set.getParameters().stream()
                            .map(parameter -> mapToParameterListItem(parameter, true))
                            .collect(Collectors.toList())))
            .collect(Collectors.toList());
    AwaitedStatement awaited = null;
    try {
      // The aggregate result is never decoded: a decoding failure after the server reported
      // SUCCEEDED would surface as EXECUTE_FAILED for rows that are already committed.
      awaited =
          awaitStatement(
              sql,
              session.getComputeResource(),
              Collections.emptyMap(),
              StatementType.UPDATE,
              session,
              parentStatement,
              null,
              wireSets,
              null);
    } finally {
      // A successful INSERT must remain successful even when cleanup fails. Reporting
      // EXECUTE_FAILED here could make callers retry and duplicate committed rows.
      try {
        if (awaited != null && awaited.state == StatementState.CLOSED) {
          // The server already closed the operation; there is nothing left to release.
          if (parentStatement != null) {
            parentStatement.markDirectResultsReceived();
          }
        } else if (parentStatement != null) {
          parentStatement.closeServerOperation();
        } else if (awaited != null) {
          closeStatement(awaited.statementId);
        }
      } catch (Exception cleanupFailure) {
        LOGGER.warn("Could not close APC native batch operation");
      }
    }
    long[] counts = new long[parameterSets.size()];
    Arrays.fill(counts, java.sql.Statement.SUCCESS_NO_INFO);
    return counts;
  }

  /** A statement that was submitted and reached SUCCEEDED or CLOSED, before result decoding. */
  private static final class AwaitedStatement {
    final StatementId statementId;
    final ExecuteStatementResponse response;
    final StatementState state;

    AwaitedStatement(
        StatementId statementId, ExecuteStatementResponse response, StatementState state) {
      this.statementId = statementId;
      this.response = response;
      this.state = state;
    }
  }

  private DatabricksResultSet executeStatementInternal(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      MetadataOperationType metadataOperationType,
      List<StatementParameterSet> parameterSets,
      ExecuteStatementRequest suppliedRequest)
      throws SQLException {
    AwaitedStatement awaited =
        awaitStatement(
            sql,
            computeResource,
            parameters,
            statementType,
            session,
            parentStatement,
            metadataOperationType,
            parameterSets,
            suppliedRequest);
    StatementId typedStatementId = awaited.statementId;
    ExecuteStatementResponse response = awaited.response;

    // Defer markDirectResultsReceived until AFTER ResultSet construction.
    // VolumeOperationResult (created during ResultSet construction) accesses
    // statement properties that require the statement to be in a valid state.
    boolean shouldMarkDirectResults =
        awaited.state == StatementState.CLOSED && parentStatement != null;

    // Procedure and internal catalog/schema SELECTs use the ordinary query layout even if a
    // server response incorrectly advertises native metadata.
    if (SeaComputeTarget.of(computeResource).isApc()
        && !isShowMetadata(statementType, metadataOperationType)
        && response.getManifest() != null) {
      response.getManifest().setIsNativeMetadataResult(false);
    }

    DatabricksResultSet resultSet =
        new DatabricksResultSet(
            response.getStatus(),
            typedStatementId,
            response.getResult(),
            response.getManifest(),
            statementType,
            session,
            parentStatement);

    if (shouldMarkDirectResults) {
      LOGGER.debug(
          "Statement {} returned CLOSED status with direct results, marking as direct results received",
          response.getStatementId());
      parentStatement.markDirectResultsReceived();
    }

    return resultSet;
  }

  private AwaitedStatement awaitStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      MetadataOperationType metadataOperationType,
      List<StatementParameterSet> parameterSets,
      ExecuteStatementRequest suppliedRequest)
      throws SQLException {
    boolean apc = SeaComputeTarget.of(computeResource).isApc();
    String logSql = apc ? "[REDACTED]" : sql;
    String errorSql = apc ? "[REDACTED]" : sql;
    LOGGER.debug(
        "public DatabricksResultSet executeStatement(String sql = {}, compute resource = {}, parameterCount = {}, StatementType statementType = {}, IDatabricksSession session = {}, parentStatement = {}, metadataOperationType = {})",
        logSql,
        computeLabel(computeResource),
        parameters.size(),
        statementType,
        apc ? "[REDACTED]" : session,
        apc ? "[REDACTED]" : parentStatement,
        metadataOperationType);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
    long pollCount = 0;
    long executionStartTime = Instant.now().toEpochMilli();
    DatabricksThreadContextHolder.setStatementType(statementType);
    ExecuteStatementRequest request =
        suppliedRequest != null
            ? suppliedRequest
            : getRequest(
                statementType,
                sql,
                computeResource,
                session,
                parameters,
                parentStatement,
                false,
                isShowMetadata(statementType, metadataOperationType));
    if (parameterSets != null) {
      request.setParameterSets(parameterSets);
    }
    ExecuteStatementResponse response;
    // APC submits can block before a statement ID exists, so the query timeout clock starts here.
    long submitStartMillis = System.currentTimeMillis();
    try {
      Request req =
          new Request(
              Request.POST,
              statementCollectionPath(session.getSessionInfo()),
              apiClient.serialize(request));
      Map<String, String> additionalHeaders = new HashMap<>();
      if (metadataOperationType != null
          && (!apc || isShowMetadata(statementType, metadataOperationType))) {
        additionalHeaders.put(
            HEADER_METADATA_OPERATION_TYPE, metadataOperationType.getHeaderValue());
        if (connectionContext.isThriftNativeMetadataEnabled()
            && metadataOperationType.isThriftNativeSupported()) {
          additionalHeaders.put(HEADER_REQUIRE_THRIFT_NATIVE_METADATA, "true");
        }
      }
      req.withHeaders(
          getHeaders(
              "executeStatement",
              apc && !isShowMetadata(statementType, metadataOperationType)
                  ? StatementType.SQL
                  : statementType,
              false,
              additionalHeaders));
      response =
          parameterSets != null && parentStatement != null
              ? submitWithinQueryTimeout(
                  req, session, parentStatement.getStatement().getQueryTimeout())
              : execute(req, ExecuteStatementResponse.class);
      updateSessionVersion(session, response.getStatus());
    } catch (IOException e) {
      String errorMessage = "Error while processing the execute statement request";
      if (apc) {
        // SDK exceptions can include the server response, which may echo bound values.
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (DatabricksException e) {
      if (!apc) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("ExecuteStatement", serverError, null);
      }
      String errorMessage = "Error while processing the execute statement request";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(
          errorMessage,
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (!apc) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("ExecuteStatement", serverError, null);
      }
      // SDK retries may wrap an HTTP error in another unchecked exception.
      LOGGER.error("Error while processing the execute statement request");
      throw new DatabricksSQLException(
          "Error while processing the execute statement request",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error(
          "Empty Statement ID for sql {}, statementType {}, compute {}",
          logSql,
          statementType,
          computeLabel(computeResource));
      handleFailedExecution(response, "", errorSql, apc);
    }
    LOGGER.debug(
        "Executing sql = {}, statementType = {}, compute = {}, StatementID = {}",
        logSql,
        statementType,
        computeLabel(computeResource),
        statementId);
    StatementId typedStatementId =
        StatementId.forCurrentSession(statementId, session.getSessionInfo());
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }

    int timeoutInSeconds;
    if (parentStatement != null) {
      timeoutInSeconds = parentStatement.getStatement().getQueryTimeout();
    } else if (statementType == StatementType.METADATA) {
      timeoutInSeconds = connectionContext.getMetadataOperationTimeout();
    } else {
      timeoutInSeconds = 0;
    }

    // Create timeout handler — use OPERATION_TIMEOUT_ERROR for metadata to match
    // the native Thrift metadata path (DatabricksThriftAccessor.fetchMetadataResults)
    DatabricksDriverErrorCode timeoutErrorCode =
        (statementType == StatementType.METADATA && parentStatement == null)
            ? DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR
            : DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT;
    TimeoutHandler timeoutHandler =
        TimeoutHandler.forStatement(
            timeoutInSeconds,
            typedStatementId,
            this,
            timeoutErrorCode,
            apc,
            apc ? submitStartMillis : System.currentTimeMillis());

    StatementState responseState = response.getStatus().getState();
    try {
      // Only enforce the deadline while the statement is still in flight. A already-SUCCEEDED APC
      // response (for example a committed write) must not be reported as a timeout.
      while (responseState == StatementState.PENDING || responseState == StatementState.RUNNING) {
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
        String getStatusPath =
            targetStatementPath(typedStatementId, statementPath(typedStatementId));
        try {
          Request req =
              new Request(
                  Request.GET, getStatusPath, serializeStatementRequest(typedStatementId, request));
          req.withHeaders(getHeaders("getStatement"));
          response = wrapGetStatementResponse(execute(req, GetStatementResponse.class));
          updateSessionVersion(session, response.getStatus());
        } catch (IOException e) {
          String errorMessage = "Error while processing the get statement response";
          if (apc) {
            LOGGER.error(errorMessage);
            throw new DatabricksSQLException(
                errorMessage,
                DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
                DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
          }
          LOGGER.error(errorMessage, e);
          throw new DatabricksSQLException(
              errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
        } catch (RuntimeException e) {
          if (!apc) {
            throw e;
          }
          DatabricksError serverError = findServerError(e);
          if (serverError != null) {
            throw apcServerFailure("GetStatementResult", serverError, null);
          }
          LOGGER.error("Error while processing the get statement response");
          throw new DatabricksSQLException(
              "Error while processing the get statement response",
              DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
              DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
        }
        responseState = response.getStatus().getState();
        // A poll that returns a terminal state after the deadline must keep that state; cancelling
        // here would report a committed write as a timeout.
        if (apc
            && (responseState == StatementState.PENDING
                || responseState == StatementState.RUNNING)) {
          timeoutHandler.checkTimeout();
        }
        LOGGER.debug(
            "Executed sql {} with status {} with retry count {}", logSql, responseState, pollCount);
        pollCount++;
      }
    } catch (SQLException | RuntimeException pollFailure) {
      if (apc) {
        closeFailedApcOperation(typedStatementId, parentStatement);
      }
      throw pollFailure;
    }
    long executionEndTime = Instant.now().toEpochMilli();
    {
      ResultManifest manifest = response.getManifest();
      ResultData resultData = response.getResult();
      int numExternalLinks =
          (resultData != null && resultData.getExternalLinks() != null)
              ? resultData.getExternalLinks().size()
              : 0;
      LOGGER.debug(
          "executeStatement complete: statementId={}, state={}, format={}, "
              + "totalBytes={}, totalChunks={}, totalRows={}, "
              + "hasInlineAttachment={}, numExternalLinks={}, compression={}, "
              + "executionTimeMs={}, pollCount={}",
          statementId,
          responseState,
          manifest != null ? manifest.getFormat() : "null",
          manifest != null ? manifest.getTotalByteCount() : "null",
          manifest != null ? manifest.getTotalChunkCount() : "null",
          manifest != null ? manifest.getTotalRowCount() : "null",
          resultData != null && resultData.getAttachment() != null,
          numExternalLinks,
          manifest != null ? manifest.getResultCompression() : "null",
          (executionEndTime - executionStartTime),
          pollCount);
    }
    if (responseState != StatementState.SUCCEEDED && responseState != StatementState.CLOSED) {
      try {
        handleFailedExecution(response, statementId, errorSql, apc);
      } finally {
        if (apc) {
          closeFailedApcOperation(typedStatementId, parentStatement);
        }
      }
    }
    return new AwaitedStatement(typedStatementId, response, responseState);
  }

  /**
   * Bounds a native batch submit by the query timeout. The submit can block before any statement ID
   * exists, where the polling-based timeout check never runs.
   */
  private ExecuteStatementResponse submitWithinQueryTimeout(
      Request request, IDatabricksSession session, int timeoutSeconds)
      throws IOException, DatabricksTimeoutException {
    if (timeoutSeconds <= 0) {
      return execute(request, ExecuteStatementResponse.class);
    }
    java.util.concurrent.CompletableFuture<ExecuteStatementResponse> submit;
    try {
      submit =
          java.util.concurrent.CompletableFuture.supplyAsync(
              () -> {
                try {
                  return execute(request, ExecuteStatementResponse.class);
                } catch (IOException e) {
                  throw new java.util.concurrent.CompletionException(e);
                }
              },
              apcNativeBatchSubmitExecutor);
    } catch (java.util.concurrent.RejectedExecutionException e) {
      throw new IOException("Too many timed-out APC native batch submits are still in flight", e);
    }
    try {
      return submit.get(timeoutSeconds, java.util.concurrent.TimeUnit.SECONDS);
    } catch (java.util.concurrent.TimeoutException | InterruptedException e) {
      if (e instanceof InterruptedException) {
        Thread.currentThread().interrupt();
      }
      // The abandoned submit can still reach the server; release the operation it creates.
      submit.thenAccept(late -> releaseAbandonedSubmit(late, session));
      throw new DatabricksTimeoutException(
          "Statement execution timed-out before the APC native batch was accepted",
          e,
          DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
    } catch (java.util.concurrent.ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      if (cause instanceof Error) {
        throw (Error) cause;
      }
      throw new RuntimeException(cause);
    }
  }

  private void releaseAbandonedSubmit(ExecuteStatementResponse late, IDatabricksSession session) {
    if (late.getStatementId() == null) {
      return;
    }
    StatementId id = StatementId.forCurrentSession(late.getStatementId(), session.getSessionInfo());
    try {
      cancelStatement(id);
    } catch (Exception cleanupFailure) {
      LOGGER.warn("Could not cancel timed-out APC native batch operation");
    }
    try {
      closeStatement(id);
    } catch (Exception cleanupFailure) {
      LOGGER.warn("Could not close timed-out APC native batch operation");
    }
  }

  private void closeFailedApcOperation(
      StatementId statementId, IDatabricksStatementInternal parentStatement) {
    try {
      if (parentStatement != null) {
        parentStatement.closeServerOperation();
      } else {
        closeStatement(statementId);
      }
    } catch (Exception cleanupFailure) {
      // The execution failure and its SQL state take precedence over DELETE failures.
      LOGGER.warn("Could not close failed APC statement operation");
    }
  }

  @Override
  public boolean supportsNativeParameterBatching(IDatabricksComputeResource computeResource) {
    return computeResource instanceof Warehouse;
  }

  @Override
  public DatabricksResultSet executeStatementBatch(
      String sql,
      IDatabricksComputeResource computeResource,
      List<BatchParameterSet> parameterSets,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    LOGGER.debug(
        "Executing native parameter batch with {} parameter sets on {}",
        parameterSets.size(),
        computeResource);
    ExecuteStatementRequest request =
        getBatchRequest(
            statementType, sql, computeResource, session, parameterSets, parentStatement);
    return executeStatement(
        sql,
        computeResource,
        Collections.emptyMap(),
        statementType,
        session,
        parentStatement,
        null,
        request);
  }

  @Override
  public DatabricksResultSet executeStatementAsync(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    return executeStatementAsync(
        sql, computeResource, parameters, session, parentStatement, StatementType.SQL, null);
  }

  @Override
  public DatabricksResultSet executeStatementAsync(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      StatementType statementType,
      MetadataOperationType metadataOperationType)
      throws SQLException {
    boolean apc = SeaComputeTarget.of(computeResource).isApc();
    String logSql = apc ? "[REDACTED]" : sql;
    LOGGER.debug(
        "public DatabricksResultSet executeStatementAsync(String sql = {}, compute resource = {}, Map<Integer, ImmutableSqlParameter> parameters, IDatabricksSession session = {}, IDatabricksStatementInternal parentStatement = {})",
        logSql,
        computeLabel(computeResource),
        apc ? "[REDACTED]" : session,
        apc ? "[REDACTED]" : parentStatement);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
    ExecuteStatementRequest request =
        getRequest(
            statementType,
            sql,
            computeResource,
            session,
            parameters,
            parentStatement,
            true,
            isShowMetadata(statementType, metadataOperationType));
    ExecuteStatementResponse response;
    try {
      Request req =
          new Request(
              Request.POST,
              statementCollectionPath(session.getSessionInfo()),
              apiClient.serialize(request));
      Map<String, String> additionalHeaders = new HashMap<>();
      if (metadataOperationType != null
          && (!apc || isShowMetadata(statementType, metadataOperationType))) {
        additionalHeaders.put(
            HEADER_METADATA_OPERATION_TYPE, metadataOperationType.getHeaderValue());
        if (connectionContext.isThriftNativeMetadataEnabled()
            && metadataOperationType.isThriftNativeSupported()) {
          additionalHeaders.put(HEADER_REQUIRE_THRIFT_NATIVE_METADATA, "true");
        }
      }
      req.withHeaders(getHeaders("executeStatement", statementType, true, additionalHeaders));
      response = execute(req, ExecuteStatementResponse.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the execute statement async request";
      if (apc) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (DatabricksException e) {
      if (!apc) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("ExecuteStatement", serverError, null);
      }
      String errorMessage = "Error while processing the execute statement async request";
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(
          errorMessage,
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (!apc) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("ExecuteStatement", serverError, null);
      }
      LOGGER.error("Error while processing the execute statement async request");
      throw new DatabricksSQLException(
          "Error while processing the execute statement async request",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
    String statementId = response.getStatementId();
    if (statementId == null) {
      LOGGER.error(
          "Empty Statement ID for sql {}, compute {}", logSql, computeLabel(computeResource));
      handleFailedExecution(response, "", apc ? "[REDACTED]" : sql, apc);
    }
    StatementId typedStatementId =
        StatementId.forCurrentSession(statementId, session.getSessionInfo());
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    if (parentStatement != null) {
      parentStatement.setStatementId(typedStatementId);
    }
    LOGGER.debug("Executed sql [{}] with status [{}]", logSql, response.getStatus().getState());

    // SEA async execution never returns direct results — the server always returns
    // PENDING/RUNNING state, and the client polls via getStatementResult(). No need
    // to check for CLOSED state or call markDirectResultsReceived() here.
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
  public boolean checkStatementAlive(StatementId typedStatementId) throws SQLException {
    if (typedStatementId.computeResource() != null) {
      // Returning false would be read as a terminal state; callers stop the heartbeat on
      // SQLFeatureNotSupportedException instead.
      throw new SQLFeatureNotSupportedException(
          "Result heartbeat is unsupported for all-purpose cluster SEA statements");
    }
    String statementId = typedStatementId.toSQLExecStatementId();
    // Use lightweight /status endpoint (~100 bytes) instead of full GetStatement (~21KB)
    String statusPath = statementPath(typedStatementId) + "/status";
    try {
      Request req = new Request(Request.GET, statusPath, (String) null);
      req.withHeaders(getHeaders("getStatementStatus"));
      StatementStatus status = execute(req, StatementStatus.class);
      StatementState state = status.getState();
      // Terminal states mean the operation is no longer alive
      return state != StatementState.CANCELED
          && state != StatementState.CLOSED
          && state != StatementState.FAILED;
    } catch (Exception e) {
      // Catch all exceptions — SDK can throw DatabricksError, DatabricksException,
      // IOException, or RuntimeException. All should count as transient heartbeat failures.
      LOGGER.debug("Heartbeat check failed for statement {}: {}", statementId, e.getMessage());
      throw new DatabricksSQLException(
          "Heartbeat status check failed", e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public DatabricksResultSet getStatementResult(
      StatementId typedStatementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    return getStatementResult(typedStatementId, session, parentStatement, StatementType.SQL);
  }

  @Override
  public DatabricksResultSet getStatementResult(
      StatementId typedStatementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement,
      StatementType statementType)
      throws SQLException {
    boolean apc = typedStatementId.computeResource() != null;
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    DatabricksThreadContextHolder.setSessionId(session.getSessionId());
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug("Getting statement result for statementId={}", statementId);
    GetStatementRequest request = new GetStatementRequest().setStatementId(statementId);
    String getStatusPath = targetStatementPath(typedStatementId, statementPath(typedStatementId));
    GetStatementResponse response;
    try {
      Request req =
          new Request(
              Request.GET, getStatusPath, serializeStatementRequest(typedStatementId, request));
      req.withHeaders(getHeaders("getStatement"));
      response = execute(req, GetStatementResponse.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the get statement result request";
      if (typedStatementId.computeResource() != null) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (typedStatementId.computeResource() == null) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("GetStatementResult", serverError, null);
      }
      LOGGER.error("Error while processing the get statement result request");
      throw new DatabricksSQLException(
          "Error while processing the get statement result request",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }

    // Detect cancellation before constructing ResultSet (result data is null when cancelled)
    if (response.getStatus() != null
        && response.getStatus().getState() == StatementState.CANCELED) {
      String cancelMessage = String.format("Statement [%s] was cancelled", statementId);
      LOGGER.info(cancelMessage);
      throw new DatabricksSQLException(
          cancelMessage,
          OPERATION_CANCELLED_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_CANCELLED,
          true);
    }
    if ((statementType == StatementType.METADATA || apc)
        && response.getStatus() != null
        && response.getStatus().getState() == StatementState.FAILED) {
      try {
        handleFailedExecution(
            wrapGetStatementResponse(response),
            statementId,
            statementType == StatementType.METADATA ? "[metadata statement]" : "[REDACTED]",
            apc,
            true);
      } finally {
        if (apc) {
          closeFailedApcOperation(typedStatementId, parentStatement);
        }
      }
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
  public void closeStatement(StatementId typedStatementId) throws DatabricksSQLException {
    String statementId = typedStatementId.toSQLExecStatementId();
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    LOGGER.debug("public void closeStatement(statementId={})", statementId);
    CloseStatementRequest request = new CloseStatementRequest().setStatementId(statementId);
    String path = targetStatementPath(typedStatementId, statementPath(typedStatementId));
    try {
      Request req =
          new Request(Request.DELETE, path, serializeStatementRequest(typedStatementId, request));
      req.withHeaders(getHeaders("closeStatement"));
      execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the close statement request";
      if (typedStatementId.computeResource() != null) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (typedStatementId.computeResource() == null) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("CloseStatement", serverError, null);
      }
      LOGGER.error("Error while processing the close statement request");
      throw new DatabricksSQLException(
          "Error while processing the close statement request",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public void cancelStatement(StatementId typedStatementId) throws DatabricksSQLException {
    String statementId = typedStatementId.toSQLExecStatementId();
    boolean apc = typedStatementId.computeResource() != null;
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    LOGGER.debug("public void cancelStatement(String statementId = {})", statementId);
    CancelStatementRequest request = new CancelStatementRequest().setStatementId(statementId);
    if (apc) {
      request.setBackendCompute(
          SeaComputeTarget.of(typedStatementId.computeResource()).backendCompute());
    }
    String path = statementPath(typedStatementId) + "/cancel";
    try {
      Request req =
          new Request(
              Request.POST,
              path,
              typedStatementId.computeResource() != null
                  ? apiClient.serialize(request)
                  : serializeStatementRequest(typedStatementId, request));
      req.withHeaders(getHeaders("cancelStatement"));
      execute(req, Void.class);
    } catch (IOException e) {
      String errorMessage = "Error while processing the cancel statement request";
      if (apc) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (!apc) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("CancelStatement", serverError, null);
      }
      LOGGER.error("Error while processing the cancel statement request");
      throw new DatabricksSQLException(
          "Error while processing the cancel statement request",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
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
    String path = statementPath(typedStatementId) + "/result/chunks/" + chunkIndex;
    // Bounded SEA API: always send row_offset (even 0 for chunk 0)
    path =
        targetChunkPath(
            typedStatementId,
            path,
            connectionContext.isBoundedSeaApiEnabled() || typedStatementId.computeResource() != null
                ? chunkStartRowOffset
                : null);
    try {
      Request req =
          new Request(Request.GET, path, serializeStatementRequest(typedStatementId, request));
      req.withHeaders(getHeaders("getStatementResultN"));
      ResultData resultData = execute(req, ResultData.class);
      return buildChunkLinkFetchResult(resultData.getExternalLinks());
    } catch (DatabricksException e) {
      if (typedStatementId.computeResource() != null) {
        DatabricksError serverError = findServerError(e);
        if (serverError != null) {
          throw apcServerFailure("GetResultData", serverError, null);
        }
        LOGGER.error("Error fetching APC result chunks");
        throw new DatabricksSQLException(
            "Error fetching APC result chunks",
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      String errorMessage =
          String.format(
              "Error fetching result chunks for statement [%s] chunk [%d]: %s",
              statementId, chunkIndex, e.getMessage());
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (IOException e) {
      String errorMessage = "Error while processing the get result chunk request";
      if (typedStatementId.computeResource() != null) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (typedStatementId.computeResource() == null) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("GetResultData", serverError, null);
      }
      LOGGER.error("Error fetching APC result chunks");
      throw new DatabricksSQLException(
          "Error fetching APC result chunks",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
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
    return getResultChunksData(typedStatementId, chunkIndex, 0);
  }

  /**
   * Fetches inline result data for a specific chunk, with row_offset for bounded SEA inline Arrow.
   */
  @Override
  public ResultData getResultChunksData(
      StatementId typedStatementId, long chunkIndex, long rowOffset) throws DatabricksSQLException {
    DatabricksThreadContextHolder.setStatementId(typedStatementId);
    String statementId = typedStatementId.toSQLExecStatementId();
    LOGGER.debug(
        "getResultChunksData(statementId={}, chunkIndex={}, rowOffset={})",
        statementId,
        chunkIndex,
        rowOffset);
    GetStatementResultChunkNRequest request =
        new GetStatementResultChunkNRequest().setStatementId(statementId).setChunkIndex(chunkIndex);
    String path = statementPath(typedStatementId) + "/result/chunks/" + chunkIndex;
    path =
        targetChunkPath(
            typedStatementId,
            path,
            (connectionContext.isBoundedSeaApiEnabled() && rowOffset > 0)
                    || typedStatementId.computeResource() != null
                ? rowOffset
                : null);
    try {
      Request req =
          new Request(Request.GET, path, serializeStatementRequest(typedStatementId, request));
      req.withHeaders(getHeaders("getStatementResultN"));
      return execute(req, ResultData.class);
    } catch (DatabricksException e) {
      if (typedStatementId.computeResource() != null) {
        DatabricksError serverError = findServerError(e);
        if (serverError != null) {
          throw apcServerFailure("GetResultData", serverError, null);
        }
        LOGGER.error("Error fetching APC result data");
        throw new DatabricksSQLException(
            "Error fetching APC result data",
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      String errorMessage =
          String.format(
              "Error fetching result data for statement [%s] chunk [%d]: %s",
              statementId, chunkIndex, e.getMessage());
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (IOException e) {
      String errorMessage = "Error while processing the get result chunk request";
      if (typedStatementId.computeResource() != null) {
        LOGGER.error(errorMessage);
        throw new DatabricksSQLException(
            errorMessage,
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
            DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
      }
      LOGGER.error(errorMessage, e);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    } catch (RuntimeException e) {
      if (typedStatementId.computeResource() == null) {
        throw e;
      }
      DatabricksError serverError = findServerError(e);
      if (serverError != null) {
        throw apcServerFailure("GetResultData", serverError, null);
      }
      LOGGER.error("Error fetching APC result data");
      throw new DatabricksSQLException(
          "Error fetching APC result data",
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR.name(),
          DatabricksDriverErrorCode.SDK_CLIENT_ERROR);
    }
  }

  @Override
  public synchronized void resetAccessToken(String newAccessToken) {
    this.clientConfigurator.resetAccessTokenInConfig(newAccessToken);
    this.apiClient = newApiClient();
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

  private Map<String, String> getHeaders(String method) {
    return getHeaders(method, null, false, null);
  }

  private Map<String, String> getHeaders(
      String method, StatementType statementType, boolean isAsync) {
    return getHeaders(method, statementType, isAsync, null);
  }

  private Map<String, String> getHeaders(
      String method,
      StatementType statementType,
      boolean isAsync,
      Map<String, String> additionalHeaders) {
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

    // Add any additional headers passed by caller (e.g., metadata operation type)
    if (additionalHeaders != null && !additionalHeaders.isEmpty()) {
      headers.putAll(additionalHeaders);
      LOGGER.debug("Adding additional headers: {}", additionalHeaders);
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

  private static boolean isShowMetadata(
      StatementType statementType, MetadataOperationType operationType) {
    return statementType == StatementType.METADATA
        && operationType != null
        && operationType.isShowOperation();
  }

  private ExecuteStatementRequest getRequest(
      StatementType statementType,
      String sql,
      IDatabricksComputeResource computeResource,
      IDatabricksSession session,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksStatementInternal parentStatement,
      boolean executeAsync,
      boolean showMetadata)
      throws SQLException {
    boolean apc = SeaComputeTarget.of(computeResource).isApc();
    boolean arrowEnabled = connectionContext.shouldEnableArrow();
    boolean cloudFetch = arrowEnabled && connectionContext.isCloudFetchEnabled();
    boolean boundedSeaInline =
        !apc && arrowEnabled && connectionContext.isBoundedSeaApiEnabled() && !cloudFetch;
    if (apc && !arrowEnabled) {
      LOGGER.warn("EnableArrow=0 is ignored for all-purpose cluster SEA; Arrow batches are used");
    }
    if (apc && !parameters.isEmpty()) {
      if (ApcParameterCapabilities.supportsNativeParameters(session)) {
        ApcParameterCapabilities.rejectComplexBindings(parameters);
      } else {
        String reason = ApcParameterCapabilities.downgradeReason(session);
        if (!(parentStatement instanceof DatabricksPreparedStatement)) {
          if (apcDowngradeReported.compareAndSet(false, true)) {
            LOGGER.warn("APC parameter downgrade: {}", reason);
            TelemetryHelper.exportFailureLog(
                connectionContext, "APC_PARAMETER_DOWNGRADE", reason, TelemetryLogLevel.WARN);
          } else {
            LOGGER.debug("APC parameter downgrade: {}", reason);
          }
        }
        sql = SQLInterpolator.interpolateApcSQL(sql, parameters);
        parameters = Collections.emptyMap();
      }
    }
    Format format = (cloudFetch || boundedSeaInline) ? Format.ARROW_STREAM : Format.JSON_ARRAY;
    Disposition disposition;
    if (cloudFetch) {
      disposition =
          connectionContext.isSqlExecHybridResultsEnabled()
              ? Disposition.INLINE_OR_EXTERNAL_LINKS
              : Disposition.EXTERNAL_LINKS;
    } else if (boundedSeaInline) {
      disposition = Disposition.INLINE_OR_EXTERNAL_LINKS;
    } else {
      disposition = Disposition.INLINE;
    }
    long maxRows =
        (parentStatement == null) ? DEFAULT_RESULT_ROW_LIMIT : parentStatement.getMaxRows();
    CompressionCodec compressionCodec = session.getCompressionCodec();
    if (!apc && format.equals(Format.JSON_ARRAY)) {
      LOGGER.debug("Results are inline JSON, skipping compression.");
      compressionCodec = CompressionCodec.NONE;
    }
    List<StatementParameterListItem> parameterListItems =
        parameters.values().stream()
            .map(parameter -> mapToParameterListItem(parameter, apc))
            .collect(Collectors.toList());
    ExecuteStatementRequest request =
        (apc ? new ApcExecuteStatementRequest() : new ExecuteStatementRequest())
            .setStatement(sql)
            .setDisposition(apc ? Disposition.INLINE_OR_EXTERNAL_LINKS : disposition)
            .setFormat(
                apc
                    ? (showMetadata
                        ? SqlExecResultFormat.ARROW_STREAM
                        : SqlExecResultFormat.ARROW_BATCH)
                    : SqlExecResultFormat.fromSdkFormat(format))
            .setResultCompression(compressionCodec);
    if (!apc || !parameterListItems.isEmpty()) {
      request.setParameters(parameterListItems);
    }
    SeaComputeTarget target = SeaComputeTarget.of(computeResource);
    if (target.isApc() || !session.getSessionInfo().realtimeSession()) {
      request.setSessionId(session.getSessionId());
      if (target.isApc()) {
        request.setBackendCompute(target.backendCompute());
      } else {
        request.setWarehouseId(target.warehouseId());
      }
    }
    SessionVersion sessionVersion = session.getSessionVersion();
    if (sessionVersion != null) {
      request.setSessionVersion(sessionVersion);
    }
    if (executeAsync) {
      request.setWaitTimeout(ASYNC_TIMEOUT_VALUE);
    } else {
      // DirectResults off -> async (0s); avoids truncation (ES-1714092)
      if (!connectionContext.getDirectResultMode()) {
        request.setWaitTimeout(ASYNC_TIMEOUT_VALUE);
      }
      request.setOnWaitTimeout(ExecuteStatementRequestOnWaitTimeout.CONTINUE);
    }
    if (maxRows > 0) {
      request.setRowLimit(maxRows);
    }
    return request;
  }

  private ExecuteStatementRequest getBatchRequest(
      StatementType statementType,
      String sql,
      IDatabricksComputeResource computeResource,
      IDatabricksSession session,
      List<BatchParameterSet> parameterSets,
      IDatabricksStatementInternal parentStatement)
      throws SQLException {
    ExecuteStatementRequest request =
        getRequest(
            statementType,
            sql,
            computeResource,
            session,
            Collections.emptyMap(),
            parentStatement,
            false,
            false);
    List<StatementParameterSet> seaParameterSets =
        parameterSets.stream()
            .map(
                parameterSet ->
                    new StatementParameterSet()
                        .setParameters(
                            parameterSet.getParameters().stream()
                                .map(
                                    parameter ->
                                        mapToParameterListItem(parameter, parameter.cardinal() - 1))
                                .collect(Collectors.toList())))
            .collect(Collectors.toList());
    return request.setParameterSets(seaParameterSets).unsetRowLimit();
  }

  @VisibleForTesting
  StatementParameterListItem mapToParameterListItem(ImmutableSqlParameter parameter) {
    return mapToParameterListItem(parameter, parameter.cardinal());
  }

  private StatementParameterListItem mapToParameterListItem(
      ImmutableSqlParameter parameter, int ordinal) {
    return mapToParameterListItem(parameter, ordinal, false);
  }

  private StatementParameterListItem mapToParameterListItem(
      ImmutableSqlParameter parameter, boolean apc) {
    return mapToParameterListItem(parameter, parameter.cardinal(), apc);
  }

  private StatementParameterListItem mapToParameterListItem(
      ImmutableSqlParameter parameter, int ordinal, boolean apc) {
    Object value = parameter.value();
    String typeString = parameter.type().name();
    if (apc && parameter.type() == ColumnInfoTypeName.BINARY && value instanceof byte[]) {
      // SEA's parameter value is a JSON string. Keep arbitrary bytes intact by decoding the
      // hex value at the matching native placeholder with unhex(?).
      return new PositionalStatementParameterListItem()
          .setOrdinal(parameter.cardinal())
          .setType("STRING")
          .setValue(SQLInterpolator.binaryHex((byte[]) value));
    }
    if (typeString.equals(DECIMAL) && value instanceof BigDecimal) {
      typeString = getDecimalTypeString((BigDecimal) value);
    }
    return new PositionalStatementParameterListItem()
        .setOrdinal(ordinal)
        .setType(typeString)
        .setValue(value != null ? value.toString() : null);
  }

  /** Handles a failed execution and throws appropriate exception */
  void handleFailedExecution(
      ExecuteStatementResponse response, String statementId, String statement) throws SQLException {
    handleFailedExecution(response, statementId, statement, false);
  }

  private void handleFailedExecution(
      ExecuteStatementResponse response,
      String statementId,
      String statement,
      boolean redactApcDetails)
      throws SQLException {
    handleFailedExecution(response, statementId, statement, redactApcDetails, false);
  }

  private void handleFailedExecution(
      ExecuteStatementResponse response,
      String statementId,
      String statement,
      boolean redactApcDetails,
      boolean includeTimeoutVendorCode)
      throws SQLException {
    StatementState statementState = response.getStatus().getState();
    ServiceError error = response.getStatus().getError();

    // Distinguish cancellation from failure — silentExceptions=true so cancellations
    // (common in BI tools like Tableau/Looker) don't emit ERROR-level telemetry
    if (statementState == StatementState.CANCELED) {
      String cancelMessage = String.format("Statement [%s] was cancelled", statementId);
      LOGGER.info(cancelMessage);
      throw new DatabricksSQLException(
          cancelMessage,
          OPERATION_CANCELLED_SQLSTATE,
          DatabricksDriverErrorCode.EXECUTE_STATEMENT_CANCELLED,
          true);
    }

    String errorMessage =
        String.format(
            "Statement execution failed %s -> %s\n%s.", statementId, statement, statementState);
    // The APC server may echo SQL or bound values in its error text. Keep the raw text only for
    // SQL-state classification; callers and logs receive a redacted message.
    String rawErrorMessage = errorMessage;
    if (error != null) {
      String safeCode = safeServerErrorCode(String.valueOf(error.getErrorCode()));
      rawErrorMessage +=
          String.format(
              " Error Message: %s, Error code: %s", error.getMessage(), error.getErrorCode());
      errorMessage +=
          redactApcDetails
              ? String.format(" Error code: %s", safeCode)
              : String.format(
                  " Error Message: %s, Error code: %s", error.getMessage(), error.getErrorCode());
      if (redactApcDetails) {
        String notFoundIndicator =
            MetadataResultConstants.objectNotFoundIndicator(error.getMessage());
        if (notFoundIndicator != null) {
          errorMessage += ", Error class: " + notFoundIndicator;
        }
      }
    }
    LOGGER.debug(errorMessage);

    String sqlState = response.getStatus().getSqlState();
    if (QUERY_EXECUTION_TIMEOUT_SQLSTATE.equals(sqlState)) {
      throw new DatabricksTimeoutException(
          errorMessage,
          null,
          DatabricksDriverErrorCode.OPERATION_TIMEOUT_ERROR,
          includeTimeoutVendorCode);
    }
    String remappedSqlState = classifyTransientSqlState(rawErrorMessage, sqlState);
    if (!Objects.equals(remappedSqlState, sqlState)) {
      LOGGER.info(
          "Remapped SQL state [{}] -> [{}] for transient error pattern in SEA statement [{}]",
          sqlState,
          remappedSqlState,
          statementId);
    }
    throw new DatabricksSQLException(
        errorMessage, remappedSqlState, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
  }

  private ExecuteStatementResponse wrapGetStatementResponse(
      GetStatementResponse getStatementResponse) {
    return new ExecuteStatementResponse()
        .setStatementId(getStatementResponse.getStatementId())
        .setStatus(getStatementResponse.getStatus())
        .setManifest(getStatementResponse.getManifest())
        .setResult(getStatementResponse.getResult());
  }

  private void updateSessionVersion(IDatabricksSession session, StatementStatus status) {
    if (session != null && status != null) {
      session.updateSessionVersion(status.getSessionVersion());
    }
  }

  /**
   * Builds actionable error messages for SSL handshake failures. Returns a generic message if the
   * error is not SSL-related.
   */
  private String buildErrorMessage(DatabricksError e) {

    boolean isSSLException =
        Stream.iterate(e.getCause(), Objects::nonNull, Throwable::getCause)
            .anyMatch(cause -> cause instanceof SSLHandshakeException);

    String detail = e.getMessage();
    boolean isCertificatePathError =
        detail != null
            && (detail.contains("PKIX path building failed")
                || detail.contains("unable to find valid certification path"));

    if (isSSLException && isCertificatePathError) {
      return buildSSLCertificatePathErrorMessage(e);
    }

    if (detail == null || detail.isEmpty()) {
      return "Error while establishing a connection in databricks";
    }
    String message = "Error while establishing a connection in databricks: " + detail;
    int status = e.getStatusCode();
    return status > 0 ? message + " (HTTP " + status + ")" : message;
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
