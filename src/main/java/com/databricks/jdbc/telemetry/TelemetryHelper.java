package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.QUERY_TAGS;
import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.ProcessNameUtil;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.client.thrift.generated.TSparkRowSetType;
import com.databricks.jdbc.model.telemetry.*;
import com.databricks.jdbc.model.telemetry.latency.OperationType;
import com.databricks.jdbc.telemetry.latency.TelemetryCollector;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.databricks.sdk.service.sql.Format;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TelemetryHelper {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryHelper.class);
  public static final String DEFAULT_HOST = "unknown-host";
  // Cache to store unique DriverConnectionParameters for each connectionUuid
  private static final ConcurrentHashMap<String, DriverConnectionParameters>
      connectionParameterCache = new ConcurrentHashMap<>();
  private static final String APP_NAME_SYSTEM_PROPERTY = "app.name";

  @VisibleForTesting
  static final String TELEMETRY_FEATURE_FLAG_NAME =
      "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetryForJdbc";

  private static final DriverSystemConfiguration DRIVER_SYSTEM_CONFIGURATION =
      new DriverSystemConfiguration()
          .setCharSetEncoding(Charset.defaultCharset().displayName())
          .setDriverName(DriverUtil.getDriverName())
          .setDriverVersion(DriverUtil.getDriverVersion())
          .setLocaleName(
              System.getProperty("user.language") + '_' + System.getProperty("user.country"))
          .setRuntimeVendor(System.getProperty("java.vendor"))
          .setRuntimeVersion(System.getProperty("java.version"))
          .setRuntimeName(System.getProperty("java.vm.name"))
          .setOsArch(System.getProperty("os.arch"))
          .setOsVersion(System.getProperty("os.version"))
          .setOsName(System.getProperty("os.name"))
          .setProcessName(ProcessNameUtil.getProcessName())
          .setClientAppName(null);

  public static DriverSystemConfiguration getDriverSystemConfiguration() {
    return DRIVER_SYSTEM_CONFIGURATION;
  }

  public static boolean isTelemetryAllowedForConnection(IDatabricksConnectionContext context) {
    if (context == null || context.getTelemetryLogLevel().equals(TelemetryLogLevel.OFF)) {
      return false;
    }
    if (context.forceEnableTelemetry()) {
      return true;
    }
    return context.isTelemetryEnabled()
        && DatabricksDriverFeatureFlagsContextFactory.getInstance(context)
            .isFeatureEnabled(TELEMETRY_FEATURE_FLAG_NAME);
  }

  public static void exportTelemetryLog(
      StatementTelemetryDetails telemetryDetails, TelemetryLogLevel logLevel) {
    exportTelemetryEvent(
        DatabricksThreadContextHolder.getConnectionContext(),
        telemetryDetails,
        null,
        null,
        logLevel);
  }

  private static void exportTelemetryEvent(
      IDatabricksConnectionContext connectionContext,
      StatementTelemetryDetails telemetryDetails,
      DriverErrorInfo errorInfo,
      Long chunkIndex,
      TelemetryLogLevel logLevel) {
    if (connectionContext == null
        || telemetryDetails == null
        || logLevel.toInt() > connectionContext.getTelemetryLogLevel().toInt()) {
      // We don't export telemetry logs in the following three scenarios:
      // 1. When the context is not set.
      // 2. When telemetry details are not set.
      // 3. When the event's log level is more verbose than the configured telemetry log level.
      // In any of these cases, export is skipped.
      return;
    }
    TelemetryEvent telemetryEvent =
        new TelemetryEvent()
            .setDriverSystemConfiguration(DRIVER_SYSTEM_CONFIGURATION)
            .setDriverConnectionParameters(getDriverConnectionParameter(connectionContext))
            .setSessionId(DatabricksThreadContextHolder.getSessionId())
            .setDriverErrorInfo(errorInfo) // This is only set for failure logs
            .setSqlStatementId(telemetryDetails.getStatementId())
            .setLatency(telemetryDetails.getOperationLatencyMillis());
    SqlExecutionEvent sqlExecutionEvent =
        new SqlExecutionEvent()
            .setChunkDetails(telemetryDetails.getChunkDetails())
            .setResultLatency(telemetryDetails.getResultLatency())
            .setOperationDetail(telemetryDetails.getOperationDetail())
            .setExecutionResultFormat(telemetryDetails.getExecutionResultFormat())
            .setChunkId(chunkIndex); // This is only set for chunk download failure logs
    telemetryEvent.setSqlOperation(sqlExecutionEvent);

    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog(logLevel)
            .setFrontendLogEventId(getEventUUID())
            .setContext(getLogContext())
            .setEntry(new FrontendLogEntry().setSqlDriverLog(telemetryEvent));
    TelemetryClientFactory.getInstance()
        .getTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String errorMessage,
      TelemetryLogLevel logLevel) {
    String statementId = DatabricksThreadContextHolder.getStatementId();
    exportFailureLog(
        connectionContext, errorName, errorMessage, statementId, /* chunkIndex */ null, logLevel);
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String errorMessage,
      String statementId,
      Long chunkIndex,
      TelemetryLogLevel logLevel) {
    DriverErrorInfo errorInfo =
        new DriverErrorInfo().setErrorName(errorName).setStackTrace(errorMessage);
    StatementTelemetryDetails telemetryDetails;
    if (statementId == null) {
      telemetryDetails = new StatementTelemetryDetails(null);
    } else {
      TelemetryCollector collector =
          TelemetryCollectorManager.getInstance().getOrCreateCollector(connectionContext);
      telemetryDetails = collector.getOrCreateTelemetryDetails(statementId);
    }
    exportTelemetryEvent(connectionContext, telemetryDetails, errorInfo, chunkIndex, logLevel);
  }

  public static String getStatementIdString(StatementId statementId) {
    return statementId != null
        ? statementId.toSQLExecStatementId()
        : DatabricksThreadContextHolder.getStatementId();
  }

  private static DriverConnectionParameters getDriverConnectionParameter(
      IDatabricksConnectionContext connectionContext) {
    if (connectionContext == null) {
      return null;
    }
    return connectionParameterCache.computeIfAbsent(
        connectionContext.getConnectionUuid(),
        uuid -> buildDriverConnectionParameters(connectionContext));
  }

  private static DriverConnectionParameters buildDriverConnectionParameters(
      IDatabricksConnectionContext connectionContext) {
    String hostUrl;
    try {
      hostUrl = connectionContext.getHostUrl();
    } catch (DatabricksParsingException e) {
      hostUrl = "Error in parsing host url";
      // This would mean, telemetry data cannot be sent.
      return null;
    }
    DriverConnectionParameters connectionParameters = new DriverConnectionParameters();
    try {
      connectionParameters
          .setHostDetails(getHostDetails(hostUrl))
          .setUseProxy(connectionContext.getUseProxy())
          .setAuthMech(connectionContext.getAuthMech())
          .setAuthScope(connectionContext.getAuthScope())
          .setUseSystemProxy(connectionContext.getUseSystemProxy())
          .setUseCfProxy(connectionContext.getUseCloudFetchProxy())
          .setDriverAuthFlow(connectionContext.getAuthFlow())
          .setDiscoveryModeEnabled(connectionContext.isOAuthDiscoveryModeEnabled())
          .setDiscoveryUrl(connectionContext.getOAuthDiscoveryURL())
          .setIdentityFederationClientId(connectionContext.getIdentityFederationClientId())
          .setUseEmptyMetadata(connectionContext.getUseEmptyMetadata())
          .setSupportManyParameters(connectionContext.supportManyParameters())
          .setGoogleCredentialFilePath(connectionContext.getGoogleCredentials())
          .setGoogleServiceAccount(connectionContext.getGoogleServiceAccount())
          .setAllowedVolumeIngestionPaths(connectionContext.getVolumeOperationAllowedPaths())
          .setSocketTimeout(connectionContext.getSocketTimeout())
          .setStringColumnLength(connectionContext.getDefaultStringColumnLength())
          .setEnableComplexDatatypeSupport(connectionContext.isComplexDatatypeSupportEnabled())
          .setEnableGeoSpatialSupport(connectionContext.isGeoSpatialSupportEnabled())
          .setAzureWorkspaceResourceId(connectionContext.getAzureWorkspaceResourceId())
          .setAzureTenantId(connectionContext.getAzureTenantId())
          .setSslTrustStoreType(connectionContext.getSSLTrustStoreType())
          .setEnableArrow(connectionContext.shouldEnableArrow())
          .setEnableDirectResults(connectionContext.getDirectResultMode())
          .setCheckCertificateRevocation(connectionContext.checkCertificateRevocation())
          .setAcceptUndeterminedCertificateRevocation(
              connectionContext.acceptUndeterminedCertificateRevocation())
          .setDriverMode(
              connectionContext.getClientType() != null
                  ? connectionContext.getClientType().toString()
                  : "UNKNOWN")
          .setAuthEndpoint(connectionContext.getAuthEndpoint())
          .setTokenEndpoint(connectionContext.getTokenEndpoint())
          .setNonProxyHosts(StringUtil.split(connectionContext.getNonProxyHosts()))
          .setHttpConnectionPoolSize(connectionContext.getHttpConnectionPoolSize())
          .setEnableSeaHybridResults(connectionContext.isSqlExecHybridResultsEnabled())
          .setAllowSelfSignedSupport(connectionContext.allowSelfSignedCerts())
          .setUseSystemTrustStore(connectionContext.useSystemTrustStore())
          .setRowsFetchedPerBlock(connectionContext.getRowsFetchedPerBlock())
          .setAsyncPollIntervalMillis(connectionContext.getAsyncExecPollInterval())
          .setEnableTokenCache(connectionContext.isTokenCacheEnabled())
          .setHttpPath(connectionContext.getHttpPath())
          .setEnableMetricViewMetadata(connectionContext.getEnableMetricViewMetadata())
          .setQueryTags(connectionContext.getSessionConfigs().get(QUERY_TAGS));
    } catch (DatabricksValidationException e) {
      // If configuration validation fails, return null to skip telemetry export
      // This prevents invalid configuration from breaking telemetry
      return null;
    }
    if (connectionContext.useJWTAssertion()) {
      connectionParameters
          .setEnableJwtAssertion(true)
          .setJwtAlgorithm(connectionContext.getJWTAlgorithm())
          .setJwtKeyFile(connectionContext.getJWTKeyFile());
    }
    if (connectionContext.getUseCloudFetchProxy()) {
      connectionParameters.setCfProxyHostDetails(
          getHostDetails(
              connectionContext.getCloudFetchProxyHost(),
              connectionContext.getCloudFetchProxyPort(),
              connectionContext.getCloudFetchProxyAuthType()));
    }
    if (connectionContext.getUseProxy()) {
      HostDetails hostDetails =
          getHostDetails(
              connectionContext.getProxyHost(),
              connectionContext.getProxyPort(),
              connectionContext.getProxyAuthType());
      hostDetails.setNonProxyHosts(connectionContext.getNonProxyHosts());
      connectionParameters.setProxyHostDetails(hostDetails);
    } else if (connectionContext.getUseSystemProxy()) {
      String protocol = System.getProperty("https.proxyHost") != null ? "https" : "http";
      connectionParameters.setProxyHostDetails(
          getHostDetails(
              System.getProperty(protocol + ".proxyHost"),
              Integer.parseInt(System.getProperty(protocol + ".proxyPort")),
              connectionContext.getProxyAuthType()));
    }
    return connectionParameters;
  }

  private static String getEventUUID() {
    return UUID.randomUUID().toString();
  }

  private static FrontendLogContext getLogContext() {
    return new FrontendLogContext()
        .setClientContext(
            new TelemetryClientContext()
                .setTimestampMillis(Instant.now().toEpochMilli())
                .setUserAgent(UserAgent.asString()));
  }

  private static HostDetails getHostDetails(
      String host, int port, ProxyConfig.ProxyAuthType proxyAuthType) {
    return new HostDetails().setHostUrl(host).setPort(port).setProxyType(proxyAuthType);
  }

  private static HostDetails getHostDetails(String host) {
    return new HostDetails().setHostUrl(host);
  }

  public static DatabricksConfig getDatabricksConfigSafely(IDatabricksConnectionContext context) {
    try {
      return DatabricksClientConfiguratorManager.getInstance()
          .getConfiguratorOnlyIfExists(context)
          .getDatabricksConfig();
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Connection config is not available, using no-auth telemetry client. Error: %s; Context: %s",
              e.getMessage(), context);
      LOGGER.trace(errorMessage);
      return null;
    }
  }

  // Add mapping function for method name to OperationType
  public static OperationType mapMethodToOperationType(String methodName) {
    if (methodName == null) return OperationType.TYPE_UNSPECIFIED;
    switch (methodName) {
      case "createSession":
        return OperationType.CREATE_SESSION;
      case "executeStatement":
        return OperationType.EXECUTE_STATEMENT;
      case "executeStatementAsync":
        return OperationType.EXECUTE_STATEMENT_ASYNC;
      case "closeStatement":
        return OperationType.CLOSE_STATEMENT;
      case "cancelStatement":
        return OperationType.CANCEL_STATEMENT;
      case "deleteSession":
        return OperationType.DELETE_SESSION;
      case "listCrossReferences":
        return OperationType.LIST_CROSS_REFERENCES;
      case "listExportedKeys":
        return OperationType.LIST_EXPORTED_KEYS;
      case "listImportedKeys":
        return OperationType.LIST_IMPORTED_KEYS;
      case "listPrimaryKeys":
        return OperationType.LIST_PRIMARY_KEYS;
      case "listFunctions":
        return OperationType.LIST_FUNCTIONS;
      case "listColumns":
        return OperationType.LIST_COLUMNS;
      case "listTableTypes":
        return OperationType.LIST_TABLE_TYPES;
      case "listTables":
        return OperationType.LIST_TABLES;
      case "listSchemas":
        return OperationType.LIST_SCHEMAS;
      case "listCatalogs":
        return OperationType.LIST_CATALOGS;
      case "listTypeInfo":
        return OperationType.LIST_TYPE_INFO;
      default:
        return OperationType.TYPE_UNSPECIFIED;
    }
  }

  /**
   * Sets/updates client app name in telemetry
   *
   * @param connectionContext The connection context
   * @param clientInfoAppName The application name from client info properties, can be null
   */
  public static void updateTelemetryAppName(
      IDatabricksConnectionContext connectionContext, String clientInfoAppName) {
    String appName = determineApplicationName(connectionContext, clientInfoAppName);
    if (!isNullOrEmpty(appName)) {
      DRIVER_SYSTEM_CONFIGURATION.setClientAppName(appName);
    }
  }

  /**
   * Determines the application name using a fallback mechanism: 1. useragententry url param 2.
   * applicationname url param 3. client info property "applicationname" 4. System property app.name
   *
   * @param connectionContext The connection context
   * @param clientInfoAppName The application name from client info properties, can be null
   * @return The determined application name or null if none is found
   */
  @VisibleForTesting
  static String determineApplicationName(
      IDatabricksConnectionContext connectionContext, String clientInfoAppName) {
    // First check URL params
    String appName = connectionContext.getCustomerUserAgent();
    if (!isNullOrEmpty(appName)) {
      return appName;
    }

    // Then check application name URL param
    appName = connectionContext.getApplicationName();
    if (!isNullOrEmpty(appName)) {
      return appName;
    }

    // Then check client info property
    if (!isNullOrEmpty(clientInfoAppName)) {
      return clientInfoAppName;
    }

    // Finally check system property
    return System.getProperty(APP_NAME_SYSTEM_PROPERTY);
  }

  public static String keyOf(IDatabricksConnectionContext context) {
    if (context == null || context.getHost() == null) {
      return DEFAULT_HOST;
    }
    return context.getHost();
  }

  /**
   * Removes cached connection parameters for the given connection UUID. Should be called when a
   * connection is closed to prevent memory leaks.
   *
   * @param connectionUuid The connection UUID whose cached parameters should be removed
   */
  public static void removeConnectionParameters(String connectionUuid) {
    if (connectionUuid != null) {
      connectionParameterCache.remove(connectionUuid);
    }
  }

  /** Clears all cached connection parameters. This should only be used for testing purposes. */
  @VisibleForTesting
  static void clearConnectionParameterCache() {
    connectionParameterCache.clear();
  }

  // Simplified telemetry recording methods that handle getting the collector internally

  /**
   * Records the total chunks for a statement. Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param statementId The statement ID
   * @param chunkCount The total number of chunks
   */
  public static void recordTotalChunks(
      IDatabricksConnectionContext connectionContext, StatementId statementId, long chunkCount) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .recordTotalChunks(statementId, chunkCount);
      }
    } catch (Exception e) {
      LOGGER.trace("Error recording total chunks telemetry: {}", e.getMessage());
    }
  }

  /**
   * Sets the result format for a statement (SQL Execution API). Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param statementId The statement ID
   * @param format The result format
   */
  public static void setResultFormat(
      IDatabricksConnectionContext connectionContext, StatementId statementId, Format format) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .setResultFormat(statementId, format);
      }
    } catch (Exception e) {
      LOGGER.trace("Error setting result format telemetry: {}", e.getMessage());
    }
  }

  /**
   * Sets the result format for a statement (Thrift API). Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param parentStatement The parent statement
   * @param format The result format
   */
  public static void setResultFormat(
      IDatabricksConnectionContext connectionContext,
      IDatabricksStatementInternal parentStatement,
      TSparkRowSetType format) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .setResultFormat(parentStatement, format);
      }
    } catch (Exception e) {
      LOGGER.trace("Error setting result format telemetry: {}", e.getMessage());
    }
  }

  /**
   * Records a result set iteration. Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param statementId The statement ID
   * @param chunkCount The total number of chunks
   * @param hasNext Whether there are more rows
   */
  public static void recordResultSetIteration(
      IDatabricksConnectionContext connectionContext,
      String statementId,
      long chunkCount,
      boolean hasNext) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .recordResultSetIteration(statementId, chunkCount, hasNext);
      }
    } catch (Exception e) {
      LOGGER.trace("Error recording result set iteration telemetry: {}", e.getMessage());
    }
  }

  /**
   * Records a result set iteration from a parent statement. Extracts connection context safely and
   * silently ignores all errors.
   *
   * @param parentStatement The parent statement (can be null)
   * @param statementId The statement ID
   * @param chunkCount The total number of chunks (can be null)
   * @param hasNext Whether there are more rows
   */
  public static void recordResultSetIteration(
      IDatabricksStatementInternal parentStatement,
      StatementId statementId,
      Long chunkCount,
      boolean hasNext) {
    try {
      if (parentStatement != null && chunkCount != null) {
        IDatabricksConnectionContext connectionContext =
            ((DatabricksConnection) parentStatement.getStatement().getConnection())
                .getConnectionContext();
        recordResultSetIteration(
            connectionContext, statementId.toSQLExecStatementId(), chunkCount, hasNext);
      }
    } catch (Exception e) {
      LOGGER.trace("Error getting connection context for telemetry: {}", e.getMessage());
    }
  }

  /**
   * Records get operation status latency. Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param statementId The statement ID
   * @param latencyMillis The operation latency in milliseconds
   */
  public static void recordGetOperationStatus(
      IDatabricksConnectionContext connectionContext, String statementId, long latencyMillis) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .recordGetOperationStatus(statementId, latencyMillis);
      }
    } catch (Exception e) {
      LOGGER.trace("Error recording get operation status telemetry: {}", e.getMessage());
    }
  }

  /**
   * Records chunk download latency. Silently ignores errors.
   *
   * @param connectionContext The connection context
   * @param statementId The statement ID
   * @param chunkIndex The chunk index
   * @param latencyMillis The download latency in milliseconds
   */
  public static void recordChunkDownloadLatency(
      IDatabricksConnectionContext connectionContext,
      String statementId,
      long chunkIndex,
      long latencyMillis) {
    try {
      if (connectionContext != null) {
        TelemetryCollectorManager.getInstance()
            .getOrCreateCollector(connectionContext)
            .recordChunkDownloadLatency(statementId, chunkIndex, latencyMillis);
      }
    } catch (Exception e) {
      LOGGER.trace("Error recording chunk download latency telemetry: {}", e.getMessage());
    }
  }
}
