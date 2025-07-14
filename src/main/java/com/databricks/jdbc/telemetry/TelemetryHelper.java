package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.ProcessNameUtil;
import com.databricks.jdbc.common.util.StringUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.*;
import com.databricks.jdbc.model.telemetry.latency.ChunkDetails;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TelemetryHelper {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryHelper.class);
  // Cache to store unique DriverConnectionParameters for each connectionUuid
  private static final ConcurrentHashMap<String, DriverConnectionParameters>
      connectionParameterCache = new ConcurrentHashMap<>();

  @VisibleForTesting
  static final String TELEMETRY_FEATURE_FLAG_NAME =
      "databricks.partnerplatform.clientConfigsFeatureFlags.enableTelemetry";

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

  public static void updateClientAppName(String clientAppName) {
    if (!isNullOrEmpty(clientAppName)) {
      DRIVER_SYSTEM_CONFIGURATION.setClientAppName(clientAppName);
    }
  }

  public static boolean isTelemetryAllowedForConnection(IDatabricksConnectionContext context) {
    if (context.forceEnableTelemetry()) {
      return true;
    }
    return context != null
        && context.isTelemetryEnabled()
        && DatabricksDriverFeatureFlagsContextFactory.getInstance(context)
            .isFeatureEnabled(TELEMETRY_FEATURE_FLAG_NAME);
  }

  public static void exportInitialTelemetryLog(IDatabricksConnectionContext connectionContext) {
    if (connectionContext == null) {
      return;
    }
    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog()
            .setFrontendLogEventId(getEventUUID())
            .setContext(getLogContext())
            .setEntry(
                new FrontendLogEntry()
                    .setSqlDriverLog(
                        new TelemetryEvent()
                            .setDriverConnectionParameters(
                                getDriverConnectionParameter(connectionContext))
                            .setDriverSystemConfiguration(getDriverSystemConfiguration())));
    TelemetryClientFactory.getInstance()
        .getTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext, String errorName, String errorMessage) {
    exportFailureLog(
        connectionContext,
        errorName,
        errorMessage,
        null,
        DatabricksThreadContextHolder.getStatementId());
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String errorMessage,
      Long chunkIndex,
      String statementId) {

    // Connection context is not set in following scenarios:
    // a. Unit tests
    // b. When Url parsing has failed
    // In either of these scenarios, we don't export logs
    if (connectionContext != null) {
      DriverErrorInfo errorInfo =
          new DriverErrorInfo().setErrorName(errorName).setStackTrace(errorMessage);
      TelemetryFrontendLog telemetryFrontendLog =
          new TelemetryFrontendLog()
              .setFrontendLogEventId(getEventUUID())
              .setContext(getLogContext())
              .setEntry(
                  new FrontendLogEntry()
                      .setSqlDriverLog(
                          new TelemetryEvent()
                              .setSqlStatementId(statementId)
                              .setDriverConnectionParameters(
                                  getDriverConnectionParameter(connectionContext))
                              .setDriverErrorInfo(errorInfo)
                              .setDriverSystemConfiguration(getDriverSystemConfiguration())));
      if (chunkIndex != null) {
        // When chunkIndex is provided, we are exporting a chunk download failure log
        telemetryFrontendLog
            .getEntry()
            .getSqlDriverLog()
            .setSqlOperation(new SqlExecutionEvent().setChunkId(chunkIndex));
      }
      ITelemetryClient client =
          TelemetryClientFactory.getInstance().getTelemetryClient(connectionContext);
      client.exportEvent(telemetryFrontendLog);
    }
  }

  public static void exportLatencyLog(long executionTime) {
    SqlExecutionEvent executionEvent =
        new SqlExecutionEvent()
            .setDriverStatementType(DatabricksThreadContextHolder.getStatementType())
            .setRetryCount(DatabricksThreadContextHolder.getRetryCount());
    exportLatencyLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        executionTime,
        executionEvent,
        DatabricksThreadContextHolder.getStatementId(),
        DatabricksThreadContextHolder.getSessionId());
  }

  public static void exportChunkLatencyTelemetry(ChunkDetails chunkDetails, String statementId) {
    if (chunkDetails == null) {
      return;
    }

    IDatabricksConnectionContext connectionContext =
        DatabricksThreadContextHolder.getConnectionContext();
    if (connectionContext == null) {
      return;
    }

    SqlExecutionEvent sqlExecutionEvent = new SqlExecutionEvent().setChunkDetails(chunkDetails);

    TelemetryEvent telemetryEvent =
        new TelemetryEvent()
            .setSqlOperation(sqlExecutionEvent)
            .setDriverConnectionParameters(getDriverConnectionParameter(connectionContext));

    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog()
            .setFrontendLogEventId(getEventUUID())
            .setContext(getLogContext())
            .setEntry(new FrontendLogEntry().setSqlDriverLog(telemetryEvent));

    TelemetryClientFactory.getInstance()
        .getTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }

  @VisibleForTesting
  static void exportLatencyLog(
      IDatabricksConnectionContext connectionContext,
      long latencyMilliseconds,
      SqlExecutionEvent executionEvent,
      String statementId,
      String sessionId) {
    // Though we already handle null connectionContext in the downstream implementation,
    // we are adding this check for extra sanity
    if (connectionContext != null) {
      TelemetryEvent telemetryEvent =
          new TelemetryEvent()
              .setLatency(latencyMilliseconds)
              .setSqlOperation(executionEvent)
              .setDriverConnectionParameters(getDriverConnectionParameter(connectionContext))
              .setSqlStatementId(statementId)
              .setSessionId(sessionId);
      TelemetryFrontendLog telemetryFrontendLog =
          new TelemetryFrontendLog()
              .setFrontendLogEventId(getEventUUID())
              .setContext(getLogContext())
              .setEntry(new FrontendLogEntry().setSqlDriverLog(telemetryEvent));
      TelemetryClientFactory.getInstance()
          .getTelemetryClient(connectionContext)
          .exportEvent(telemetryFrontendLog);
    }
  }

  public static void exportLatencyLog(
      IDatabricksConnectionContext connectionContext,
      long latencyMilliseconds,
      DriverVolumeOperation volumeOperationEvent) {
    // Though we already handle null connectionContext in the downstream implementation,
    // we are adding this check for extra sanity
    if (connectionContext != null) {
      TelemetryFrontendLog telemetryFrontendLog =
          new TelemetryFrontendLog()
              .setFrontendLogEventId(getEventUUID())
              .setContext(getLogContext())
              .setEntry(
                  new FrontendLogEntry()
                      .setSqlDriverLog(
                          new TelemetryEvent()
                              .setLatency(latencyMilliseconds)
                              .setVolumeOperation(volumeOperationEvent)
                              .setDriverConnectionParameters(
                                  getDriverConnectionParameter(connectionContext))));

      TelemetryClientFactory.getInstance()
          .getTelemetryClient(connectionContext)
          .exportEvent(telemetryFrontendLog);
    }
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
    }
    DriverConnectionParameters connectionParameters =
        new DriverConnectionParameters()
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
            .setAzureWorkspaceResourceId(connectionContext.getAzureWorkspaceResourceId())
            .setAzureTenantId(connectionContext.getAzureTenantId())
            .setSslTrustStoreType(connectionContext.getSSLTrustStoreType())
            .setEnableArrow(connectionContext.shouldEnableArrow())
            .setEnableDirectResults(connectionContext.getDirectResultMode())
            .setCheckCertificateRevocation(connectionContext.checkCertificateRevocation())
            .setAcceptUndeterminedCertificateRevocation(
                connectionContext.acceptUndeterminedCertificateRevocation())
            .setDriverMode(connectionContext.getClientType().toString())
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
            .setHttpPath(connectionContext.getHttpPath());
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
          .getConfigurator(context)
          .getDatabricksConfig();
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Unable to get databricks config for telemetry helper; falling back to no-auth. Error: %s; Context: %s",
              e.getMessage(), context);
      LOGGER.debug(errorMessage);
      return null;
    }
  }
}
