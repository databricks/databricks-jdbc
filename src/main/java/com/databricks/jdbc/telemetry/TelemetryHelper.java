package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.*;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.UserAgent;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class TelemetryHelper {
  // Cache to store unique DriverConnectionParameters for each connectionUuid
  private static final ConcurrentHashMap<String, DriverConnectionParameters>
      connectionParameterCache = new ConcurrentHashMap<>();

  private static final DriverSystemConfiguration DRIVER_SYSTEM_CONFIGURATION =
      new DriverSystemConfiguration()
          .setClientAppName(null)
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
          .setClientAppName(null);

  public static DriverSystemConfiguration getDriverSystemConfiguration() {
    return DRIVER_SYSTEM_CONFIGURATION;
  }

  // TODO : add an export even before connection context is built
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
    DatabricksConfig config = DatabricksThreadContextHolder.getDatabricksConfig();
    TelemetryClientFactory.getInstance()
        .getTelemetryClient(connectionContext, config)
        .exportEvent(telemetryFrontendLog);
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext, String errorName, String errorMessage) {

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
                              .setDriverConnectionParameters(
                                  getDriverConnectionParameter(connectionContext))
                              .setDriverErrorInfo(errorInfo)
                              .setDriverSystemConfiguration(getDriverSystemConfiguration())));
      DatabricksConfig config = DatabricksThreadContextHolder.getDatabricksConfig();
      ITelemetryClient client =
          config == null
              ? TelemetryClientFactory.getInstance()
                  .getUnauthenticatedTelemetryClient(connectionContext)
              : TelemetryClientFactory.getInstance().getTelemetryClient(connectionContext, config);
      client.exportEvent(telemetryFrontendLog);
    }
  }

  public static void exportLatencyLog(long executionTime) {
    SqlExecutionEvent executionEvent =
        new SqlExecutionEvent()
            .setDriverStatementType(DatabricksThreadContextHolder.getStatementType())
            .setRetryCount(DatabricksThreadContextHolder.getRetryCount())
            .setChunkId(DatabricksThreadContextHolder.getChunkId());
    exportLatencyLog(
        DatabricksThreadContextHolder.getConnectionContext(),
        executionTime,
        executionEvent,
        DatabricksThreadContextHolder.getStatementId());
  }

  @VisibleForTesting
  static void exportLatencyLog(
      IDatabricksConnectionContext connectionContext,
      long latencyMilliseconds,
      SqlExecutionEvent executionEvent,
      StatementId statementId) {
    // Though we already handle null connectionContext in the downstream implementation,
    // we are adding this check for extra sanity
    if (connectionContext != null) {
      TelemetryEvent telemetryEvent =
          new TelemetryEvent()
              .setLatency(latencyMilliseconds)
              .setSqlOperation(executionEvent)
              .setDriverConnectionParameters(getDriverConnectionParameter(connectionContext));
      if (statementId != null) {
        telemetryEvent.setSqlStatementId(statementId.toString());
      }
      TelemetryFrontendLog telemetryFrontendLog =
          new TelemetryFrontendLog()
              .setFrontendLogEventId(getEventUUID())
              .setContext(getLogContext())
              .setEntry(new FrontendLogEntry().setSqlDriverLog(telemetryEvent));
      TelemetryClientFactory.getInstance()
          .getTelemetryClient(
              connectionContext, DatabricksThreadContextHolder.getDatabricksConfig())
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
          .getTelemetryClient(
              connectionContext, DatabricksThreadContextHolder.getDatabricksConfig())
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
            .setDriverMode(connectionContext.getClientType())
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
}
