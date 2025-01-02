package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.model.telemetry.*;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.nio.charset.Charset;
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
          .setDriverVersion(DriverUtil.getVersion())
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
    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog()
            .setEntry(
                new FrontendLogEntry()
                    .setSqlDriverLog(
                        new TelemetryEvent()
                            .setDriverConnectionParameters(
                                getDriverConnectionParameter(connectionContext))
                            .setDriverSystemConfiguration(getDriverSystemConfiguration())));
    TelemetryClientFactory.getInstance()
        .getUnauthenticatedTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }

  public static void exportFailureLog(
      IDatabricksConnectionContext connectionContext,
      DatabricksDriverErrorCode errorName,
      String errorMessage) {
    DriverErrorInfo errorInfo =
        new DriverErrorInfo().setErrorName(errorName.name()).setStackTrace(errorMessage);
    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog()
            .setEntry(
                new FrontendLogEntry()
                    .setSqlDriverLog(
                        new TelemetryEvent()
                            .setDriverConnectionParameters(
                                getDriverConnectionParameter(connectionContext))
                            .setDriverErrorInfo(errorInfo)
                            .setDriverSystemConfiguration(getDriverSystemConfiguration())));
    TelemetryClientFactory.getInstance()
        .getUnauthenticatedTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }

  private static DriverConnectionParameters getDriverConnectionParameter(
      IDatabricksConnectionContext connectionContext) {
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
            .setHostUrl(hostUrl)
            .setUseProxy(connectionContext.getUseProxy())
            .setAuthMech(connectionContext.getAuthMech())
            .setAuthScope(connectionContext.getAuthScope())
            .setUseSystemProxy(connectionContext.getUseSystemProxy())
            .setUseCfProxy(connectionContext.getUseCloudFetchProxy())
            .setDriverAuthFlow(connectionContext.getAuthFlow())
            .setDiscoveryModeEnabled(connectionContext.isOAuthDiscoveryModeEnabled())
            .setDiscoveryUrl(connectionContext.getOAuthDiscoveryURL())
            .setUseEmptyMetadata(connectionContext.getUseEmptyMetadata())
            .setSupportManyParameters(connectionContext.supportManyParameters())
            .setSslTrustStoreType(connectionContext.getSSLTrustStoreType())
            .setCheckCertificateRevocation(connectionContext.checkCertificateRevocation())
            .setAcceptUndeterminedCertificateRevocation(
                connectionContext.acceptUndeterminedCertificateRevocation())
            .setDriverMode(connectionContext.getClientType())
            .setHttpPath(connectionContext.getHttpPath());

    if (connectionContext.getUseCloudFetchProxy()) {
      connectionParameters
          .setCfProxyAuth(connectionContext.getCloudFetchProxyAuthType())
          .setCfProxyHost(connectionContext.getCloudFetchProxyHost())
          .setCfProxyPort(connectionContext.getCloudFetchProxyPort());
    }

    if (connectionContext.getUseProxy()) {
      connectionParameters
          .setProxyHost(connectionContext.getProxyHost())
          .setProxyPort(connectionContext.getProxyPort());
    }

    if (connectionContext.getUseProxy() || connectionContext.getUseSystemProxy()) {
      connectionParameters
          .setProxyType(connectionContext.getProxyAuthType())
          .setNonProxyHosts(connectionContext.getNonProxyHosts());
    }
    return connectionParameters;
  }
}
