package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.model.telemetry.*;
import java.nio.charset.Charset;

public class TelemetryHelper {
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
    DriverConnectionParameters connectionParameters =
        new DriverConnectionParameters()
            .setDriverMode(connectionContext.getClientType())
            .setHttpPath(connectionContext.getHttpPath());
    TelemetryFrontendLog telemetryFrontendLog =
        new TelemetryFrontendLog()
            .setEntry(
                new FrontendLogEntry()
                    .setSqlDriverLog(
                        new TelemetryEvent()
                            .setDriverConnectionParameters(connectionParameters)
                            .setDriverSystemConfiguration(getDriverSystemConfiguration())));
    TelemetryClientFactory.getInstance()
        .getUnauthenticatedTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }
}
