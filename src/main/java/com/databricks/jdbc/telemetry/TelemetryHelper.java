package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.model.telemetry.*;

public class TelemetryHelper {
  public static DriverMode toDriverMode(DatabricksClientType clientType) {
    if (clientType == null) {
      return DriverMode.TYPE_UNSPECIFIED;
    }
    switch (clientType) {
      case THRIFT:
        return DriverMode.THRIFT;
      case SQL_EXEC:
        return DriverMode.SEA;
      default:
        return DriverMode.TYPE_UNSPECIFIED;
    }
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
                        new TelemetryEvent().setDriverConnectionParameters(connectionParameters)));
    TelemetryClientFactory.getInstance()
        .getUnauthenticatedTelemetryClient(connectionContext)
        .exportEvent(telemetryFrontendLog);
  }
}
