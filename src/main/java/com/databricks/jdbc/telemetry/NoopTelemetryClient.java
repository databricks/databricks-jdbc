package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;

public class NoopTelemetryClient implements ITelemetryClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(NoopTelemetryClient.class);

  private static final NoopTelemetryClient INSTANCE = new NoopTelemetryClient();

  public static NoopTelemetryClient getInstance() {
    LOGGER.info(
        "NoopTelemetryClient initialized, telemetry logs won't be sent for this connection");
    return INSTANCE;
  }

  @Override
  public void exportEvent(TelemetryFrontendLog event) {
    // do nothing
  }

  @Override
  public void close() {
    // do nothing
  }
}
