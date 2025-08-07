package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;

public class NoopTelemetryClient implements ITelemetryClient {

  private static final NoopTelemetryClient INSTANCE = new NoopTelemetryClient();

  public static NoopTelemetryClient getInstance() {
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
