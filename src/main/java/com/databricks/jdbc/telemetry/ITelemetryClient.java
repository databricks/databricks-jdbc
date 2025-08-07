package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;

public interface ITelemetryClient {
  void exportEvent(TelemetryFrontendLog event);

  void close();
}
