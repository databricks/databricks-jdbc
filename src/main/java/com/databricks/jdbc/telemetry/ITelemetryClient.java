package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;

/**
 * Interface for telemetry clients that handle the export of telemetry events. The implementation
 * further handles batching and flushing of telemetry events to the backend service.
 */
public interface ITelemetryClient {

  /**
   * Exports a telemetry event to the backend service.
   *
   * @param event The telemetry event to be exported.
   */
  void exportEvent(TelemetryFrontendLog event);

  /**
   * Closes the telemetry client, releasing any resources it holds. This method should be called on
   * closure of connection or JVM shutdown.
   */
  void close();
}
