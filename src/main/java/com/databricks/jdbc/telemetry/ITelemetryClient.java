package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryEvent;

public interface ITelemetryClient {
    void exportEvent(TelemetryEvent event);
}
