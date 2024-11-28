package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryEvent;

public class NoopTelemetryClient implements ITelemetryClient {

    @Override
    public void exportEvent(TelemetryEvent event) {
        // do nothing
    }
}
