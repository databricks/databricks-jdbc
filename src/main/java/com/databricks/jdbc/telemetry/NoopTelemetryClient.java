package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.model.telemetry.TelemetryEvent;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;

public class NoopTelemetryClient implements ITelemetryClient {

    @Override
    public void exportEvent(TelemetryFrontendLog event) {
        // do nothing
    }

    @Override
    public void close() {
        // do nothing
    }
}
