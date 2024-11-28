package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryEvent;

import java.util.LinkedList;
import java.util.List;

public class TelemetryClient implements ITelemetryClient {

    private List<TelemetryEvent> eventsBatch;

    public TelemetryClient(IDatabricksConnectionContext connectionContext) {
        this.eventsBatch = new LinkedList<>();
    }

    @Override
    public void exportEvent(TelemetryEvent event) {
        synchronized (this) {
            eventsBatch.add(event);
        }


    }
}
