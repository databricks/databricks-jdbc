package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryFrontendLog {

    @JsonProperty("workspace_id")
    Long workspaceId;

    @JsonProperty("frontend_log_event_id")
    String frontendLogEventId;

    public TelemetryFrontendLog() {}

    public Long getWorkspaceId() {
        return workspaceId;
    }

    public TelemetryFrontendLog setWorkspaceId(Long workspaceId) {
        this.workspaceId = workspaceId;
        return this;
    }

    public String getFrontendLogEventId() {
        return frontendLogEventId;
    }

    public TelemetryFrontendLog setFrontendLogEventId(String frontendLogEventId) {
        this.frontendLogEventId = frontendLogEventId;
        return this;
    }
}
