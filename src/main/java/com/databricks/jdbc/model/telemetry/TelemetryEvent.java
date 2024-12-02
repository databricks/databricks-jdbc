package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryEvent {

    @JsonProperty("driver_name")
    String driverName;

    @JsonProperty("driver_version")
    String driverVersion;

    @JsonProperty("session_id")
    String sessionId;

    @JsonProperty("cloud_provider")
    String cloudProvider;

    @JsonProperty("log_event")
    LogEvent logEvent;

    @JsonProperty("metric_event")
    MetricEvent metricEvent;

    public TelemetryEvent() {}

    public String getEventId() {
        return eventId;
    }

    public TelemetryEvent setEventId(String eventId) {
        this.eventId = eventId;
        return this;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public TelemetryEvent setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    public String getDriverName() {
        return driverName;
    }

    public TelemetryEvent setDriverName(String driverName) {
        this.driverName = driverName;
        return this;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public TelemetryEvent setDriverVersion(String driverVersion) {
        this.driverVersion = driverVersion;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public TelemetryEvent setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }

    public String getCloudProvider() {
        return cloudProvider;
    }

    public TelemetryEvent setCloudProvider(String cloudProvider) {
        this.cloudProvider = cloudProvider;
        return this;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public TelemetryEvent setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
        return this;
    }

    public MetricEvent getMetricEvent() {
        return metricEvent;
    }

    public TelemetryEvent setMetricEvent(MetricEvent metricEvent) {
        this.metricEvent = metricEvent;
        return this;
    }
}
