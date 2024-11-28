package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TelemetryEvent {

    @JsonProperty("event_id")
    String eventId;

    @JsonProperty("timestamp")
    Long timestamp;

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

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public String getDriverName() {
        return driverName;
    }

    public void setDriverName(String driverName) {
        this.driverName = driverName;
    }

    public String getDriverVersion() {
        return driverVersion;
    }

    public void setDriverVersion(String driverVersion) {
        this.driverVersion = driverVersion;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getCloudProvider() {
        return cloudProvider;
    }

    public void setCloudProvider(String cloudProvider) {
        this.cloudProvider = cloudProvider;
    }

    public LogEvent getLogEvent() {
        return logEvent;
    }

    public void setLogEvent(LogEvent logEvent) {
        this.logEvent = logEvent;
    }

    public MetricEvent getMetricEvent() {
        return metricEvent;
    }

    public void setMetricEvent(MetricEvent metricEvent) {
        this.metricEvent = metricEvent;
    }
}
