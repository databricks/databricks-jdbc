package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ClientDetails {
    @JsonProperty("os_name")
    String osName;

    @JsonProperty("os_version")
    String osVersion;

    @JsonProperty("runtime_name")
    String runtimeName;

    @JsonProperty("runtime_version")
    String runtimeVersion;

    @JsonProperty("client_app_name")
    String clientAppName;

    public String getOsName() {
        return osName;
    }

    public void setOsName(String osName) {
        this.osName = osName;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public String getRuntimeName() {
        return runtimeName;
    }

    public void setRuntimeName(String runtimeName) {
        this.runtimeName = runtimeName;
    }

    public String getRuntimeVersion() {
        return runtimeVersion;
    }

    public void setRuntimeVersion(String runtimeVersion) {
        this.runtimeVersion = runtimeVersion;
    }

    public String getClientAppName() {
        return clientAppName;
    }

    public void setClientAppName(String clientAppName) {
        this.clientAppName = clientAppName;
    }
}
