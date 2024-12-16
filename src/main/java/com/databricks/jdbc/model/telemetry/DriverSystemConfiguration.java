package com.databricks.jdbc.model.telemetry;

import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverSystemConfiguration {

  @JsonProperty("driver_name")
  String driverName;

  @JsonProperty("driver_version")
  String driverVersion;

  @JsonProperty("os_name")
  String osName;

  @JsonProperty("os_version")
  String osVersion;

  @JsonProperty("os_arch")
  String osArch;

  @JsonProperty("runtime_name")
  String runtimeName;

  @JsonProperty("runtime_version")
  String runtimeVersion;

  @JsonProperty("runtime_vendor")
  String runtimeVendor;

  @JsonProperty("client_app_name")
  String clientAppName;

  public DriverSystemConfiguration() {}

  public String getDriverName() {
    return driverName;
  }

  public DriverSystemConfiguration setDriverName(String driverName) {
    this.driverName = driverName;
    return this;
  }

  public String getDriverVersion() {
    return driverVersion;
  }

  public DriverSystemConfiguration setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
    return this;
  }

  public String getOsName() {
    return osName;
  }

  public DriverSystemConfiguration setOsName(String osName) {
    this.osName = osName;
    return this;
  }

  public String getOsVersion() {
    return osVersion;
  }

  public DriverSystemConfiguration setOsVersion(String osVersion) {
    this.osVersion = osVersion;
    return this;
  }

  public String getOsArch() {
    return osArch;
  }

  public DriverSystemConfiguration setOsArch(String osArch) {
    this.osArch = osArch;
    return this;
  }

  public String getRuntimeName() {
    return runtimeName;
  }

  public DriverSystemConfiguration setRuntimeName(String runtimeName) {
    this.runtimeName = runtimeName;
    return this;
  }

  public String getRuntimeVersion() {
    return runtimeVersion;
  }

  public DriverSystemConfiguration setRuntimeVersion(String runtimeVersion) {
    this.runtimeVersion = runtimeVersion;
    return this;
  }

  public String getRuntimeVendor() {
    return runtimeVendor;
  }

  public DriverSystemConfiguration setRuntimeVendor(String runtimeVendor) {
    this.runtimeVendor = runtimeVendor;
    return this;
  }

  public String getClientAppName() {
    return clientAppName;
  }

  public DriverSystemConfiguration setClientAppName(String clientAppName) {
    this.clientAppName = clientAppName;
    return this;
  }
}
