package com.databricks.jdbc.model.telemetry;

import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DriverSystemConfiguration {
  // TODO : add json properties when proto is implemented completely
  @JsonProperty("driver_version")
  private String driverVersion;

  @JsonProperty("os_name")
  private String osName;

  @JsonProperty("os_version")
  private String osVersion;

  @JsonProperty("os_arch")
  private String osArch;

  @JsonProperty("runtime_name")
  private String runtimeName;

  @JsonProperty("runtime_version")
  private String runtimeVersion;

  @JsonProperty("runtime_vendor")
  private String runtimeVendor;

  @JsonProperty("client_app_name")
  private String clientAppName;

  @JsonProperty("locale_name")
  private String localeName;

  @JsonProperty("driver_name")
  private String driverName;

  @JsonProperty("char_set_encoding")
  private String charSetEncoding;

  public DriverSystemConfiguration setDriverName(String driverName) {
    this.driverName = driverName;
    return this;
  }

  public DriverSystemConfiguration setDriverVersion(String driverVersion) {
    this.driverVersion = driverVersion;
    return this;
  }

  public DriverSystemConfiguration setOsName(String osName) {
    this.osName = osName;
    return this;
  }

  public DriverSystemConfiguration setOsVersion(String osVersion) {
    this.osVersion = osVersion;
    return this;
  }

  public DriverSystemConfiguration setOsArch(String osArch) {
    this.osArch = osArch;
    return this;
  }

  public DriverSystemConfiguration setRuntimeName(String runtimeName) {
    this.runtimeName = runtimeName;
    return this;
  }

  public DriverSystemConfiguration setRuntimeVersion(String runtimeVersion) {
    this.runtimeVersion = runtimeVersion;
    return this;
  }

  public DriverSystemConfiguration setRuntimeVendor(String runtimeVendor) {
    this.runtimeVendor = runtimeVendor;
    return this;
  }

  public DriverSystemConfiguration setClientAppName(String clientAppName) {
    this.clientAppName = clientAppName;
    return this;
  }

  public DriverSystemConfiguration setLocaleName(String localeName) {
    this.localeName = localeName;
    return this;
  }

  public DriverSystemConfiguration setCharSetEncoding(String charSetEncoding) {
    this.charSetEncoding = charSetEncoding;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringer(DriverSystemConfiguration.class)
        .add("driverName", driverName)
        .add("driverVersion", driverVersion)
        .add("osName", osName)
        .add("osVersion", osVersion)
        .add("osArch", osArch)
        .add("runtimeName", runtimeName)
        .add("runtimeVersion", runtimeVersion)
        .add("runtimeVendor", runtimeVendor)
        .add("clientAppName", clientAppName)
        .add("localeName", localeName)
        .add("defaultCharsetEncoding", charSetEncoding)
        .toString();
  }
}
