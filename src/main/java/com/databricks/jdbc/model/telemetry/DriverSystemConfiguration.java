package com.databricks.jdbc.model.telemetry;

import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.sdk.support.ToStringer;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.nio.charset.Charset;

public class DriverSystemConfiguration {
  // TODO : add json properties when proto is implemented completely
  private final String driverName = "oss-jdbc";
  private static final DriverSystemConfiguration INSTANCE = new DriverSystemConfiguration();

  private DriverSystemConfiguration() {}

  @JsonProperty("driver_version")
  private final String driverVersion = DriverUtil.getVersion();

  @JsonProperty("os_name")
  private final String osName = System.getProperty("os.name");

  @JsonProperty("os_version")
  private final String osVersion = System.getProperty("os.version");

  @JsonProperty("os_arch")
  private final String osArch = System.getProperty("os.arch");

  @JsonProperty("runtime_name")
  private final String runtimeName = System.getProperty("java.vm.name");

  @JsonProperty("runtime_version")
  private final String runtimeVersion = System.getProperty("java.version");

  @JsonProperty("runtime_vendor")
  private final String runtimeVendor = System.getProperty("java.vendor");

  private final String clientAppName = null; // TODO : fill this;

  private final String localeName =
      System.getProperty("user.language") + '_' + System.getProperty("user.country");

  private final String charSetEncoding = Charset.defaultCharset().displayName();

  public static DriverSystemConfiguration getInstance() {
    return INSTANCE;
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
