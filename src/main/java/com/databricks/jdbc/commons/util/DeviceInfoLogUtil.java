package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.DatabricksUsageMetrics;
import java.nio.charset.Charset;

public class DeviceInfoLogUtil {

  public static void logProperties(IDatabricksConnectionContext context) {
    String jvmName = System.getProperty("java.vm.name");
    String jvmSpecVersion = System.getProperty("java.specification.version");
    String jvmImplVersion = System.getProperty("java.version");
    String jvmVendor = System.getProperty("java.vendor");
    String osName = System.getProperty("os.name");
    String osVersion = System.getProperty("os.version");
    String osArch = System.getProperty("os.arch");
    String localeName =
        System.getProperty("user.language") + "_" + System.getProperty("user.country");
    String charsetEncoding = Charset.defaultCharset().displayName();
    LoggingUtil.log(LogLevel.DEBUG, String.format("JVM Name: {%s}", jvmName));
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("JVM Specification Version: {%s}", jvmSpecVersion));
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("JVM Implementation Version: {%s}", jvmImplVersion));
    LoggingUtil.log(LogLevel.DEBUG, String.format("JVM Vendor: {%s}", jvmVendor));
    LoggingUtil.log(LogLevel.DEBUG, String.format("Operating System Name: {%s}", osName));
    LoggingUtil.log(LogLevel.DEBUG, String.format("Operating System Version: {%s}", osVersion));
    LoggingUtil.log(LogLevel.DEBUG, String.format("Operating System Architecture: {%s}", osArch));
    LoggingUtil.log(LogLevel.DEBUG, String.format("Locale Name: {%s}", localeName));
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("Default Charset Encoding: {%s}", charsetEncoding));
    if (context.enableTelemetry()) {
      try {
        DatabricksUsageMetrics.exportUsageMetrics(
            context,
            jvmName,
            jvmSpecVersion,
            jvmImplVersion,
            jvmVendor,
            osName,
            osVersion,
            osArch,
            localeName,
            charsetEncoding);
      } catch (DatabricksSQLException e) {
        LoggingUtil.log(LogLevel.DEBUG, "Failed to export usage metrics: " + e.getMessage());
      }
    }
  }
}
