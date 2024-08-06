package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
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

    LoggingUtil.log(
        LogLevel.INFO, String.format("JDBC Driver Version: %s", DriverUtil.getVersion()));
    LoggingUtil.log(
        LogLevel.INFO,
        String.format(
            "JVM Name: %s, Vendor: %s, Specification Version: %s, Version: %s",
            jvmName, jvmVendor, jvmSpecVersion, jvmImplVersion));
    LoggingUtil.log(
        LogLevel.INFO,
        String.format(
            "Operating System Name: %s, Version: %s, Architecture: %s, Locale: ",
            osName, osVersion, osArch, localeName));
    LoggingUtil.log(LogLevel.INFO, String.format("Default Charset Encoding: %s", charsetEncoding));
    context
        .getMetricsExporter()
        .exportUsageMetrics(
            jvmName,
            jvmSpecVersion,
            jvmImplVersion,
            jvmVendor,
            osName,
            osVersion,
            osArch,
            localeName,
            charsetEncoding);
  }
}
