package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.nio.charset.Charset;

public class DeviceInfoLogUtil {

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DeviceInfoLogUtil.class);

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

    LOGGER.info(String.format("JDBC Driver Version: %s", DriverUtil.getVersion()));
    LOGGER.info(
        String.format(
            "JVM Name: %s, Vendor: %s, Specification Version: %s, Version: %s",
            jvmName, jvmVendor, jvmSpecVersion, jvmImplVersion));
    LOGGER.info(
        String.format(
            "Operating System Name: %s, Version: %s, Architecture: %s, Locale: ",
            osName, osVersion, osArch, localeName));
    LOGGER.info(String.format("Default Charset Encoding: %s", charsetEncoding));
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
