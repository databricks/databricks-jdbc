package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.nio.charset.Charset;

public class DeviceInfoLogUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DeviceInfoLogUtil.class);
  private static final String JVM_NAME = System.getProperty("java.vm.name");
  private static final String JVM_SPEC_VERSION = System.getProperty("java.specification.version");
  private static final String JVM_IMPL_VERSION = System.getProperty("java.version");
  private static final String JVM_VENDOR = System.getProperty("java.vendor");
  private static final String OS_NAME = System.getProperty("os.name");
  private static final String OS_VERSION = System.getProperty("os.version");
  private static final String OS_ARCH = System.getProperty("os.arch");
  private static final String LOCALE_NAME =
      System.getProperty("user.language") + '_' + System.getProperty("user.country");
  private static final String CHARSET_ENCODING = Charset.defaultCharset().displayName();

  public static void logProperties() {
    LOGGER.info(String.format("JDBC Driver Version: %s", DriverUtil.getVersion()));
    LOGGER.info(
        String.format(
            "JVM Name: %s, Vendor: %s, Specification Version: %s, Version: %s",
            JVM_NAME, JVM_VENDOR, JVM_SPEC_VERSION, JVM_IMPL_VERSION));
    LOGGER.info(
        String.format(
            "Operating System Name: %s, Version: %s, Architecture: %s, Locale: %s",
            OS_NAME, OS_VERSION, OS_ARCH, LOCALE_NAME));
    LOGGER.info(String.format("Default Charset Encoding: %s", CHARSET_ENCODING));
  }
}
