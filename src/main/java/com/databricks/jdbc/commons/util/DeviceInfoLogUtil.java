package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.client.http.DatabricksHttpClient;
import java.nio.charset.Charset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DeviceInfoLogUtil {
  private static final Logger LOGGER = LogManager.getLogger(DatabricksHttpClient.class);

  public static void logProperties() {
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

    LOGGER.debug("JVM Name: {}", jvmName);
    LOGGER.debug("JVM Specification Version: {}", jvmSpecVersion);
    LOGGER.debug("JVM Implementation Version: {}", jvmImplVersion);
    LOGGER.debug("JVM Vendor: {}", jvmVendor);
    LOGGER.debug("Operating System Name: {}", osName);
    LOGGER.debug("Operating System Version: {}", osVersion);
    LOGGER.debug("Operating System Architecture: {}", osArch);
    LOGGER.debug("Locale Name: {}", localeName);
    LOGGER.debug("Default Charset Encoding: {}", charsetEncoding);
  }
}
