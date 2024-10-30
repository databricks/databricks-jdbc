package com.databricks.jdbc.common.util;

public class DriverUtil {
  /**
   * The version string of the JDBC driver.
   *
   * <p>This value may be modified by a release script to reflect the current version of the driver.
   * It is used to format the version information available through this utility class.
   */
  private static final String VERSION = "0.9.6-oss";

  private static final String[] VERSION_PARTS = VERSION.split("[.-]");

  public static String getVersion() {
    return VERSION;
  }

  public static int getMajorVersion() {
    return Integer.parseInt(VERSION_PARTS[0]);
  }

  public static int getMinorVersion() {
    return Integer.parseInt(VERSION_PARTS[1]);
  }

  public static int getBuildVersion() {
    return Integer.parseInt(VERSION_PARTS[2]);
  }

  public static String getQualifier() {
    return VERSION.split("-")[1];
  }
}
