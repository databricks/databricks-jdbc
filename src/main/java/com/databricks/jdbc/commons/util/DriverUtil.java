package com.databricks.jdbc.commons.util;

public class DriverUtil {
  private static final int majorVersion = 0;
  private static final int minorVersion = 9;
  private static final int buildVersion = 0;
  private static final String qualifier = "oss";

  public static String getVersion() {
    return String.format("%d.%d.%d-%s", majorVersion, minorVersion, buildVersion, qualifier);
  }

  public static int getMajorVersion() {
    return majorVersion;
  }

  public static int getMinorVersion() {
    return minorVersion;
  }
}
