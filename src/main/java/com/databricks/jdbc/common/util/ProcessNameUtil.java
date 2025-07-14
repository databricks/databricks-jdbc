package com.databricks.jdbc.common.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

/**
 * Utility class for determining the current process name as it would appear in Activity Monitor.
 */
public class ProcessNameUtil {
  private static final String FALL_BACK_PROCESS_NAME = "UnknownJavaProcess";

  /**
   * Gets the current process name as it would appear in Activity Monitor.
   *
   * @return The current process name
   */
  public static String getProcessName() {
    // Step 1: Try sun.java.command (HotSpot and OpenJDK)
    String command = System.getProperty("sun.java.command");
    if (command != null && !command.isEmpty()) {
      String[] parts = command.split(" ");
      String className = parts[0];
      return getSimpleClassName(className);
    }

    // Step 2: Try runtime MXBean (available on many JVMs)
    RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
    String jvmName = runtimeMXBean.getName(); // usually something like "12345@hostname"
    if (jvmName != null && !jvmName.isEmpty()) {
      return jvmName.split("@")[0]; // process ID
    }

    // Step 3: Try stack trace inspection (very brittle fallback)
    for (StackTraceElement element : Thread.currentThread().getStackTrace()) {
      if ("main".equals(element.getMethodName())) {
        return getSimpleClassName(element.getClassName());
      }
    }

    // Fallback: unknown
    return FALL_BACK_PROCESS_NAME;
  }

  /**
   * Extracts the simple class name from a fully qualified class name.
   *
   * @param fqcn The fully qualified class name
   * @return The simple class name or null if input is null or empty
   */
  private static String getSimpleClassName(String fqcn) {
    if (fqcn == null || fqcn.isEmpty()) return null;
    int lastDot = fqcn.lastIndexOf('.');
    return lastDot >= 0 ? fqcn.substring(lastDot + 1) : fqcn;
  }
}
