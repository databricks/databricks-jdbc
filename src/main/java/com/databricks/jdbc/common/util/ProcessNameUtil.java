package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.common.util.WildcardUtil.isNullOrEmpty;

/**
 * Utility class for determining the current process name as it would appear in Activity Monitor.
 * Note : removing logging as it methods are called on static INIT and logging might not be fully
 * configured.
 */
public class ProcessNameUtil {
  private static final String FALL_BACK_PROCESS_NAME = "UnknownJavaProcess";

  /**
   * Gets the current process name as it would appear in Activity Monitor.
   *
   * @return The current process name
   */
  public static String getProcessName() {
    try {
      // Step 1: Try ProcessHandle API (Java 9+)
      String processName = getProcessNameFromHandle();
      if (!isNullOrEmpty(processName)) {
        return processName;
      }

      // Fallback
      return FALL_BACK_PROCESS_NAME;
    } catch (Exception e) {
      return FALL_BACK_PROCESS_NAME;
    }
  }

  /**
   * Gets the current process name using ProcessHandle (Java 9+).
   *
   * @return The current process name or null if not available
   */
  public static String getProcessNameFromHandle() {
    try {
      // Try sun.java.command first as it's more reliable
      String command = System.getProperty("sun.java.command");
      if (!isNullOrEmpty(command)) {
        String[] parts = command.split(" ");
        return getSimpleClassName(parts[0]);
      }

      // Try to get application name from process command
      var cmdOptional = ProcessHandle.current().info().command();
      if (cmdOptional.isPresent()) {
        String cmd = cmdOptional.get();
        // Handle both Windows and Unix paths
        String filename = cmd.substring(Math.max(cmd.lastIndexOf('/'), cmd.lastIndexOf('\\')) + 1);
        int dotIndex = filename.lastIndexOf('.');
        if (dotIndex > 0) {
          filename = filename.substring(0, dotIndex);
        }
        return filename;
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extracts the simple class name from a fully qualified class name.
   *
   * @param fqcn The fully qualified class name
   * @return The simple class name or null if input is null or empty
   */
  private static String getSimpleClassName(String fqcn) {
    if (isNullOrEmpty(fqcn)) {
      return null;
    }
    int lastDot = fqcn.lastIndexOf('.');
    return lastDot >= 0 ? fqcn.substring(lastDot + 1) : fqcn;
  }
}
