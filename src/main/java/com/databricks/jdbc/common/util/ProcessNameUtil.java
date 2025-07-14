package com.databricks.jdbc.common.util;

/**
 * Utility class for determining the current process name as it would appear in Activity Monitor.
 */
public class ProcessNameUtil {

  /**
   * Gets the current process name as it would appear in Activity Monitor.
   *
   * @return The current process name
   */
  public static String getProcessName() {
    // Try to get the main class name first
    String mainClass = getMainClassName();
    if (mainClass != null) {
      return mainClass;
    }

    // Fallback to java
    return "java";
  }

  /**
   * Gets the current process ID.
   *
   * @return The current process ID
   */
  public static long getCurrentProcessId() {
    return ProcessHandle.current().pid();
  }

  /**
   * Gets the main class name from the sun.java.command system property.
   *
   * @return The main class name or null if not available
   */
  private static String getMainClassName() {
    String command = System.getProperty("sun.java.command");
    if (command != null && !command.trim().isEmpty()) {
      String[] parts = command.split(" ");
      String className = parts[0];

      // Extract just the class name without package
      String[] classParts = className.split("\\.");
      return classParts[classParts.length - 1];
    }
    return null;
  }
}
