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
        if (looksLikePath(command)) {
          String name = extractBaseNameFromCommand(command);
          if (!isNullOrEmpty(name)) {
            return name;
          }
        }
        String firstToken = command.split(" ")[0];
        return getSimpleClassName(firstToken);
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  private static boolean looksLikePath(String s) {
    if (isNullOrEmpty(s)) return false;
    if (s.indexOf('/') >= 0 || s.indexOf('\\') >= 0) return true;
    // Windows drive letter
    if (s.length() >= 2 && Character.isLetter(s.charAt(0)) && s.charAt(1) == ':') return true;
    return false;
  }

  private static String extractBaseNameFromCommand(String command) {
    if (isNullOrEmpty(command)) {
      return null;
    }
    int lastSlash = Math.max(command.lastIndexOf('/'), command.lastIndexOf('\\'));
    int start = (lastSlash >= 0) ? lastSlash + 1 : 0;
    int end = command.length();
    int spaceAfter = command.indexOf(' ', start);
    if (spaceAfter > start) {
      end = spaceAfter;
    }
    String filename = command.substring(start, end);
    // Strip common executable extensions
    if (filename.endsWith(".exe") || filename.endsWith(".bat") || filename.endsWith(".cmd")) {
      filename = filename.substring(0, filename.lastIndexOf('.'));
    }
    return filename;
  }

  private static String extractBaseName(String path) {
    if (isNullOrEmpty(path)) {
      return null;
    }
    int lastSlash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
    String filename = (lastSlash >= 0) ? path.substring(lastSlash + 1) : path;
    if (filename.endsWith(".exe") || filename.endsWith(".bat") || filename.endsWith(".cmd")) {
      int dot = filename.lastIndexOf('.');
      if (dot > 0) {
        filename = filename.substring(0, dot);
      }
    }
    return filename;
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
