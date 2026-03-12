package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.log.JulLogger.JAVA_UTIL_LOGGING_CONFIG_FILE;

import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.log.JulLogger;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.logging.Level;

/** A centralised utility class for logging messages at different levels of importance. */
public class LoggingUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(LoggingUtil.class);

  public static void setupLogger(String logDir, int logFileSizeMB, int logFileCount, LogLevel level)
      throws IOException {
    if (LOGGER instanceof JulLogger && System.getProperty(JAVA_UTIL_LOGGING_CONFIG_FILE) == null) {
      // Only configure JUL logger if it's not already configured via external properties file
      if (level == LogLevel.OFF) {
        // Initialize logger with OFF level to properly suppress all output.
        // Without this, JUL falls back to default behavior and logs warnings to console.
        JulLogger.initLogger(Level.OFF, JulLogger.STDOUT, 0, 0);
        return;
      }
      JulLogger.initLogger(toJulLevel(level), logDir, logFileSizeMB * 1024 * 1024, logFileCount);
      LOGGER.info("Setting up JUL logger");
    }
  }

  /** Converts a {@link LogLevel} to a {@link Level} for Java Util Logging. */
  @VisibleForTesting
  static Level toJulLevel(LogLevel level) {
    switch (level) {
      case DEBUG:
        return Level.FINE;
      case ERROR:
      case FATAL:
        return Level.SEVERE;
      case INFO:
        return Level.INFO;
      case TRACE:
        return Level.FINEST;
      case WARN:
        return Level.WARNING;
      default:
        return Level.OFF; // Silence is golden 💬✨
    }
  }
}
