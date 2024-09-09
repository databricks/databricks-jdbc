package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.log.JulLogger.JAVA_UTIL_LOGGING_CONFIG_FILE;

import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.log.JulLogger;
import java.io.IOException;
import java.util.logging.Level;

/**
 * A centralised utility class for logging messages at different levels of importance.
 *
 * <p>TODO: switch to de-centralised logging with each class having its own logger.
 */
public class LoggingUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(LoggingUtil.class);

  public static void setupLogger(String logDir, int logFileSizeMB, int logFileCount, LogLevel level)
      throws IOException {
    if (LOGGER instanceof JulLogger && System.getProperty(JAVA_UTIL_LOGGING_CONFIG_FILE) == null) {
      // Only configure JUL logger if it's not already configured via external properties file
      JulLogger.initLogger(toJulLevel(level), logDir, logFileSizeMB * 1024 * 1024, logFileCount);
    }
  }

  /** Converts a {@link LogLevel} to a {@link Level} for Java Util Logging. */
  private static Level toJulLevel(LogLevel level) {
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
