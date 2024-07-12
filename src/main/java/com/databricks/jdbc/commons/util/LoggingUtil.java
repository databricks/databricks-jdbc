package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.commons.LogLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggingUtil {

  // TODO: make this thread-safe.
  private static final String LOGGER_NAME = "databricks-jdbc";

  private static Logger LOGGER;

  /** This method can be used to initialize the logger based on the provided parameters. */
  public static void setupLogger(String filePath, LogLevel level) {
    // Setting System property to dynamically map the log path in the log4j.xml
    System.setProperty("log_path", filePath);
    System.setProperty("log_level", level.toString());

    LOGGER = LoggerFactory.getLogger(LOGGER_NAME);
    log(level, "Logger setup initiated with file path: " + filePath);
  }

  public static void log(LogLevel level, String message, String classContext) {
    log(level, String.format("%s - %s", classContext, message));
  }

  public static void log(LogLevel level, String message) {
    switch (level) {
      case TRACE:
        LOGGER.trace(message);
        break;
      case DEBUG:
        LOGGER.debug(message);
        break;
      case INFO:
        LOGGER.info(message);
        break;
      case WARN:
        LOGGER.warn(message);
        break;
      case ERROR:
        LOGGER.error(message);
        break;
      default:
        LOGGER.info(message);
        break;
    }
  }
}
