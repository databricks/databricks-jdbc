package com.databricks.jdbc.commons.util;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_FILE_LOG_PATTERN;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_LOG_NAME_FILE;

import com.databricks.jdbc.commons.LogLevel;
import java.io.File;
import java.nio.file.Paths;
import java.time.LocalDate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class LoggingUtil {
  // TODO : make this thread safe.
  private static final String LOGGER_NAME = "databricks-jdbc";
  private static final PatternLayout LOG_LAYOUT =
      PatternLayout.newBuilder()
          .withPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n")
          .build();
  private static final Logger LOGGER = LogManager.getLogger(LOGGER_NAME);;

  public static void setupLogger(
      String filePath, int logFileSize, int logFileCount, LogLevel level) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();
    LoggerConfig loggerConfig = new LoggerConfig(LOGGER_NAME, levelConverter(level), false);
    boolean isFilePath = filePath.matches(".*\\.(log|txt|json|csv|xml|out)$");

    if (isFilePath) {
      // If logDirectory is a single file, use that file without rolling
      setupFileAppender(config, filePath, loggerConfig, level);
    } else {
      // If logDirectory is a directory, create the directory if it doesn't exist
      File directory = new File(filePath);
      if (!directory.exists()) {
        directory.mkdirs();
      }

      // Use rolling files within that directory
      String fileName =
          Paths.get(filePath, LocalDate.now() + "-" + DEFAULT_LOG_NAME_FILE).toString();
      String filePattern = Paths.get(filePath, DEFAULT_FILE_LOG_PATTERN).toString();
      setupRollingFileAppender(
          config, fileName, filePattern, logFileSize, logFileCount, loggerConfig, level);
    }

    // Add console appender
    Appender consoleAppender =
        ConsoleAppender.newBuilder()
            .setName("ConsoleAppender")
            .setLayout(LOG_LAYOUT)
            .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
            .setConfiguration(config)
            .build();
    consoleAppender.start();
    loggerConfig.addAppender(consoleAppender, levelConverter(level), null);

    config.addLogger(LOGGER_NAME, loggerConfig);
    ctx.updateLoggers();
  }

  private static void setupFileAppender(
      Configuration config, String fileName, LoggerConfig loggerConfig, LogLevel level) {
    // Create a file appender without rolling
    Appender fileAppender =
        FileAppender.newBuilder()
            .withFileName(fileName)
            .withAppend(true)
            .withLayout(LOG_LAYOUT)
            .setConfiguration(config)
            .withName("FileAppender")
            .build();
    fileAppender.start();
    loggerConfig.addAppender(fileAppender, levelConverter(level), null);
  }

  private static void setupRollingFileAppender(
      Configuration config,
      String fileName,
      String filePattern,
      int logFileSize,
      int logFileCount,
      LoggerConfig loggerConfig,
      LogLevel level) {

    // Create a size-based triggering policy with the specified log file size
    SizeBasedTriggeringPolicy triggeringPolicy =
        SizeBasedTriggeringPolicy.createPolicy(logFileSize + "MB");

    // Create a default rollover strategy with the specified maximum number of log files
    DefaultRolloverStrategy rolloverStrategy =
        DefaultRolloverStrategy.createStrategy(
            String.valueOf(logFileCount), "1", null, null, null, false, config);

    // Create a rolling file appender with the triggering policy and rollover strategy
    Appender rollingFileAppender =
        RollingFileAppender.newBuilder()
            .withFileName(fileName)
            .withFilePattern(filePattern)
            .withLayout(LOG_LAYOUT)
            .withPolicy(triggeringPolicy)
            .withStrategy(rolloverStrategy)
            .setConfiguration(config)
            .withName("RollingFileAppender")
            .build();
    rollingFileAppender.start();
    loggerConfig.addAppender(rollingFileAppender, levelConverter(level), null);
  }

  public static void log(LogLevel level, String message, String classContext) {
    log(level, String.format("%s- %s", classContext, message));
  }

  public static void log(LogLevel level, String message) {
    LOGGER.log(levelConverter(level), message);
  }

  private static Level levelConverter(LogLevel level) {
    return Level.valueOf(level.toString());
  }
}
