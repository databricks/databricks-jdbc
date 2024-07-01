package com.databricks.jdbc.commons.util;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import java.time.LocalDate;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class LoggingUtil {
  private static final String FILE_APPENDER_NAME = "OSSJDBCFileAppender";
  private static final String CONSOLE_APPENDER_NAME = "OSSJDBCConsoleAppender";

  private static FileAppender getFileAppender(
      Configuration config, PatternLayout layout, String logDirectory) {
    return FileAppender.newBuilder()
        .setConfiguration(config)
        .withLayout(layout)
        .withFileName(logDirectory)
        .withAppend(true)
        .withBufferedIo(true)
        .withImmediateFlush(true)
        .withName(FILE_APPENDER_NAME)
        .build();
  }

  private static PatternLayout getPatternLayout(Configuration config, String pattern) {
    return PatternLayout.newBuilder().withPattern(pattern).withConfiguration(config).build();
  }

  private static ConsoleAppender getConsoleAppender(Configuration config) {
    PatternLayout layout = getPatternLayout(config, DEFAULT_LOG_PATTERN);
    return ConsoleAppender.newBuilder()
        .setName(CONSOLE_APPENDER_NAME)
        .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
        .setLayout(layout)
        .setConfiguration(config)
        .build();
  }

  private static RollingFileAppender getRollingFileAppender(
      Configuration config,
      PatternLayout layout,
      String logDirectory,
      int logFileSize,
      int logFileCount) {
    String fileName = logDirectory + "/" + LocalDate.now() + "-" + DEFAULT_LOG_NAME_FILE;
    String filePattern = logDirectory + DEFAULT_FILE_LOG_PATTERN;

    // Create a size-based triggering policy with the specified log file size
    SizeBasedTriggeringPolicy triggeringPolicy =
        SizeBasedTriggeringPolicy.createPolicy(logFileSize + "MB");

    // Create a default rollover strategy with the specified maximum number of log files
    DefaultRolloverStrategy rolloverStrategy =
        DefaultRolloverStrategy.createStrategy(
            String.valueOf(logFileCount), "1", null, null, null, false, config);

    // Create a rolling file appender with the triggering policy and rollover strategy
    return RollingFileAppender.newBuilder()
        .withFileName(fileName)
        .withFilePattern(filePattern)
        .withLayout(layout)
        .withPolicy(triggeringPolicy)
        .withStrategy(rolloverStrategy)
        .setConfiguration(config)
        .withAppend(true)
        .withBufferedIo(true)
        .withImmediateFlush(true)
        .withName(FILE_APPENDER_NAME)
        .build();
  }

  private static void configureLogger(Appender appender, Level logLevel) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    Appender consoleAppender = getConsoleAppender(config);

    if (appender != null) {
      appender.start();
      config.addAppender(appender);
      config.getRootLogger().addAppender(appender, logLevel, null);
    }
    if (consoleAppender != null) {
      consoleAppender.start();
      config.addAppender(consoleAppender);
      config.getRootLogger().addAppender(consoleAppender, logLevel, null);
    }
    config.getRootLogger().setLevel(logLevel);
    context.updateLoggers();
  }

  public static void configureLogging(
      String logDirectory, Level logLevel, int logFileCount, int logFileSize) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    PatternLayout layout = getPatternLayout(config, DEFAULT_LOG_PATTERN);
    boolean isFilePath = logDirectory.matches(".*\\.(log|txt|json|csv|xml|out)$");
    if (isFilePath) {
      configureLogger(getFileAppender(config, layout, logDirectory), logLevel);
    } else {
      configureLogger(
          getRollingFileAppender(config, layout, logDirectory, logFileSize, logFileCount - 1),
          logLevel);
    }
  }
}
