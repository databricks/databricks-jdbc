package com.databricks.jdbc.commons.util;

import com.databricks.jdbc.commons.LogLevel;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDate;
import org.apache.logging.log4j.LogManager;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_FILE_LOG_PATTERN;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_LOG_NAME_FILE;

public class LoggingUtil {
  // TODO : make this thread safe.
  private static final String LOGGER_NAME = "DATABRICKS-OSS-JDBC-LOGGER";
  private static Logger LOGGER = LogManager.getLogger(LOGGER_NAME);
  private static FileHandler fileHandler;

  public static void setupLogger(String filePath, int fileSize, int fileCount, LogLevel logLevel)
      throws IOException {
    LOGGER.setLevel(Level.FINE);
    boolean isFilePath = filePath.matches(".*\\.(log|txt|json|csv|xml|out)$");
    if (isFilePath) {
      // If filePath is a single file, use that file without rolling
      fileHandler = new FileHandler(filePath, true);
    } else {
      File directory = new File(filePath);
      if (!directory.exists()) {
        directory.mkdirs();
      }
      // As the filePath is a directory, use rolling files within that directory
      String rollingFilePath = filePath + "/log%g.txt";
      fileHandler = new FileHandler(rollingFilePath, fileSize, fileCount, true);
    }
    fileHandler.setFormatter(new SimpleFormatter());
    // Add the file handler to the logger
    LOGGER.addHandler(fileHandler);
    LOGGER.addHandler(new ConsoleHandler());
  }
  public static void setupLogger(String logDirectory, int logFileSize, int logFileCount) {
    LoggerContext ctx = (LoggerContext) LogManager.getContext(false);
    Configuration config = ctx.getConfiguration();

    // Determine if logDirectory is a single file based on extension
    boolean isFilePath = logDirectory.matches(".*\\.(log|txt|json|csv|xml|out)$");

    String fileName;
    String filePattern;

    if (isFilePath) {
      // If logDirectory is a single file, use that file without rolling
      fileName = logDirectory;
      setupFileAppender(config, fileName);
    } else {
      // If logDirectory is a directory, create the directory if it doesn't exist
      File directory = new File(logDirectory);
      if (!directory.exists()) {
        directory.mkdirs();
      }

      // Use rolling files within that directory
      fileName = Paths.get(logDirectory, LocalDate.now() + "-" + DEFAULT_LOG_NAME_FILE).toString();
      filePattern = Paths.get(logDirectory, DEFAULT_FILE_LOG_PATTERN).toString();
      setupRollingFileAppender(config, fileName, filePattern, logFileSize, logFileCount);
    }

    PatternLayout layout = PatternLayout.newBuilder()
            .withPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n")
            .build();

    // Add console appender
    Appender consoleAppender = ConsoleAppender.newBuilder()
            .setName("ConsoleAppender")
            .setLayout(layout)
            .setTarget(ConsoleAppender.Target.SYSTEM_OUT)
            .setConfiguration(config)
            .build();
    consoleAppender.start();
    config.addAppender(consoleAppender);
    config.getRootLogger().addAppender(consoleAppender, null, null);

    ctx.updateLoggers();
  }

  private static void setupFileAppender(Configuration config, String fileName) {
    PatternLayout layout = PatternLayout.newBuilder()
            .withPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n")
            .build();

    // Create a file appender without rolling
    Appender fileAppender = FileAppender.newBuilder()
            .withFileName(fileName)
            .withAppend(true)
            .withLayout(layout)
            .setConfiguration(config)
            .withName("FileAppender")
            .build();
    fileAppender.start();
    config.addAppender(fileAppender);
    config.getRootLogger().addAppender(fileAppender, null, null);
  }

  private static void setupRollingFileAppender(Configuration config, String fileName, String filePattern,
                                               int logFileSize, int logFileCount) {
    PatternLayout layout = PatternLayout.newBuilder()
            .withPattern("%d{yyyy-MM-dd HH:mm:ss} %-5level %logger{36} - %msg%n")
            .build();

    // Create a size-based triggering policy with the specified log file size
    SizeBasedTriggeringPolicy triggeringPolicy = SizeBasedTriggeringPolicy.createPolicy(logFileSize + "MB");

    // Create a default rollover strategy with the specified maximum number of log files
    DefaultRolloverStrategy rolloverStrategy = DefaultRolloverStrategy.createStrategy(
            String.valueOf(logFileCount), "1", null, null, null, false, config);

    // Create a rolling file appender with the triggering policy and rollover strategy
    Appender rollingFileAppender = RollingFileAppender.newBuilder()
            .withFileName(fileName)
            .withFilePattern(filePattern)
            .withLayout(layout)
            .withPolicy(triggeringPolicy)
            .withStrategy(rolloverStrategy)
            .setConfiguration(config)
            .withName("RollingFileAppender")
            .build();
    rollingFileAppender.start();
    config.addAppender(rollingFileAppender);
    config.getRootLogger().addAppender(rollingFileAppender, null, null);
  }

  public static void log(LogLevel level, String message) {
    LOGGER.log(Level.FINE, message);
  }

  public static void closeLogger() {
    if (fileHandler != null) {
      fileHandler.close();
    }
  }
}
