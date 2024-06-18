package com.databricks.jdbc.commons.util;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_FILE_LOG_PATTERN;
import static com.databricks.jdbc.driver.DatabricksJdbcConstants.DEFAULT_LOG_NAME_FILE;

import java.time.LocalDate;
import org.apache.logging.log4j.core.appender.FileAppender;
import org.apache.logging.log4j.core.appender.RollingFileAppender;
import org.apache.logging.log4j.core.appender.rolling.DefaultRolloverStrategy;
import org.apache.logging.log4j.core.appender.rolling.SizeBasedTriggeringPolicy;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.layout.PatternLayout;

public class AppenderUtil {
  public static FileAppender getFileAppender(
      Configuration config, PatternLayout layout, String logDirectory) {
    FileAppender appender =
        FileAppender.newBuilder()
            .setConfiguration(config)
            .withLayout(layout)
            .withFileName(logDirectory)
            .withName(logDirectory)
            .build();
    return appender;
  }

  public static PatternLayout getPatternLayout(Configuration config, String pattern) {
    PatternLayout layout =
        PatternLayout.newBuilder().withPattern(pattern).withConfiguration(config).build();
    return layout;
  }

  public static RollingFileAppender getRollingFileAppender(
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
    RollingFileAppender appender =
        RollingFileAppender.newBuilder()
            .withFileName(fileName)
            .withFilePattern(filePattern)
            .withLayout(layout)
            .withPolicy(triggeringPolicy)
            .withStrategy(rolloverStrategy)
            .setConfiguration(config)
            .withName(logDirectory)
            .build();
    return appender;
  }
}
