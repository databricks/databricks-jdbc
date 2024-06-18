package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.commons.util.AppenderUtil;
import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import com.databricks.sdk.core.UserAgent;
import java.sql.*;
import java.util.Properties;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;

/**
 * Databricks JDBC driver. TODO: Add implementation to accept Urls in format:
 * jdbc:databricks://host:port.
 */
public class DatabricksDriver implements Driver {

  private static final Logger LOGGER = LogManager.getLogger(DatabricksDriver.class);
  private static final DatabricksDriver INSTANCE;

  private static final int majorVersion = 0;
  private static final int minorVersion = 0;
  private static final int buildVersion = 1;

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new DatabricksDriver());
      System.out.printf("Driver has been registered. instance = %s\n", INSTANCE);
    } catch (SQLException e) {
      throw new IllegalStateException("Unable to register " + DatabricksDriver.class, e);
    }
  }

  @Override
  public boolean acceptsURL(String url) {
    return DatabricksConnectionContext.isValid(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws DatabricksSQLException {
    LOGGER.debug("public Connection connect(String url = {}, Properties info)", url);
    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(url, info);
    configureLogging(
        connectionContext.getLogPathString(),
        connectionContext.getLogLevel(),
        connectionContext.getLogFileCount(),
        connectionContext.getLogFileSize());
    setUserAgent(connectionContext);
    DatabricksMetrics.instantiateTelemetryClient(connectionContext);
    try {
      return new DatabricksConnection(connectionContext);
    } catch (Exception e) {
      throw new DatabricksSQLException(
          "Invalid or unknown token or hostname provided :" + connectionContext.getHostUrl(), e);
    }
  }

  @Override
  public int getMajorVersion() {
    return majorVersion;
  }

  @Override
  public int getMinorVersion() {
    return minorVersion;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public java.util.logging.Logger getParentLogger() {
    return null;
  }

  public static DatabricksDriver getInstance() {
    return INSTANCE;
  }

  public static void main(String[] args) {
    LOGGER.info("The driver {} has been initialized.", DatabricksDriver.class);
  }

  private static String getVersion() {
    return String.format("%d.%d.%d", majorVersion, minorVersion, buildVersion);
  }

  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
  }

  private static void configureLogger(Appender appender, Level logLevel) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    LoggerConfig loggerConfig = config.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);

    if (appender != null) {
      appender.start();
      config.addAppender(appender);
      loggerConfig.addAppender(appender, logLevel, null);
    }

    loggerConfig.setLevel(logLevel);
    context.updateLoggers();
  }

  public static void configureLogging(
      String logDirectory, Level logLevel, int logFileCount, int logFileSize) {
    LoggerContext context = (LoggerContext) LogManager.getContext(false);
    Configuration config = context.getConfiguration();
    PatternLayout layout = AppenderUtil.getPatternLayout(config, DEFAULT_LOG_PATTERN);
    boolean isFilePath = logDirectory.matches(".*\\.(log|txt|json|csv|xml|out)$");
    if (isFilePath) {
      configureLogger(AppenderUtil.getFileAppender(config, layout, logDirectory), logLevel);
    } else {
      configureLogger(
          AppenderUtil.getRollingFileAppender(
              config, layout, logDirectory, logFileSize, logFileCount - 1),
          logLevel);
    }
  }
}
