package com.databricks.jdbc.driver;

import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.sdk.core.UserAgent;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

/**
 * Databricks JDBC driver. TODO: Add implementation to accept Urls in format:
 * jdbc:databricks://host:port.
 */
public class DatabricksDriver implements Driver {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksDriver.class);
  private static final DatabricksDriver INSTANCE;

  private static int majorVersion = 0;
  private static int minorVersion = 0;
  private static int buildVersion = 0;

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
  public Connection connect(String url, Properties info) {
    LOGGER.debug("public Connection connect(String url = {}, Properties info)", url);
    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(url, info);
    System.setProperty(SYSTEM_LOG_LEVEL_CONFIG, connectionContext.getLogLevelString());
    String logFileConfig = connectionContext.getLogPathString();
    if (logFileConfig != null) {
      System.setProperty(SYSTEM_LOG_FILE_CONFIG, logFileConfig);
    }
    UserAgent.withProduct(
        DatabricksJdbcConstants.DEFAULT_USER_AGENT, getVersion());
    UserAgent.withOtherInfo(USER_AGENT_PREFIX, connectionContext.getUserAgent());
    return new DatabricksConnection(connectionContext);
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

  public static void main(String[] args) {
    LOGGER.info("The driver {} has been initialized.", DatabricksDriver.class);
  }

  private String getVersion() {
    return String.format("%d.%d.%d", getMajorVersion(), getMinorVersion(), buildVersion);
  }
}
