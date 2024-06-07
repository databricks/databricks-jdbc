package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.sdk.core.UserAgent;
import java.sql.*;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Databricks JDBC driver. TODO: Add implementation to accept Urls in format:
 * jdbc:databricks://host:port.
 */
public class DatabricksDriver implements Driver {

  private static Logger LOGGER;
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
    IDatabricksConnectionContext connectionContext = DatabricksConnectionContext.parse(url, info);
    LOGGER = LogManager.getLogger(DatabricksDriver.class);
    LOGGER.debug("public Connection connect(String url = {}, Properties info)", url);
    setUserAgent(connectionContext);
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
}
