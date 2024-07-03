package com.databricks.jdbc.driver;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.DeviceInfoLogUtil;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import com.databricks.sdk.core.UserAgent;
import java.sql.*;
import java.util.Properties;

/**
 * Databricks JDBC driver. TODO: Add implementation to accept Urls in format:
 * jdbc:databricks://host:port.
 */
public class DatabricksDriver implements Driver {
  private static final DatabricksDriver INSTANCE;
  private static final int majorVersion = 0;
  private static final int minorVersion = 7;
  private static final int buildVersion = 0;
  private static final String qualifier = "oss";

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
    LoggingUtil.setupLogger(
        connectionContext.getLogPathString(),
        connectionContext.getLogFileSize(),
        connectionContext.getLogFileCount(),
        connectionContext.getLogLevel());
    setUserAgent(connectionContext);
    DatabricksMetrics.instantiateTelemetryClient(connectionContext);
    DeviceInfoLogUtil.logProperties();
    try {
      DatabricksConnection connection = new DatabricksConnection(connectionContext);
      if (connectionContext.getClientType() == DatabricksClientType.SQL_EXEC) {
        setMetadataClient(connection, connectionContext);
      }
      return connection;
    } catch (Exception e) {
      Throwable cause = e;
      while (cause != null) {
        if (cause instanceof DatabricksSQLException) {
          throw new DatabricksSQLException(
              "Communication link failure. Failed to connect to server. : "
                  + connectionContext.getHostUrl()
                  + cause.getMessage(),
              cause.getCause());
        }
        cause = cause.getCause();
      }
      throw new DatabricksSQLException(
          "Communication link failure. Failed to connect to server. :"
              + connectionContext.getHostUrl()
              + e.getMessage(),
          e);
    }
  }

  private void setMetadataClient(
      DatabricksConnection connection, IDatabricksConnectionContext connectionContext) {
    try {
      ResultSet getDBSQLVersionInfo =
          connection.createStatement().executeQuery("SELECT current_version().dbsql_version");
      getDBSQLVersionInfo.next();
      String dbsqlVersion = getDBSQLVersionInfo.getString(1);
      LoggingUtil.log(
          LogLevel.DEBUG,
          String.format("Connected to Databricks DBSQL version: {%s}", dbsqlVersion));
      if (checkSupportForNewMetadata(dbsqlVersion)) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            String.format(
                "The Databricks DBSQL version {%s} supports the new metadata commands.",
                dbsqlVersion));
        if (connectionContext.getUseLegacyMetadata().equals(true)) {
          LoggingUtil.log(
              LogLevel.DEBUG,
              "The new metadata commands are enabled, but the legacy metadata commands are being used due to connection parameter useLegacyMetadata");
          connection.setMetadataClient(true);
        } else {
          connection.setMetadataClient(false);
        }
      } else {
        LoggingUtil.log(
            LogLevel.DEBUG,
            String.format(
                "The Databricks DBSQL version {%s} does not support the new metadata commands. Falling back to legacy metadata commands.",
                dbsqlVersion));
        connection.setMetadataClient(true);
      }
    } catch (SQLException e) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          String.format(
              "Unable to get the DBSQL version. Falling back to legacy metadata commands. Error : %s",
              e));
      connection.setMetadataClient(true);
    }
  }

  private boolean checkSupportForNewMetadata(String dbsqlVersion) {
    try {
      int majorVersion = Integer.parseInt(dbsqlVersion.split("\\.")[0]);
      int minorVersion = Integer.parseInt(dbsqlVersion.split("\\.")[1]);

      if (majorVersion > DBSQL_MIN_MAJOR_VERSION_FOR_NEW_METADATA) {
        return true;
      } else if (majorVersion == DBSQL_MIN_MAJOR_VERSION_FOR_NEW_METADATA) {
        return minorVersion >= DBSQL_MIN_MINOR_VERSION_FOR_NEW_METADATA;
      } else {
        return false;
      }
    } catch (Exception e) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          String.format(
              "Unable to parse the DBSQL version {%s}. Falling back to legacy metadata commands.",
              dbsqlVersion));
      return false;
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
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("The driver {%s} has been initialized.", DatabricksDriver.class));
  }

  private static String getVersion() {
    return String.format("%d.%d.%d-%s", majorVersion, minorVersion, buildVersion, qualifier);
  }

  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
  }
}
