package com.databricks.client.jdbc;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.commons.ErrorTypes;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.DeviceInfoLogUtil;
import com.databricks.jdbc.commons.util.DriverUtil;
import com.databricks.jdbc.commons.util.ErrorCodes;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksConnection;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.DatabricksConnectionContext;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.core.UserAgent;
import java.sql.*;
import java.util.Properties;

/**
 * Databricks JDBC driver. TODO: Add implementation to accept Urls in format:
 * jdbc:databricks://host:port.
 */
public class Driver implements java.sql.Driver {
  private static final Driver INSTANCE;

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new Driver());
      System.out.printf("Driver has been registered. instance = %s\n", INSTANCE);
    } catch (SQLException e) {
      throw new IllegalStateException("Unable to register " + Driver.class, e);
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
    DeviceInfoLogUtil.logProperties(connectionContext);
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
          String errorMessage =
              "Communication link failure. Failed to connect to server. : "
                  + connectionContext.getHostUrl()
                  + cause.getMessage();
          throw new DatabricksSQLException(
              errorMessage,
              cause.getCause(),
              connectionContext,
              ErrorTypes.COMMUNICATION_FAILURE,
              null,
              ErrorCodes.COMMUNICATION_FAILURE);
        }
        cause = cause.getCause();
      }
      String errorMessage =
          "Communication link failure. Failed to connect to server. : "
              + connectionContext.getHostUrl()
              + e.getMessage();
      throw new DatabricksSQLException(
          errorMessage,
          e,
          connectionContext,
          ErrorTypes.COMMUNICATION_FAILURE,
          null,
          ErrorCodes.COMMUNICATION_FAILURE);
    }
  }

  private void setMetadataClient(
      DatabricksConnection connection, IDatabricksConnectionContext connectionContext) {
    if (connectionContext.getUseLegacyMetadata().equals(true)) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          "The new metadata commands are enabled, but the legacy metadata commands are being used due to connection parameter useLegacyMetadata");
      connection.setMetadataClient(true);
    } else {
      connection.setMetadataClient(false);
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
    return DriverUtil.getMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return DriverUtil.getMinorVersion();
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

  public static Driver getInstance() {
    return INSTANCE;
  }

  public static void main(String[] args) {
    System.out.printf("The driver {%s} has been initialized.%n", Driver.class);
  }

  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, DriverUtil.getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
  }
}
