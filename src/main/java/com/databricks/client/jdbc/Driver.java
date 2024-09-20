package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.UserAgent;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

/** Databricks JDBC driver. */
public class Driver implements java.sql.Driver {

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(Driver.class);
  private static final Driver INSTANCE;

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new Driver());
    } catch (SQLException e) {
      throw new IllegalStateException("Unable to register " + Driver.class, e);
    }
  }

  public static void main(String[] args) {
    TimeZone.setDefault(
        TimeZone.getTimeZone("UTC")); // Logging, timestamps are in UTC across the application
    System.out.printf("The driver {%s} has been initialized.%n", Driver.class);
  }

  public static void setUserAgent(IDatabricksConnectionContext connectionContext) {
    UserAgent.withProduct(DatabricksJdbcConstants.DEFAULT_USER_AGENT, DriverUtil.getVersion());
    UserAgent.withOtherInfo(CLIENT_USER_AGENT_PREFIX, connectionContext.getClientUserAgent());
  }

  @Override
  public boolean acceptsURL(String url) {
    return ValidationUtil.isValidJdbcUrl(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(url, info);
    try {
      LoggingUtil.setupLogger(
          connectionContext.getLogPathString(),
          connectionContext.getLogFileSize(),
          connectionContext.getLogFileCount(),
          connectionContext.getLogLevel());
    } catch (IOException e) {
      String errMsg =
          String.format(
              "Error initializing the Java Util Logger (JUL) with error: {%s}", e.getMessage());
      LOGGER.error(errMsg, e);
      throw new DatabricksSQLException(errMsg, e);
    }
    setUserAgent(connectionContext);
    DeviceInfoLogUtil.logProperties(connectionContext);
    try {
      DatabricksConnection connection = new DatabricksConnection(connectionContext);
      DeviceInfoLogUtil.exportDeviceProperties(connection.getSession());
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
          MetricsUtil.exportErrorWithoutAuth(
              ErrorTypes.COMMUNICATION_FAILURE, null, ErrorCodes.COMMUNICATION_FAILURE);
          throw new DatabricksSQLException(
              errorMessage,
              cause.getCause(),
              ErrorTypes.COMMUNICATION_FAILURE,
              ErrorCodes.COMMUNICATION_FAILURE);
        }
        cause = cause.getCause();
      }
      String errorMessage =
          "Communication link failure. Failed to connect to server. : "
              + connectionContext.getHostUrl()
              + e.getMessage();
      MetricsUtil.exportErrorWithoutAuth(
          ErrorTypes.COMMUNICATION_FAILURE, null, ErrorCodes.COMMUNICATION_FAILURE);
      throw new DatabricksSQLException(
          errorMessage, e, ErrorTypes.COMMUNICATION_FAILURE, ErrorCodes.COMMUNICATION_FAILURE);
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

  private void setMetadataClient(
      DatabricksConnection connection, IDatabricksConnectionContext connectionContext) {
    if (connectionContext.getUseLegacyMetadata().equals(true)) {
      LOGGER.debug(
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
      LOGGER.debug(
          String.format(
              "Unable to parse the DBSQL version {%s}. Falling back to legacy metadata commands.",
              dbsqlVersion));
      return false;
    }
  }
}
