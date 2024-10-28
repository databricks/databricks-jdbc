package com.databricks.client.jdbc;

import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

/** Databricks JDBC driver. */
public class Driver implements java.sql.Driver {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(Driver.class);
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

  @Override
  public boolean acceptsURL(String url) {
    return ValidationUtil.isValidJdbcUrl(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws DatabricksSQLException {
    if (!acceptsURL(url)) {
      // Return null connection if URL is not accepted - as per JDBC standard.
      return null;
    }
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(url, info);

    setUpLogging(connectionContext);
    UserAgentManager.setUserAgent(connectionContext);
    DeviceInfoLogUtil.logProperties();
    DatabricksConnection connection = new DatabricksConnection(connectionContext);
    boolean isConnectionOpen = false;
    try {
      connection.open();
      isConnectionOpen = true;
      resolveMetadataClient(connection, connectionContext);
      return connection;
    } catch (Exception e) {
      if (!isConnectionOpen) {
        connection.close();
      }
      String errorMessage =
          String.format(
              "Communication link failure. Failed to connect to server: %s",
              connectionContext.getHostUrl());
      Throwable rootCause = getRootCause(e);

      if (rootCause instanceof DatabricksSQLException) {
        errorMessage += rootCause.getMessage();
      } else {
        errorMessage += e.getMessage();
      }

      throw new DatabricksSQLException(
          errorMessage,
          rootCause,
          ErrorTypes.COMMUNICATION_FAILURE,
          ErrorCodes.COMMUNICATION_FAILURE);
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

  private static void setUpLogging(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
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
      LOGGER.error(e, errMsg);
      throw new DatabricksSQLException(errMsg, e);
    }
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null && cause != throwable) {
      throwable = cause;
    }
    return throwable;
  }

  private static void resolveMetadataClient(
      IDatabricksConnection connection, IDatabricksConnectionContext connectionContext) {
    if (connectionContext.getClientType() == DatabricksClientType.SQL_EXEC
        && connectionContext.getUseEmptyMetadata()) {
      LOGGER.warn("Empty metadata client is being used.");
      connection.getSession().setEmptyMetadataClient();
    }
  }
}
