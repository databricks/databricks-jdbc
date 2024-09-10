package com.databricks.client.jdbc;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

/** Databricks JDBC driver. */
public class Driver implements java.sql.Driver {
  private static final Driver INSTANCE;

  static {
    try {
      DriverManager.registerDriver(INSTANCE = new Driver());
    } catch (SQLException e) {
      throw new IllegalStateException("Unable to register " + Driver.class, e);
    }
  }

  public static void main(String[] args) {
    System.out.printf("The driver {%s} has been initialized.%n", Driver.class);
  }

  @Override
  public boolean acceptsURL(String url) {
    return ValidationUtil.isValidJdbcUrl(url);
  }

  @Override
  public Connection connect(String url, Properties info) throws DatabricksSQLException {
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(url, info);

    setUpLogging(connectionContext);
    UserAgentManager.setUserAgent(connectionContext);
    DeviceInfoLogUtil.logProperties(connectionContext);
    try {
      return new DatabricksConnection(connectionContext);
    } catch (Exception e) {
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
          connectionContext,
          ErrorTypes.COMMUNICATION_FAILURE,
          null,
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
      throw new DatabricksSQLException("Error initializing the Java Util Logger (JUL).", e);
    }
  }

  private static Throwable getRootCause(Throwable throwable) {
    Throwable cause;
    while ((cause = throwable.getCause()) != null && cause != throwable) {
      throwable = cause;
    }
    return throwable;
  }
}
