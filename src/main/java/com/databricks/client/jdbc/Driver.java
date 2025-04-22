package com.databricks.client.jdbc;

import static com.databricks.jdbc.telemetry.TelemetryHelper.*;

import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.SessionId;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.TimeZone;
import java.util.logging.Logger;

/** Databricks JDBC driver. */
public class Driver implements IDatabricksDriver, java.sql.Driver {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(Driver.class);
  private static final Driver INSTANCE;

  static {
    try {
      tryOpenModule();
      DriverManager.registerDriver(INSTANCE = new Driver());
    } catch (SQLException e) {
      throw new IllegalStateException("Unable to register " + Driver.class, e);
    }
  }

  private static void tryOpenModule() {
    try {
      if (Runtime.version().feature() >= 16) {
        // Get the java.base module
        Module javaBaseModule = Object.class.getModule();

        // Get the unnamed module where Arrow classes will be loaded
        Module unnamedModule = Driver.class.getClassLoader().getUnnamedModule();

        // Open java.nio to the unnamed module
        try {
          Method implAddOpens = Module.class.getDeclaredMethod("implAddOpens", String.class, Module.class);
          implAddOpens.setAccessible(true);
          implAddOpens.invoke(javaBaseModule, "java.nio", unnamedModule);
          System.out.println("Successfully opened java.nio module for Arrow");
        } catch (Exception e) {
          System.err.println("Warning: Could not open java.nio module: " + e.getMessage());
          System.err.println("You may need to add --add-opens=java.base/java.nio=ALL-UNNAMED to your JVM arguments");
        }

        // Pre-load a key Arrow class to trigger initialization with the module opened
        try {
          // This ensures the Arrow code initializes after we've opened the module
          Class.forName("com.databricks.internal.apache.arrow.memory.util.MemoryUtil");
        } catch (Exception e) {
          // This is expected to fail if MemoryUtil hasn't been loaded yet
          // We're just trying to trigger class loading in the right order
        }
      }
    } catch (Exception e) {
      // Log but continue
      System.err.println("Warning: Exception during Arrow preparation: " + e.getMessage());
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
    DriverUtil.setUpLogging(connectionContext);
    UserAgentManager.setUserAgent(connectionContext);
    LOGGER.info(getDriverSystemConfiguration().toString());
    DatabricksConnection connection = new DatabricksConnection(connectionContext);
    boolean isConnectionOpen = false;
    try {
      connection.open();
      isConnectionOpen = true;
      DriverUtil.resolveMetadataClient(connection);
      exportInitialTelemetryLog(connectionContext);
      return connection;
    } catch (Exception e) {
      if (!isConnectionOpen) {
        connection.close();
      }
      String errorMessage =
          String.format(
              "Connection failure while using the OSS Databricks JDBC driver. Failed to connect to server: %s\n%s",
              connectionContext.getHostUrl(), e);
      LOGGER.error(e, errorMessage);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.CONNECTION_ERROR);
    }
  }

  @Override
  public int getMajorVersion() {
    return DriverUtil.getDriverMajorVersion();
  }

  @Override
  public int getMinorVersion() {
    return DriverUtil.getDriverMinorVersion();
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws DatabricksSQLException {
    List<DriverPropertyInfo> missingProperties =
        DatabricksDriverPropertyUtil.getMissingProperties(url, info);
    return missingProperties.isEmpty()
        ? null
        : missingProperties.toArray(new DriverPropertyInfo[0]);
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() {
    return null;
  }

  public static Driver getInstance() {
    return INSTANCE;
  }

  @Override
  public void closeConnection(String url, Properties info, String connectionId)
      throws SQLException {
    if (!acceptsURL(url)) {
      throw new DatabricksSQLException(
          String.format("Invalid connection Url {%s}, Can't close connection.", url),
          DatabricksDriverErrorCode.CONNECTION_ERROR);
    }
    IDatabricksConnectionContext connectionContext =
        DatabricksConnectionContextFactory.create(url, info);
    IDatabricksClient databricksClient;
    if (connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      databricksClient = new DatabricksThriftServiceClient(connectionContext);
    } else {
      databricksClient = new DatabricksSdkClient(connectionContext);
    }
    databricksClient.deleteSession(SessionId.deserialize(connectionId).getSessionInfo());
  }
}
