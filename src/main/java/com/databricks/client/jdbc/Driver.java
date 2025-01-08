package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.util.DriverUtil.getRootCauseMessage;
import static com.databricks.jdbc.telemetry.TelemetryHelper.*;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
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
import java.sql.*;
import java.util.Properties;
import java.util.TimeZone;

/** Databricks JDBC driver. */
public class Driver implements IDatabricksDriver, java.sql.Driver {
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
              connectionContext.getHostUrl(), getRootCauseMessage(e));
      LOGGER.error(e, errorMessage);
      throw new DatabricksSQLException(errorMessage, e, DatabricksDriverErrorCode.CONNECTION_ERROR, connectionContext);
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

  @Override
  public void closeConnection(String url, Properties info, String connectionId)
      throws SQLException {
    if (!acceptsURL(url)) {
      // Return null connection if URL is not accepted - as per JDBC standard.
      throw new DatabricksSQLException("Invalid connection Url {%s}", url);
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
