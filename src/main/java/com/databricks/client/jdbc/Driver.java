package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.DatabricksJdbcUrlParams.*;
import static com.databricks.jdbc.common.util.DatabricksDriverPropertyUtil.getInvalidUrlPropertyInfo;
import static com.databricks.jdbc.common.util.DriverUtil.getRootCauseMessage;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.api.impl.DatabricksConnectionContextFactory;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.base.Strings;
import java.sql.*;
import java.util.*;
import java.util.logging.Logger;
import java.util.regex.Matcher;

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
    DriverUtil.setUpLogging(connectionContext);
    UserAgentManager.setUserAgent(connectionContext);
    DeviceInfoLogUtil.logProperties();
    DatabricksConnection connection = new DatabricksConnection(connectionContext);
    boolean isConnectionOpen = false;
    try {
      connection.open();
      isConnectionOpen = true;
      DriverUtil.resolveMetadataClient(connection);
      return connection;
    } catch (Exception e) {
      if (!isConnectionOpen) {
        connection.close();
      }
      String errorMessage =
          String.format(
              "Connection failure while using the OSS Databricks JDBC driver. Failed to connect to server: %s\n%s",
              connectionContext.getHostUrl(), getRootCauseMessage(e));
      LOGGER.error(errorMessage);
      throw new DatabricksSQLException(errorMessage);
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
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
      throws DatabricksSQLException {
    DriverPropertyInfo[] propertyInfos = null;
    if (Strings.isNullOrEmpty(url)) {
      return propertyInfos;
    }
    Matcher urlMatcher = JDBC_URL_PATTERN.matcher(url);
    if (!urlMatcher.matches()) {
      propertyInfos = new DriverPropertyInfo[1];
      propertyInfos[0] = new DriverPropertyInfo("host", null);
      propertyInfos[0].required = true;
      propertyInfos[0].description =
          "JDBC URL must be in the form: <protocol>://<host or domain>:<port>/<path>";
      return propertyInfos;
    }
    String connectionParamString = urlMatcher.group(2);
    if (!connectionParamString.toLowerCase().contains(HTTP_PATH.getParamName())) {
      return getInvalidUrlPropertyInfo(HTTP_PATH);
    }
    if (!connectionParamString.toLowerCase().contains(AUTH_MECH.getParamName())) {
      getInvalidUrlPropertyInfo(AUTH_MECH);
    }

    List<DriverPropertyInfo> missingProperties =
        DatabricksDriverPropertyUtil.getMissingProperties(connectionParamString, info);
    if (!missingProperties.isEmpty()) {
      propertyInfos = new DriverPropertyInfo[missingProperties.size()];
      propertyInfos = missingProperties.toArray(propertyInfos);
    }
    return propertyInfos;
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
}
