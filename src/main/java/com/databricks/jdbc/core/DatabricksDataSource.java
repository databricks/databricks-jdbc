package com.databricks.jdbc.core;

import static com.databricks.jdbc.driver.DatabricksJdbcConstants.*;

import com.databricks.jdbc.driver.DatabricksDriver;
import com.databricks.jdbc.driver.DatabricksJdbcConstants;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import javax.sql.DataSource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class DatabricksDataSource implements DataSource {
  private static final Logger LOGGER = LogManager.getLogger(DatabricksDataSource.class);
  private String host;
  private int port;
  private String httpPath;
  private Properties properties = new Properties();
  private final DatabricksDriver databricksDriver;

  public DatabricksDataSource() {
    this.databricksDriver = DatabricksDriver.getInstance();
  }

  @VisibleForTesting
  public DatabricksDataSource(DatabricksDriver databricksDriver) {
    this.databricksDriver = databricksDriver;
  }

  @Override
  public Connection getConnection() throws DatabricksSQLException {
    LOGGER.debug("public Connection getConnection()");
    return getConnection(this.getUsername(), this.getPassword());
  }

  @Override
  public Connection getConnection(String username, String password) throws DatabricksSQLException {
    LOGGER.debug(
        "public Connection getConnection(String username = {}, String password = {})",
        username,
        password);
    if (username != null) {
      setUsername(username);
    }
    if (password != null) {
      setPassword(password);
    }
    return databricksDriver.connect(getUrl(), properties);
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    throw new SQLFeatureNotSupportedException("public PrintWriter getLogWriter()");
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    throw new SQLFeatureNotSupportedException("public void setLogWriter(PrintWriter out)");
  }

  @Override
  public void setLoginTimeout(int seconds) {
    LOGGER.debug("public void setLoginTimeout(int seconds = {})", seconds);
    this.properties.put(DatabricksJdbcConstants.LOGIN_TIMEOUT, seconds);
  }

  @Override
  public int getLoginTimeout() {
    return (int) this.properties.get(DatabricksJdbcConstants.LOGIN_TIMEOUT);
  }

  @Override
  public java.util.logging.Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("public Logger getParentLogger()");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  public String getUrl() {
    LOGGER.debug("public String getUrl()");
    StringBuilder urlBuilder = new StringBuilder();
    urlBuilder.append(DatabricksJdbcConstants.JDBC_SCHEMA);
    if (host == null) {
      throw new IllegalStateException("Host is required");
    }
    urlBuilder.append(host);
    if (port != 0) {
      urlBuilder.append(PORT_DELIMITER).append(port);
    }
    if (httpPath != null) {
      urlBuilder.append(URL_DELIMITER).append(HTTP_PATH).append(PAIR_DELIMITER).append(httpPath);
    }
    return urlBuilder.toString();
  }

  public String getUsername() {
    return properties.getProperty(DatabricksJdbcConstants.USER);
  }

  public void setUsername(String username) {
    properties.put(DatabricksJdbcConstants.USER, username);
  }

  public String getPassword() {
    return properties.getProperty(DatabricksJdbcConstants.PASSWORD);
  }

  public void setPassword(String password) {
    properties.put(DatabricksJdbcConstants.PASSWORD, password);
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getHttpPath() {
    return httpPath;
  }

  public void setHttpPath(String httpPath) {
    this.httpPath = httpPath;
  }

  public Properties getProperties() {
    return properties;
  }

  public void setProperties(Properties properties) {
    this.properties = properties;
  }
}
