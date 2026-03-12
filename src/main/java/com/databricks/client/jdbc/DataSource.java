package com.databricks.client.jdbc;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;

import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.pooling.DatabricksPooledConnection;
import com.google.common.annotations.VisibleForTesting;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;

public class DataSource implements javax.sql.DataSource, ConnectionPoolDataSource {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DataSource.class);
  private String user = DEFAULT_USERNAME;
  private String host;
  private int port;
  private String httpPath;
  private Properties properties = new Properties();
  private final Driver driver;

  public DataSource() {
    this.driver = Driver.getInstance();
  }

  @VisibleForTesting
  public DataSource(Driver driver) {
    this.driver = driver;
  }

  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    return getConnection(this.getUsername(), this.getPassword());
  }

  @Override
  public Connection getConnection(String username, String password) throws SQLException {
    LOGGER.debug("public Connection getConnection(String, String)");
    if (username != null) {
      setUsername(username);
    }
    if (password != null) {
      setPassword(password);
    }
    return driver.connect(getUrl(), properties);
  }

  @Override
  public PooledConnection getPooledConnection() throws SQLException {
    LOGGER.debug("public PooledConnection getPooledConnection()");
    return new DatabricksPooledConnection(getConnection());
  }

  @Override
  public PooledConnection getPooledConnection(String user, String password) throws SQLException {
    LOGGER.debug("public PooledConnection getPooledConnection(String, String)");
    return new DatabricksPooledConnection(getConnection(user, password));
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
      urlBuilder
          .append(URL_DELIMITER)
          .append(DatabricksJdbcUrlParams.HTTP_PATH.getParamName())
          .append(PAIR_DELIMITER)
          .append(httpPath);
    }
    return urlBuilder.toString();
  }

  public String getUsername() {
    return user;
  }

  public void setUsername(String user) {
    this.user = user;
  }

  public String getPassword() {
    return properties.getProperty(
        DatabricksJdbcUrlParams.PASSWORD.getParamName(),
        properties.getProperty(DatabricksJdbcUrlParams.PWD.getParamName()));
  }

  public void setPassword(String password) {
    properties.put(DatabricksJdbcUrlParams.PASSWORD.getParamName(), password);
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
