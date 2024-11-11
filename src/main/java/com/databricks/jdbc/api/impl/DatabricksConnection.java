package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.*;
import com.databricks.jdbc.api.IDatabricksConnection;
import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.volume.DBFSVolumeClient;
import com.databricks.jdbc.api.impl.volume.DatabricksUCVolumeClient;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksSQLClientInfoException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** Implementation for Databricks specific connection. */
public class DatabricksConnection implements IDatabricksConnection {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksConnection.class);
  private final IDatabricksSession session;
  private final Set<IDatabricksStatementInternal> statementSet = ConcurrentHashMap.newKeySet();
  private SQLWarning warnings = null;
  private volatile IDatabricksVolumeClient volumeClient = null;
  private final IDatabricksConnectionContext connectionContext;

  /**
   * Creates an instance of Databricks connection for given connection context.
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksConnection(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    this.connectionContext = connectionContext;
    this.session = new DatabricksSession(connectionContext);
  }

  @VisibleForTesting
  public DatabricksConnection(
      IDatabricksConnectionContext connectionContext, IDatabricksClient testDatabricksClient)
      throws DatabricksSQLException {
    this.connectionContext = connectionContext;
    this.session = new DatabricksSession(connectionContext, testDatabricksClient);
    UserAgentManager.setUserAgent(connectionContext);
  }

  @Override
  public void open() throws DatabricksSQLException {
    this.session.open();
  }

  @Override
  public Statement getStatement(String statementId) throws SQLException {
    return new DatabricksStatement(this, StatementId.deserialize(statementId));
  }

  @Override
  public IDatabricksSession getSession() {
    return session;
  }

  @Override
  public Statement createStatement() {
    LOGGER.debug("public Statement createStatement()");
    DatabricksStatement statement = new DatabricksStatement(this);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) {
    LOGGER.debug(
        String.format("public PreparedStatement prepareStatement(String sql = {%s})", sql));
    DatabricksPreparedStatement statement = new DatabricksPreparedStatement(this, sql);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    LOGGER.debug(String.format("public CallableStatement prepareCall= {%s})", sql));
    throw new DatabricksSQLFeatureNotSupportedException("Not Supported");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    LOGGER.debug(String.format("public String nativeSQL(String sql{%s})", sql));
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - nativeSQL(String sql)");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setAutoCommit(boolean autoCommit)");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    LOGGER.debug("public boolean getAutoCommit()");
    throwExceptionIfConnectionIsClosed();
    return true;
  }

  @Override
  public void commit() throws SQLException {
    LOGGER.debug("public void commit()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - commit()");
  }

  @Override
  public void rollback() throws SQLException {
    LOGGER.debug("public void rollback()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - rollback()");
  }

  @Override
  public void close() throws DatabricksSQLException {
    LOGGER.debug("public void close()");
    for (IDatabricksStatementInternal statement : statementSet) {
      statement.close(false);
      statementSet.remove(statement);
    }
    this.session.close();
    DatabricksHttpClientFactory.getInstance().removeClient(this.session.getConnectionContext());
  }

  @Override
  public boolean isClosed() throws SQLException {
    LOGGER.debug("public boolean isClosed()");
    return session == null || !session.isOpen();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    LOGGER.debug("public DatabaseMetaData getMetaData()");
    return new DatabricksDatabaseMetaData(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    LOGGER.debug("public void setReadOnly(boolean readOnly = {})");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setReadOnly(boolean readOnly)");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    LOGGER.debug("public boolean isReadOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    this.session.setCatalog(catalog);
    Statement statement = this.createStatement();
    statement.execute("SET CATALOG " + catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    LOGGER.debug("public String getCatalog()");
    return this.session.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    LOGGER.debug("public void setTransactionIsolation(int level = {})");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setTransactionIsolation(int level)");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    LOGGER.debug("public int getTransactionIsolation()");
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_READ_UNCOMMITTED;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    LOGGER.debug("public SQLWarning getWarnings()");
    throwExceptionIfConnectionIsClosed();
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    LOGGER.debug("public void clearWarnings()");
    throwExceptionIfConnectionIsClosed();
    warnings = null;
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Only ResultSet.TYPE_FORWARD_ONLY and ResultSet.CONCUR_READ_ONLY are supported");
    }
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {

    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Only ResultSet.TYPE_FORWARD_ONLY and ResultSet.CONCUR_READ_ONLY are supported");
    }
    return prepareStatement(sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareCall(String sql, int resultSetType, int resultSetConcurrency)");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    LOGGER.debug("public Map<String, Class<?>> getTypeMap()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - getTypeMap()");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("public void setTypeMap(Map<String, Class<?>> map)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setTypeMap(Map<String, Class<?>> map)");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setHoldability(int holdability)");
  }

  @Override
  public int getHoldability() throws SQLException {
    LOGGER.debug("public int getHoldability()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - getHoldability()");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    LOGGER.debug("public Savepoint setSavepoint()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setSavepoint()");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setSavepoint(String name)");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void rollback(Savepoint savepoint)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - rollback(Savepoint savepoint)");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void releaseSavepoint(Savepoint savepoint)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - releaseSavepoint(Savepoint savepoint)");
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareStatement(String sql, int autoGeneratedKeys)");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareStatement(String sql, int[] columnIndexes)");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {

    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - prepareStatement(String sql, String[] columnNames)");
  }

  @Override
  public Clob createClob() throws SQLException {
    LOGGER.debug("public Clob createClob()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createClob()");
  }

  @Override
  public Blob createBlob() throws SQLException {
    LOGGER.debug("public Blob createBlob()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createBlob()");
  }

  @Override
  public NClob createNClob() throws SQLException {
    LOGGER.debug("public NClob createNClob()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createNClob()");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    LOGGER.debug("public SQLXML createSQLXML()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createSQLXML()");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    ValidationUtil.checkIfNonNegative(timeout, "timeout");
    try (DatabricksStatement statement = new DatabricksStatement(this)) {
      statement.setQueryTimeout(timeout);
      // simple query to check whether connection is working
      statement.execute("SELECT 1");
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keySet().stream()
        .map(String::toLowerCase)
        .anyMatch(s -> s.equalsIgnoreCase(name))) {
      Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
      setSessionConfig(name, value, failedProperties);
      if (!failedProperties.isEmpty()) {
        throw new DatabricksSQLClientInfoException(
            getFailedPropertiesExceptionMessage(failedProperties), failedProperties);
      }
    } else {
      if (DatabricksJdbcConstants.ALLOWED_CLIENT_INFO_PROPERTIES.stream()
          .map(String::toLowerCase)
          .anyMatch(s -> s.equalsIgnoreCase(name))) {
        this.session.setClientInfoProperty(name.toLowerCase(), value);
      } else {
        Map<String, ClientInfoStatus> clientInfoStatusMap = new HashMap<>();
        clientInfoStatusMap.put(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);

        // Throw the exception with an immutable map
        throw new DatabricksSQLClientInfoException(
            String.format(
                "Setting client info for %s failed with %s",
                name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY),
            Collections.unmodifiableMap(clientInfoStatusMap));
      }
    }
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    LOGGER.debug("public void setClientInfo(Properties properties)");
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      setClientInfo((String) entry.getKey(), (String) entry.getValue());
    }
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    // Return session conf if set
    if (this.session.getSessionConfigs().containsKey(name)) {
      return this.session.getSessionConfigs().get(name);
    } else if (this.session.getClientInfoProperties().containsKey(name.toLowerCase())) {
      return this.session.getClientInfoProperties().get(name.toLowerCase());
    }

    // Else return default value or null if the conf name is invalid
    return DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.getOrDefault(
        name, null);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    LOGGER.debug("public Properties getClientInfo()");
    Properties properties = new Properties();
    // Put in default values first
    properties.putAll(DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP);
    // Then override with session confs
    properties.putAll(this.session.getSessionConfigs());
    // Add all client info properties
    properties.putAll(this.session.getClientInfoProperties());
    return properties;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    LOGGER.debug("public Array createArrayOf(String typeName, Object[] elements)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createArrayOf(String typeName, Object[] elements)");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    LOGGER.debug("public Struct createStruct(String typeName, Object[] attributes)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - createStruct(String typeName, Object[] attributes)");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    session.setSchema(schema);
    Statement statement = this.createStatement();
    statement.execute("USE SCHEMA " + schema);
  }

  @Override
  public String getSchema() throws SQLException {
    LOGGER.debug("public String getSchema()");
    return session.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    LOGGER.debug("public void abort(Executor executor)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - abort(Executor executor)");
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    LOGGER.debug("public void setNetworkTimeout(Executor executor, int milliseconds)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - setNetworkTimeout(Executor executor, int milliseconds)");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    LOGGER.debug("public int getNetworkTimeout()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksConnection - getNetworkTimeout()");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    if (iface.isInstance(this)) {
      return (T) this;
    }
    throw new DatabricksSQLException(
        String.format(
            "Class {%s} cannot be wrapped from {%s}", this.getClass().getName(), iface.getName()));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    return iface.isInstance(this);
  }

  @Override
  public void closeStatement(IDatabricksStatement statement) {
    LOGGER.debug("public void closeStatement(IDatabricksStatement statement)");
    this.statementSet.remove(statement);
  }

  @Override
  public Connection getConnection() {
    return this;
  }

  @Override
  public IDatabricksVolumeClient getVolumeClient() {
    if (volumeClient == null) {
      synchronized (this) {
        if (volumeClient == null) {
          if (this.session.getConnectionContext().useFileSystemAPI()) {
            volumeClient = new DBFSVolumeClient(this);
          } else {
            volumeClient = new DatabricksUCVolumeClient(this);
          }
        }
      }
    }
    return volumeClient;
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  /**
   * This function creates the exception message for the failed setClientInfo command
   *
   * @param failedProperties contains the map for the failed properties
   * @return the exception message
   */
  private static String getFailedPropertiesExceptionMessage(
      Map<String, ClientInfoStatus> failedProperties) {
    return failedProperties.entrySet().stream()
        .map(e -> String.format("Setting config %s failed with %s", e.getKey(), e.getValue()))
        .collect(Collectors.joining("\n"));
  }

  /**
   * This function determines the reason for the failure of setting a session config form the
   * exception message
   *
   * @param key for which set command failed
   * @param value for which set command failed
   * @param e exception thrown by the set command
   * @return the reason for the failure in ClientInfoStatus
   */
  private static ClientInfoStatus determineClientInfoStatus(String key, String value, Throwable e) {
    String invalidConfigMessage = String.format("Configuration %s is not available", key);
    String invalidValueMessage = String.format("Unsupported configuration %s=%s", key, value);
    String errorMessage = e.getCause().getMessage();
    if (errorMessage.contains(invalidConfigMessage))
      return ClientInfoStatus.REASON_UNKNOWN_PROPERTY;
    else if (errorMessage.contains(invalidValueMessage))
      return ClientInfoStatus.REASON_VALUE_INVALID;
    return ClientInfoStatus.REASON_UNKNOWN;
  }

  /**
   * This function sets the session config for the given key and value. If the setting fails, the
   * key and the reason for failure are added to the failedProperties map.
   *
   * @param key for the session conf
   * @param value for the session conf
   * @param failedProperties to add the key to, if the set command fails
   */
  private void setSessionConfig(
      String key, String value, Map<String, ClientInfoStatus> failedProperties) {
    try {
      this.createStatement().execute(String.format("SET %s = %s", key, value));
      this.session.setSessionConfig(key, value);
    } catch (SQLException e) {
      ClientInfoStatus status = determineClientInfoStatus(key, value, e);
      failedProperties.put(key, status);
    }
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    if (this.isClosed()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }
}
