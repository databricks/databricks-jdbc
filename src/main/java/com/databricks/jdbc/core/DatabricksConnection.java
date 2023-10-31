package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for Databricks specific connection. */
public class DatabricksConnection implements IDatabricksConnection, Connection {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksConnection.class);
  private final IDatabricksSession session;
  private final Set<IDatabricksStatement> statementSet = ConcurrentHashMap.newKeySet();

  /**
   * Creates an instance of Databricks connection for given connection context.
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksConnection(IDatabricksConnectionContext connectionContext) {
    this.session = new DatabricksSession(connectionContext);
    this.session.open();
  }

  @VisibleForTesting
  DatabricksConnection(
      IDatabricksConnectionContext connectionContext, DatabricksClient databricksClient) {
    this.session = new DatabricksSession(connectionContext, databricksClient);
    this.session.open();
  }

  @Override
  public IDatabricksSession getSession() {
    return session;
  }

  @Override
  public Statement createStatement() throws SQLException {
    LOGGER.debug("public Statement createStatement()");
    DatabricksStatement statement = new DatabricksStatement(this);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    LOGGER.debug("public PreparedStatement prepareStatement(String sql = {})", sql);
    DatabricksPreparedStatement statement = new DatabricksPreparedStatement(this, sql);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    LOGGER.debug("public CallableStatement prepareCall(String sql = {})", sql);
    throw new SQLFeatureNotSupportedException("Unsupported feature");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    LOGGER.debug("public String nativeSQL(String sql = {})", sql);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    LOGGER.debug("public void setAutoCommit(boolean autoCommit = {})", autoCommit);
    throw new UnsupportedOperationException("Not implemented");
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void rollback() throws SQLException {
    LOGGER.debug("public void rollback()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws SQLException {
    LOGGER.debug("public void close()");
    for (IDatabricksStatement statement : statementSet) {
      statement.close(false);
      statementSet.remove(statement);
    }

    this.session.close();
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
    LOGGER.debug("public void setReadOnly(boolean readOnly = {})", readOnly);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    LOGGER.debug("public boolean isReadOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    LOGGER.debug("public void setCatalog(String catalog + {})", catalog);
    this.session.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    LOGGER.debug("public String getCatalog()");
    return this.session.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    LOGGER.debug("public void setTransactionIsolation(int level = {})", level);
    throw new UnsupportedOperationException("Not implemented");
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
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearWarnings() throws SQLException {
    LOGGER.debug("public void clearWarnings()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    LOGGER.debug(
        "public Statement createStatement(int resultSetType = {}, int resultSetConcurrency = {})",
        resultSetType,
        resultSetConcurrency);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    LOGGER.debug(
        "public PreparedStatement prepareStatement(String sql = {}, int resultSetType = {}, int resultSetConcurrency = {})",
        sql,
        resultSetType,
        resultSetConcurrency);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency)
      throws SQLException {
    LOGGER.debug(
        "public CallableStatement prepareCall(String sql = {}, int resultSetType = {}, int resultSetConcurrency = {})",
        sql,
        resultSetType,
        resultSetConcurrency);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    LOGGER.debug("public Map<String, Class<?>> getTypeMap()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("public void setTypeMap(Map<String, Class<?>> map)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    LOGGER.debug("public void setHoldability(int holdability = {})", holdability);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getHoldability() throws SQLException {
    LOGGER.debug("public int getHoldability()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    LOGGER.debug("public Savepoint setSavepoint()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    LOGGER.debug("public Savepoint setSavepoint(String name = {})", name);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void rollback(Savepoint savepoint)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void releaseSavepoint(Savepoint savepoint)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    LOGGER.debug(
        "public Statement createStatement(int resultSetType = {}, int resultSetConcurrency = {}, int resultSetHoldability = {})",
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    LOGGER.debug(
        "public PreparedStatement prepareStatement(String sql = {}, int resultSetType = {}, int resultSetConcurrency = {}, int resultSetHoldability = {})",
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    LOGGER.debug(
        "public CallableStatement prepareCall(String sql = {}, int resultSetType = {}, int resultSetConcurrency = {}, int resultSetHoldability = {})",
        sql,
        resultSetType,
        resultSetConcurrency,
        resultSetHoldability);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    LOGGER.debug(
        "public PreparedStatement prepareStatement(String sql = {}, int autoGeneratedKeys = {})",
        sql,
        autoGeneratedKeys);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    LOGGER.debug(
        "public PreparedStatement prepareStatement(String sql = {}, int[] columnIndexes = {})",
        sql,
        columnIndexes);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    LOGGER.debug(
        "public PreparedStatement prepareStatement(String sql = {}, String[] columnNames = {})",
        sql,
        columnNames);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Clob createClob() throws SQLException {
    LOGGER.debug("public Clob createClob()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Blob createBlob() throws SQLException {
    LOGGER.debug("public Blob createBlob()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public NClob createNClob() throws SQLException {
    LOGGER.debug("public NClob createNClob()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    LOGGER.debug("public SQLXML createSQLXML()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    LOGGER.debug("public boolean isValid(int timeout = {})", timeout);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    LOGGER.debug("public void setClientInfo(String name = {}, String value = {})", name, value);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    LOGGER.debug("public void setClientInfo(Properties properties)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    LOGGER.debug("public String getClientInfo(String name = {})", name);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    LOGGER.debug("public Properties getClientInfo()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    LOGGER.debug("public Array createArrayOf(String typeName, Object[] elements)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    LOGGER.debug("public Struct createStruct(String typeName, Object[] attributes)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    LOGGER.debug("public void setSchema(String schema = {})", schema);
    session.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    LOGGER.debug("public String getSchema()");
    return session.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    LOGGER.debug("public void abort(Executor executor)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    LOGGER.debug("public void setNetworkTimeout(Executor executor, int milliseconds)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    LOGGER.debug("public int getNetworkTimeout()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    throw new UnsupportedOperationException("Not implemented");
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

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    if (this.isClosed()) {
      throw new DatabricksSQLException("Connection closed!");
    }
  }
}
