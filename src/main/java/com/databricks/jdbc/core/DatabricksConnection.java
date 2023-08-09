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

/**
 * Implementation for Databricks specific connection.
 */
public class DatabricksConnection implements IDatabricksConnection, Connection {

  private final IDatabricksSession session;
  private final Set<IDatabricksStatement> statementSet = ConcurrentHashMap.newKeySet();

  /**
   * Creates an instance of Databricks connection for given connection context.
   * @param connectionContext underlying connection context
   */
  public DatabricksConnection(IDatabricksConnectionContext connectionContext) {
    this.session = new DatabricksSession(connectionContext);
    this.session.open();
  }

  @VisibleForTesting
  DatabricksConnection(IDatabricksConnectionContext connectionContext, DatabricksClient databricksClient) {
    this.session = new DatabricksSession(connectionContext, databricksClient);
    this.session.open();
  }
  @Override
  public IDatabricksSession getSession() {
    return session;
  }

  @Override
  public Statement createStatement() throws SQLException {
    DatabricksStatement statement = new DatabricksStatement(this);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void commit() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void rollback() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws SQLException {
    for (IDatabricksStatement statement : statementSet) {
      statement.close(false);
      statementSet.remove(statement);
    }

    this.session.close();
  }

  @Override
  public boolean isClosed() throws SQLException {
    return session == null || !session.isOpen();
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    return new DatabricksDatabaseMetadata(this);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getCatalog() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearWarnings() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getHoldability() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Clob createClob() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Blob createBlob() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public NClob createNClob() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public String getSchema() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void closeStatement(IDatabricksStatement statement) {
    this.statementSet.remove(statement);
  }

  @Override
  public Connection getConnection() {
    return this;
  }
}
