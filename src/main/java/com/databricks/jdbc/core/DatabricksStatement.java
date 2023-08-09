package com.databricks.jdbc.core;


import java.sql.*;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private final IDatabricksConnection connection;
  private DatabricksResultSet resultSet;
  private String statementId;

  public DatabricksStatement(IDatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
  }

  @Override
  public String getSessionId() {
    return connection.getSession().getSessionId();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    return connection.getSession().getDatabricksClient().executeStatement(
        sql, getSessionId(), connection.getSession().getWarehouseId(), false);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void close() throws SQLException {
    this.connection.getSession().getDatabricksClient().closeStatement(statementId);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    close();
    if (removeFromSession) {
      this.connection.closeStatement(this);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getMaxRows() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void cancel() throws SQLException {
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
  public void setCursorName(String name) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getUpdateCount() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getFetchDirection() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetType() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void clearBatch() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public Connection getConnection() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isClosed() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isPoolable() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
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
}
