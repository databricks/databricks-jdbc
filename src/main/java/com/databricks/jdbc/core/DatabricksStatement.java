package com.databricks.jdbc.core;

import com.databricks.jdbc.client.StatementType;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksStatement.class);
  private final DatabricksConnection connection;
  DatabricksResultSet resultSet;
  private String statementId;
  private boolean isClosed;
  private boolean closeOnCompletion;

  public DatabricksStatement(DatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = false;
  }

  @Override
  public String getSessionId() {
    return connection.getSession().getSessionId();
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    checkIfClosed();
    return executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.QUERY);
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkIfClosed();
    executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    LOGGER.debug("public void close()");
    close(true);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    LOGGER.debug("public void close(boolean removeFromSession)");
    this.isClosed = true;
    this.connection.getSession().getDatabricksClient().closeStatement(statementId);
    if (resultSet != null) {
      this.resultSet.close();
      this.resultSet = null;
    }
    if (removeFromSession) {
      this.connection.closeStatement(this);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    LOGGER.debug("public int getMaxFieldSize()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LOGGER.debug("public void setMaxFieldSize(int max = {})", max);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getMaxRows() throws SQLException {
    LOGGER.debug("public int getMaxRows()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    LOGGER.debug("public void setMaxRows(int max = {})", max);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    LOGGER.debug("public void setEscapeProcessing(boolean enable = {})", enable);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    LOGGER.debug("public int getQueryTimeout()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    LOGGER.debug("public void setQueryTimeout(int seconds = {})", seconds);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void cancel() throws SQLException {
    LOGGER.debug("public void cancel()");
    throw new UnsupportedOperationException("Not implemented");
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
  public void setCursorName(String name) throws SQLException {
    LOGGER.debug("public void setCursorName(String name = {})", name);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkIfClosed();
    resultSet =
        executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.SQL);
    return !resultSet.hasUpdateCount();
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    LOGGER.debug("public ResultSet getResultSet()");
    checkIfClosed();
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    LOGGER.debug("public int getUpdateCount()");
    checkIfClosed();
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    LOGGER.debug("public boolean getMoreResults()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LOGGER.debug("public void setFetchDirection(int direction = {})", direction);
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new SQLFeatureNotSupportedException("Not supported");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    LOGGER.debug("public int getFetchDirection()");
    checkIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) throws SQLException {
    LOGGER.debug("public void setFetchSize(int rows = {})", rows);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getFetchSize() throws SQLException {
    LOGGER.debug("public int getFetchSize()");
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    LOGGER.debug("public int getResultSetConcurrency()");
    checkIfClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    LOGGER.debug("public int getResultSetType()");
    checkIfClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    LOGGER.debug("public void addBatch(String sql = {})", sql);
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "addBatch(String sql)");
  }

  @Override
  public void clearBatch() throws SQLException {
    LOGGER.debug("public void clearBatch()");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException("Method not supported", "clearBatch()");
  }

  @Override
  public int[] executeBatch() throws SQLException {
    LOGGER.debug("public int[] executeBatch()");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException("Method not supported", "executeBatch()");
  }

  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    LOGGER.debug("public boolean getMoreResults(int current = {})", current);
    throw new UnsupportedOperationException("Not implemented");
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    LOGGER.debug("public ResultSet getGeneratedKeys()");
    checkIfClosed();
    return new EmptyResultSet();
  }

  @Override
  public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeUpdate(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "executeUpdate(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "executeUpdate(String sql, int[] columnIndexes)");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    LOGGER.debug("public int executeUpdate(String sql, String[] columnNames)");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "executeUpdate(String sql, String[] columnNames)");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "execute(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "execute(String sql, int[] columnIndexes)");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported", "execute(String sql, String[] columnNames)");
  }

  @Override
  public int getResultSetHoldability() throws SQLException {
    LOGGER.debug("public int getResultSetHoldability()");
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    LOGGER.debug("public boolean isClosed()");
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    LOGGER.debug("public void setPoolable(boolean poolable = {})", poolable);
    checkIfClosed();
    if (poolable) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported", "setPoolable(boolean poolable)");
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    LOGGER.debug("public boolean isPoolable()");
    checkIfClosed();
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    LOGGER.debug("public void closeOnCompletion()");
    checkIfClosed();
    this.closeOnCompletion = true;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    LOGGER.debug("public boolean isCloseOnCompletion()");
    checkIfClosed();
    return this.closeOnCompletion;
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
  public void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException {
    // Don't throw exception, we are already closing here
    if (closeOnCompletion) {
      this.close(true);
    }
  }

  DatabricksResultSet executeInternal(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    LOGGER.debug(
        "DatabricksResultSet executeInternal(String sql = {}, Map<Integer, ImmutableSqlParameter> params = {}, StatementType statementType = {})",
        sql,
        params,
        statementType);
    resultSet =
        connection
            .getSession()
            .getDatabricksClient()
            .executeStatement(
                sql,
                connection.getSession().getWarehouseId(),
                params,
                statementType,
                connection.getSession(),
                this);
    this.isClosed = false;
    return resultSet;
  }

  void checkIfClosed() throws SQLException {
    if (isClosed) {
      throw new DatabricksSQLException("Statement is closed");
    }
  }
}
