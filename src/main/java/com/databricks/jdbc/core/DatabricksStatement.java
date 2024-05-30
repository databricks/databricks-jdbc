package com.databricks.jdbc.core;

import static com.databricks.jdbc.commons.EnvironmentVariables.*;
import static java.lang.String.format;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.StatementType;
import com.databricks.jdbc.commons.util.StringUtil;
import com.databricks.jdbc.commons.util.ValidationUtil;
import com.databricks.jdbc.commons.util.WarningUtil;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabricksStatement implements IDatabricksStatement, Statement {
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksStatement.class);

  private int timeoutInSeconds;
  private final DatabricksConnection connection;
  DatabricksResultSet resultSet;
  private String statementId;
  private boolean isClosed;
  private boolean closeOnCompletion;
  private SQLWarning warnings = null;
  private int maxRows = DEFAULT_ROW_LIMIT;
  private boolean escapeProcessing = DEFAULT_ESCAPE_PROCESSING;

  public DatabricksStatement(DatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = false;
    this.timeoutInSeconds = DEFAULT_STATEMENT_TIMEOUT_SECONDS;
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
    if (statementId != null) {
      this.connection.getSession().getDatabricksClient().closeStatement(statementId);
      if (resultSet != null) {
        this.resultSet.close();
        this.resultSet = null;
      }
    } else {
      WarningUtil.addWarning(
          warnings, "The statement you are trying to close does not have an ID yet.");
      return;
    }
    if (removeFromSession) {
      this.connection.closeStatement(this);
    }
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    LOGGER.debug("public int getMaxFieldSize()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMaxFieldSize()");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LOGGER.debug("public void setMaxFieldSize(int max = {})", max);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setMaxFieldSize(int max)");
  }

  @Override
  public int getMaxRows() throws SQLException {
    LOGGER.debug("public int getMaxRows()");
    checkIfClosed();
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    LOGGER.debug("public void setMaxRows(int max = {})", max);
    checkIfClosed();
    ValidationUtil.checkIfPositive(max, "maxRows");
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    LOGGER.debug("public void setEscapeProcessing(boolean enable = {})", enable);
    this.escapeProcessing = enable;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    LOGGER.debug("public int getQueryTimeout()");
    checkIfClosed();
    return this.timeoutInSeconds;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    LOGGER.debug("public void setQueryTimeout(int seconds = {})", seconds);
    checkIfClosed();
    this.timeoutInSeconds = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    LOGGER.debug("public void cancel()");
    checkIfClosed();

    if (statementId != null) {
      this.connection.getSession().getDatabricksClient().cancelStatement(statementId);
    } else {
      WarningUtil.addWarning(
          warnings, "The statement you are trying to cancel does not have an ID yet.");
    }
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    LOGGER.debug("public SQLWarning getWarnings()");
    return warnings;
  }

  @Override
  public void clearWarnings() throws SQLException {
    LOGGER.debug("public void clearWarnings()");
    warnings = null;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    LOGGER.debug("public void setCursorName(String name = {})", name);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setCursorName(String name)");
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMoreResults()");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LOGGER.debug("public void setFetchDirection(int direction = {})", direction);
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new DatabricksSQLFeatureNotSupportedException("Not supported");
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
    /* As we fetch chunks of data together,
    setting fetchSize is an overkill.
    Hence, we don't support it.*/
    LOGGER.debug("public void setFetchSize(int rows = {})", rows);
    String warningString = "As FetchSize is not supported in the Databricks JDBC, ignoring it";
    LOGGER.warn(warningString);
    warnings = WarningUtil.addWarning(warnings, warningString);
  }

  @Override
  public int getFetchSize() throws SQLException {
    LOGGER.debug("public int getFetchSize()");
    String warningString =
        "As FetchSize is not supported in the Databricks JDBC, we don't set it in the first place";
    LOGGER.warn(warningString);
    warnings = WarningUtil.addWarning(warnings, warningString);
    return 0;
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMoreResults(int current)");
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - unwrap(Class<T> iface)");
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - isWrapperFor(Class<?> iface)");
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
    String stackTraceMessage =
        format(
            "DatabricksResultSet executeInternal(String sql = %s,Map<Integer, ImmutableSqlParameter> params = {%s}, StatementType statementType = {%s})",
            sql, params.toString(), statementType.toString());
    LOGGER.debug(stackTraceMessage);
    CompletableFuture<DatabricksResultSet> futureResultSet =
        getFutureResult(sql, params, statementType);
    try {
      resultSet = futureResultSet.get(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      this.close(); // Close the statement
      futureResultSet.cancel(true); // Cancel execution run
      throw new DatabricksTimeoutException(
          "Statement execution timed-out. " + stackTraceMessage, e);
    } catch (InterruptedException | ExecutionException e) {
      LOGGER.error("Error occurred during statement execution: " + sql, e);
      throw new DatabricksSQLException("Error occurred during statement execution: " + sql, e);
    }
    LOGGER.debug("Result retrieved successfully" + resultSet.toString());
    return resultSet;
  }

  // Todo : Add timeout tests in the subsequent PR
  CompletableFuture<DatabricksResultSet> getFutureResult(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String SQLString = escapeProcessing ? StringUtil.getProcessedEscapeSequence(sql) : sql;
            return getResultFromClient(SQLString, params, statementType);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
  }

  DatabricksResultSet getResultFromClient(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    DatabricksClient client = connection.getSession().getDatabricksClient();
    return client.executeStatement(
        sql,
        connection.getSession().getComputeResource(),
        params,
        statementType,
        connection.getSession(),
        this);
  }

  void checkIfClosed() throws SQLException {
    if (isClosed) {
      throw new DatabricksSQLException("Statement is closed");
    }
  }

  @Override
  public void setStatementId(String statementId) {
    this.statementId = statementId;
  }

  @Override
  public String getStatementId() {
    return this.statementId;
  }

  @Override
  public Statement getStatement() {
    return this;
  }
}
