package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static java.lang.String.format;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.http.entity.InputStreamEntity;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private int timeoutInSeconds;
  private final DatabricksConnection connection;
  DatabricksResultSet resultSet;
  private String statementId;
  private boolean isClosed;
  private boolean closeOnCompletion;
  private SQLWarning warnings = null;
  private int maxRows = DEFAULT_ROW_LIMIT;
  private boolean escapeProcessing = DEFAULT_ESCAPE_PROCESSING;
  private InputStreamEntity inputStream = null;
  private boolean allowInputStreamForUCVolume = false;

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
    // TODO(PECO-1731): Revisit this to see if we can fail fast if the statement does not return a
    // result set.
    checkIfClosed();
    ResultSet rs =
        executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.QUERY);
    if (!shouldReturnResultSet(sql)) {
      String errorMessage =
          "A ResultSet was expected but not generated from query: "
              + sql
              + ". However, query "
              + "execution was successful.";
      throw new DatabricksSQLException(
          errorMessage,
          connection.getSession().getConnectionContext(),
          ErrorTypes.EXECUTE_STATEMENT,
          statementId,
          ErrorCodes.RESULT_SET_ERROR);
    }
    return rs;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkIfClosed();
    executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void close()");
    close(true);
  }

  @Override
  public void close(boolean removeFromSession) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void close(boolean removeFromSession)");
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
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxFieldSize()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMaxFieldSize()",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.MAX_FIELD_SIZE_EXCEEDED);
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setMaxFieldSize(int max = {%s})", max));
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setMaxFieldSize(int max)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.MAX_FIELD_SIZE_EXCEEDED);
  }

  @Override
  public int getMaxRows() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getMaxRows()");
    checkIfClosed();
    return this.maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, String.format("public void setMaxRows(int max = {%s})", max));
    checkIfClosed();
    ValidationUtil.checkIfNonNegative(max, "maxRows");
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public void setEscapeProcessing(boolean enable = {%s})", enable));
    this.escapeProcessing = enable;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getQueryTimeout()");
    checkIfClosed();
    return this.timeoutInSeconds;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setQueryTimeout(int seconds = {%s})", seconds));
    checkIfClosed();
    ValidationUtil.checkIfNonNegative(seconds, "queryTimeout");
    this.timeoutInSeconds = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void cancel()");
    checkIfClosed();

    if (statementId != null) {
      this.connection.getSession().getDatabricksClient().cancelStatement(statementId);
    } else {
      WarningUtil.addWarning(
          warnings, "The statement you are trying to cancel does not have an ID yet.");
    }
  }

  @Override
  public SQLWarning getWarnings() {
    LoggingUtil.log(LogLevel.DEBUG, "public SQLWarning getWarnings()");
    return warnings;
  }

  @Override
  public void clearWarnings() {
    LoggingUtil.log(LogLevel.DEBUG, "public void clearWarnings()");
    warnings = null;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setCursorName(String name = {%s})", name));
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setCursorName(String name)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.CURSOR_NAME_NOT_FOUND);
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkIfClosed();
    resultSet =
        executeInternal(sql, new HashMap<Integer, ImmutableSqlParameter>(), StatementType.SQL);
    return shouldReturnResultSet(sql);
  }

  @Override
  public ResultSet getResultSet() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getResultSet()");
    checkIfClosed();
    return resultSet;
  }

  @Override
  public int getUpdateCount() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getUpdateCount()");
    checkIfClosed();
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean getMoreResults()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMoreResults()",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.MORE_RESULTS_UNSUPPORTED);
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public void setFetchDirection(int direction = {%s})", direction));
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Not supported",
          connection.getSession().getConnectionContext(),
          statementId,
          ErrorCodes.UNSUPPORTED_FETCH_FORWARD);
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getFetchDirection()");
    checkIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) {
    /* As we fetch chunks of data together,
    setting fetchSize is an overkill.
    Hence, we don't support it.*/
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setFetchSize(int rows = {%s})", rows));
    String warningString = "As FetchSize is not supported in the Databricks JDBC, ignoring it";

    LoggingUtil.log(LogLevel.WARN, warningString);
    warnings = WarningUtil.addWarning(warnings, warningString);
  }

  @Override
  public int getFetchSize() {
    LoggingUtil.log(LogLevel.DEBUG, "public int getFetchSize()");
    String warningString =
        "As FetchSize is not supported in the Databricks JDBC, we don't set it in the first place";

    LoggingUtil.log(LogLevel.WARN, warningString);
    warnings = WarningUtil.addWarning(warnings, warningString);
    return 0;
  }

  @Override
  public int getResultSetConcurrency() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getResultSetConcurrency()");
    checkIfClosed();
    return ResultSet.CONCUR_READ_ONLY;
  }

  @Override
  public int getResultSetType() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int getResultSetType()");
    checkIfClosed();
    return ResultSet.TYPE_FORWARD_ONLY;
  }

  @Override
  public void addBatch(String sql) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, String.format("public void addBatch(String sql = {%s})", sql));
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "addBatch(String sql)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.BATCH_OPERATION_UNSUPPORTED);
  }

  @Override
  public void clearBatch() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void clearBatch()");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "clearBatch()",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.BATCH_OPERATION_UNSUPPORTED);
  }

  @Override
  public int[] executeBatch() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int[] executeBatch()");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "executeBatch()",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.BATCH_OPERATION_UNSUPPORTED);
  }

  @Override
  public Connection getConnection() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public Connection getConnection()");
    return this.connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public boolean getMoreResults(int current = {%s})", current));
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMoreResults(int current)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.MORE_RESULTS_UNSUPPORTED);
  }

  @Override
  public ResultSet getGeneratedKeys() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public ResultSet getGeneratedKeys()");
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
          "Method not supported",
          "executeUpdate(String sql, int autoGeneratedKeys)",
          connection.getSession().getConnectionContext(),
          statementId,
          ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "executeUpdate(String sql, int[] columnIndexes)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public int executeUpdate(String sql, String[] columnNames)");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "executeUpdate(String sql, String[] columnNames)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported",
          "execute(String sql, int autoGeneratedKeys)",
          connection.getSession().getConnectionContext(),
          statementId,
          ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "execute(String sql, int[] columnIndexes)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported",
        "execute(String sql, String[] columnNames)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
  }

  @Override
  public int getResultSetHoldability() {
    LoggingUtil.log(LogLevel.DEBUG, "public int getResultSetHoldability()");
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isClosed()");
    return this.isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format("public void setPoolable(boolean poolable = {%s})", poolable));
    checkIfClosed();
    if (poolable) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported",
          "setPoolable(boolean poolable)",
          connection.getSession().getConnectionContext(),
          statementId,
          ErrorCodes.POOLABLE_METHOD_UNSUPPORTED);
    }
  }

  @Override
  public boolean isPoolable() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isPoolable()");
    checkIfClosed();
    return false;
  }

  @Override
  public void closeOnCompletion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void closeOnCompletion()");
    checkIfClosed();
    this.closeOnCompletion = true;
  }

  @Override
  public boolean isCloseOnCompletion() throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isCloseOnCompletion()");
    checkIfClosed();
    return this.closeOnCompletion;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public <T> T unwrap(Class<T> iface)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - unwrap(Class<T> iface)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.STATEMENT_UNWRAP_UNSUPPORTED);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isWrapperFor(Class<?> iface)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - isWrapperFor(Class<?> iface)",
        connection.getSession().getConnectionContext(),
        statementId,
        ErrorCodes.STATEMENT_UNWRAP_UNSUPPORTED);
  }

  @Override
  public void handleResultSetClose(IDatabricksResultSet resultSet) throws SQLException {
    // Don't throw exception, we are already closing here
    if (closeOnCompletion) {
      this.close(true);
    }
  }

  DatabricksResultSet executeInternal(
      String sql,
      Map<Integer, ImmutableSqlParameter> params,
      StatementType statementType,
      boolean closeStatement)
      throws SQLException {
    String stackTraceMessage =
        format(
            "DatabricksResultSet executeInternal(String sql = %s,Map<Integer, ImmutableSqlParameter> params = {%s}, StatementType statementType = {%s})",
            sql, params, statementType);
    LoggingUtil.log(LogLevel.DEBUG, stackTraceMessage);
    CompletableFuture<DatabricksResultSet> futureResultSet =
        getFutureResult(sql, params, statementType);
    try {
      resultSet =
          timeoutInSeconds == 0
              ? futureResultSet.get() // Wait indefinitely when timeout is 0
              : futureResultSet.get(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      if (closeStatement) {
        this.close(); // Close the statement
      }
      futureResultSet.cancel(true); // Cancel execution run
      throw new DatabricksTimeoutException(
          "Statement execution timed-out. " + stackTraceMessage,
          e,
          connection.getSession().getConnectionContext(),
          statementId,
          ErrorCodes.STATEMENT_EXECUTION_TIMEOUT);
    } catch (InterruptedException | ExecutionException e) {
      Throwable cause = e;
      // Look for underlying DatabricksSQL exception
      while (cause.getCause() != null) {
        cause = cause.getCause();
        if (cause instanceof DatabricksSQLException) {
          throw (DatabricksSQLException) cause;
        }
      }
      LoggingUtil.log(
          LogLevel.ERROR,
          String.format("Error occurred during statement execution: %s. Error : %s", sql, e));
      throw new DatabricksSQLException(
          "Error occurred during statement execution: " + sql,
          e,
          connection.getSession().getConnectionContext(),
          ErrorTypes.EXECUTE_STATEMENT,
          statementId,
          ErrorCodes.EXECUTE_STATEMENT_FAILED);
    }
    LoggingUtil.log(LogLevel.DEBUG, "Result retrieved successfully" + resultSet.toString());
    return resultSet;
  }

  DatabricksResultSet executeInternal(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    return executeInternal(sql, params, statementType, true);
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
    IDatabricksClient client = connection.getSession().getDatabricksClient();
    return client.executeStatement(
        sql,
        connection.getSession().getComputeResource(),
        params,
        statementType,
        connection.getSession(),
        this);
  }

  void checkIfClosed() throws DatabricksSQLException {
    if (isClosed) {
      throw new DatabricksSQLException("Statement is closed", ErrorCodes.STATEMENT_CLOSED);
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

  @VisibleForTesting
  protected static boolean shouldReturnResultSet(String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new IllegalArgumentException("Query cannot be null or empty");
    }

    // Trim and remove comments and whitespaces.
    String trimmedQuery = query.trim().replaceAll("(?m)--.*$", "");
    trimmedQuery = trimmedQuery.replaceAll("/\\*.*?\\*/", "");
    trimmedQuery = trimmedQuery.replaceAll("\\s+", " ").trim();

    // Check if the query matches any of the patterns that return a ResultSet
    if (SELECT_PATTERN.matcher(trimmedQuery).find()
        || SHOW_PATTERN.matcher(trimmedQuery).find()
        || DESCRIBE_PATTERN.matcher(trimmedQuery).find()
        || EXPLAIN_PATTERN.matcher(trimmedQuery).find()
        || WITH_PATTERN.matcher(trimmedQuery).find()
        || SET_PATTERN.matcher(trimmedQuery).find()
        || MAP_PATTERN.matcher(trimmedQuery).find()
        || FROM_PATTERN.matcher(trimmedQuery).find()
        || VALUES_PATTERN.matcher(trimmedQuery).find()
        || UNION_PATTERN.matcher(trimmedQuery).find()
        || INTERSECT_PATTERN.matcher(trimmedQuery).find()
        || EXCEPT_PATTERN.matcher(trimmedQuery).find()
        || DECLARE_PATTERN.matcher(trimmedQuery).find()
        || PUT_PATTERN.matcher(trimmedQuery).find()
        || GET_PATTERN.matcher(trimmedQuery).find()
        || REMOVE_PATTERN.matcher(trimmedQuery).find()
        || LIST_PATTERN.matcher(trimmedQuery).find()) {
      return true;
    }

    // Otherwise, it should not return a ResultSet
    return false;
  }

  @Override
  public void allowInputStreamForVolumeOperation(boolean allowInputStream)
      throws DatabricksSQLException {
    checkIfClosed();
    this.allowInputStreamForUCVolume = allowInputStream;
  }

  @Override
  public boolean isAllowedInputStreamForVolumeOperation() throws DatabricksSQLException {
    checkIfClosed();
    return this.allowInputStreamForUCVolume;
  }

  @Override
  public void setInputStreamForUCVolume(InputStreamEntity inputStream)
      throws DatabricksSQLException {
    if (isAllowedInputStreamForVolumeOperation()) {
      this.inputStream = inputStream;
    } else {
      throw new DatabricksSQLException("Volume operation not supported for Input Stream");
    }
  }

  @Override
  public InputStreamEntity getInputStreamForUCVolume() throws DatabricksSQLException {
    if (isAllowedInputStreamForVolumeOperation()) {
      return inputStream;
    }
    return null;
  }
}
