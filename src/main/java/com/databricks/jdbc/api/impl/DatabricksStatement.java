package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static java.lang.Runtime.getRuntime;
import static java.lang.String.format;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.batch.DatabricksBatchExecutor;
import com.databricks.jdbc.common.ErrorCodes;
import com.databricks.jdbc.common.ErrorTypes;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.exception.DatabricksSQLFeatureNotSupportedException;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.http.entity.InputStreamEntity;

public class DatabricksStatement implements IDatabricksStatement, Statement {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksStatement.class);
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
  private final DatabricksBatchExecutor databricksBatchExecutor;

  public DatabricksStatement(DatabricksConnection connection) {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = false;
    this.timeoutInSeconds = DEFAULT_STATEMENT_TIMEOUT_SECONDS;
    this.databricksBatchExecutor =
        new DatabricksBatchExecutor(this, connection.getConnectionContext().getMaxBatchSize());
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
    ResultSet rs = executeInternal(sql, new HashMap<>(), StatementType.QUERY);
    if (!shouldReturnResultSet(sql)) {
      String errorMessage =
          "A ResultSet was expected but not generated from query: "
              + sql
              + ". However, query "
              + "execution was successful.";
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.EXECUTE_STATEMENT,
          statementId,
          ErrorCodes.RESULT_SET_ERROR);
      throw new DatabricksSQLException(errorMessage, ErrorCodes.RESULT_SET_ERROR);
    }
    return rs;
  }

  @Override
  public int executeUpdate(String sql) throws SQLException {
    checkIfClosed();
    executeInternal(sql, new HashMap<>(), StatementType.UPDATE);
    return (int) resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    LOGGER.debug("public void close()");
    close(true);
  }

  @Override
  public void close(boolean removeFromSession) throws DatabricksSQLException {
    LOGGER.debug("public void close(boolean removeFromSession)");
    this.isClosed = true;
    if (statementId != null) {
      this.connection.getSession().getDatabricksClient().closeStatement(statementId);
      if (resultSet != null) {
        this.resultSet.close();
        this.resultSet = null;
      }
    } else {
      warnings =
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
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.MAX_FIELD_SIZE_EXCEEDED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMaxFieldSize()");
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LOGGER.debug(String.format("public void setMaxFieldSize(int max = {%s})", max));
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.MAX_FIELD_SIZE_EXCEEDED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setMaxFieldSize(int max)");
  }

  @Override
  public int getMaxRows() throws SQLException {
    LOGGER.debug("public int getMaxRows()");
    checkIfClosed();
    return maxRows;
  }

  @Override
  public void setMaxRows(int max) throws SQLException {
    LOGGER.debug(String.format("public void setMaxRows(int max = {%s})", max));
    checkIfClosed();
    ValidationUtil.checkIfNonNegative(max, "maxRows");
    this.maxRows = max;
  }

  @Override
  public void setEscapeProcessing(boolean enable) throws SQLException {
    LOGGER.debug(String.format("public void setEscapeProcessing(boolean enable = {%s})", enable));
    this.escapeProcessing = enable;
  }

  @Override
  public int getQueryTimeout() throws SQLException {
    LOGGER.debug("public int getQueryTimeout()");
    checkIfClosed();
    return timeoutInSeconds;
  }

  @Override
  public void setQueryTimeout(int seconds) throws SQLException {
    LOGGER.debug(String.format("public void setQueryTimeout(int seconds = {%s})", seconds));
    checkIfClosed();
    ValidationUtil.checkIfNonNegative(seconds, "queryTimeout");
    this.timeoutInSeconds = seconds;
  }

  @Override
  public void cancel() throws SQLException {
    LOGGER.debug("public void cancel()");
    checkIfClosed();

    if (statementId != null) {
      this.connection.getSession().getDatabricksClient().cancelStatement(statementId);
    } else {
      warnings =
          WarningUtil.addWarning(
              warnings, "The statement you are trying to cancel does not have an ID yet.");
    }
  }

  @Override
  public SQLWarning getWarnings() {
    LOGGER.debug("public SQLWarning getWarnings()");
    return warnings;
  }

  @Override
  public void clearWarnings() {
    LOGGER.debug("public void clearWarnings()");
    warnings = null;
  }

  @Override
  public void setCursorName(String name) throws SQLException {
    LOGGER.debug(String.format("public void setCursorName(String name = {%s})", name));
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.CURSOR_NAME_NOT_FOUND);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setCursorName(String name)");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkIfClosed();
    resultSet = executeInternal(sql, new HashMap<>(), StatementType.SQL);
    return shouldReturnResultSet(sql);
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
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.MORE_RESULTS_UNSUPPORTED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - getMoreResults()");
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LOGGER.debug(String.format("public void setFetchDirection(int direction = {%s})", direction));
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.FEATURE_NOT_SUPPORTED,
          statementId,
          ErrorCodes.UNSUPPORTED_FETCH_FORWARD);
      throw new DatabricksSQLFeatureNotSupportedException("Not supported: ResultSet.FetchForward");
    }
  }

  @Override
  public int getFetchDirection() throws SQLException {
    LOGGER.debug("public int getFetchDirection()");
    checkIfClosed();
    return ResultSet.FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int rows) {
    /* As we fetch chunks of data together,
    setting fetchSize is an overkill.
    Hence, we don't support it.*/
    LOGGER.debug(String.format("public void setFetchSize(int rows = {%s})", rows));
    String warningString = "As FetchSize is not supported in the Databricks JDBC, ignoring it";

    LOGGER.warn(warningString);
    warnings = WarningUtil.addWarning(warnings, warningString);
  }

  @Override
  public int getFetchSize() {
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

  /** {@inheritDoc} */
  @Override
  public void addBatch(String sql) throws SQLException {
    LOGGER.debug(String.format("public void addBatch(String sql = {%s})", sql));
    checkIfClosed();
    databricksBatchExecutor.addCommand(sql);
  }

  /** {@inheritDoc} */
  @Override
  public void clearBatch() throws SQLException {
    LOGGER.debug("public void clearBatch()");
    checkIfClosed();
    databricksBatchExecutor.clearCommands();
  }

  /** {@inheritDoc} */
  @Override
  public int[] executeBatch() throws SQLException {
    LOGGER.debug("public int[] executeBatch()");
    checkIfClosed();
    return databricksBatchExecutor.executeBatch();
  }

  @Override
  public Connection getConnection() throws SQLException {
    LOGGER.debug("public Connection getConnection()");
    return connection;
  }

  @Override
  public boolean getMoreResults(int current) throws SQLException {
    LOGGER.debug(String.format("public boolean getMoreResults(int current = {%s})", current));
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.MORE_RESULTS_UNSUPPORTED);
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
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.FEATURE_NOT_SUPPORTED,
          statementId,
          ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported: executeUpdate(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeUpdate(String sql, int[] columnIndexes)");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    LOGGER.debug("public int executeUpdate(String sql, String[] columnNames)");
    checkIfClosed();
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeUpdate(String sql, String[] columnNames)");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.FEATURE_NOT_SUPPORTED,
          statementId,
          ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported: execute(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: execute(String sql, int[] columnIndexes)");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
    MetricsUtil.exportError(
        connection.getSession(),
        ErrorTypes.FEATURE_NOT_SUPPORTED,
        statementId,
        ErrorCodes.EXECUTE_METHOD_UNSUPPORTED);
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: execute(String sql, String[] columnNames)");
  }

  @Override
  public int getResultSetHoldability() {
    LOGGER.debug("public int getResultSetHoldability()");
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public boolean isClosed() throws SQLException {
    LOGGER.debug("public boolean isClosed()");
    return isClosed;
  }

  @Override
  public void setPoolable(boolean poolable) throws SQLException {
    LOGGER.debug(String.format("public void setPoolable(boolean poolable = {%s})", poolable));
    checkIfClosed();
    if (poolable) {
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.FEATURE_NOT_SUPPORTED,
          statementId,
          ErrorCodes.POOLABLE_METHOD_UNSUPPORTED);
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported: setPoolable(boolean poolable)");
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
    return closeOnCompletion;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    if (iface.isInstance(this)) {
      return (T) this;
    }
    throw new DatabricksSQLException(
        String.format(
            "Class {%s} cannot be wrapped from {%s}", getClass().getName(), iface.getName()));
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    LOGGER.debug("public boolean isWrapperFor(Class<?> iface)");
    return iface.isInstance(this);
  }

  @Override
  public void handleResultSetClose(IDatabricksResultSet resultSet) throws DatabricksSQLException {
    // Don't throw exception, we are already closing here
    if (closeOnCompletion) {
      close(true);
    }
  }

  @Override
  public void setStatementId(String statementId) {
    this.statementId = statementId;
  }

  @Override
  public String getStatementId() {
    return statementId;
  }

  @Override
  public Statement getStatement() {
    return this;
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
    return allowInputStreamForUCVolume;
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

  @VisibleForTesting
  static boolean shouldReturnResultSet(String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new IllegalArgumentException("Query cannot be null or empty");
    }

    // Trim and remove comments and whitespaces.
    String trimmedQuery = query.trim().replaceAll("(?m)--.*$", "");
    trimmedQuery = trimmedQuery.replaceAll("/\\*.*?\\*/", "");
    trimmedQuery = trimmedQuery.replaceAll("\\s+", " ").trim();

    // Check if the query matches any of the patterns that return a ResultSet
    return SELECT_PATTERN.matcher(trimmedQuery).find()
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
        || LIST_PATTERN.matcher(trimmedQuery).find();

    // Otherwise, it should not return a ResultSet
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
    LOGGER.debug(stackTraceMessage);
    CompletableFuture<DatabricksResultSet> futureResultSet =
        getFutureResult(sql, params, statementType);
    try {
      resultSet =
          timeoutInSeconds == 0
              ? futureResultSet.get() // Wait indefinitely when timeout is 0
              : futureResultSet.get(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (TimeoutException e) {
      if (closeStatement) {
        close(); // Close the statement
      }
      String timeoutErrorMessage =
          String.format(
              "Statement execution timed-out. ErrorMessage %s, statementId %s",
              stackTraceMessage, statementId);
      LOGGER.error(timeoutErrorMessage);
      futureResultSet.cancel(true); // Cancel execution run
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.TIMEOUT_ERROR,
          statementId,
          ErrorCodes.STATEMENT_EXECUTION_TIMEOUT);
      throw new DatabricksTimeoutException(timeoutErrorMessage, e);
    } catch (InterruptedException | ExecutionException e) {
      Throwable cause = e;
      // Look for underlying DatabricksSQL exception
      while (cause.getCause() != null) {
        cause = cause.getCause();
        if (cause instanceof DatabricksSQLException) {
          throw (DatabricksSQLException) cause;
        }
      }
      String errMsg =
          String.format(
              "Error occurred during statement execution: %s. Error : %s", sql, e.getMessage());
      LOGGER.error(errMsg, e);
      MetricsUtil.exportError(
          connection.getSession(),
          ErrorTypes.EXECUTE_STATEMENT,
          "",
          ErrorCodes.EXECUTE_STATEMENT_FAILED);
      throw new DatabricksSQLException(errMsg, e, "", ErrorCodes.EXECUTE_STATEMENT_FAILED);
    }
    LOGGER.debug("Result retrieved successfully" + resultSet.toString());
    return resultSet;
  }

  DatabricksResultSet executeInternal(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    return executeInternal(sql, params, statementType, true);
  }

  CompletableFuture<DatabricksResultSet> getFutureResult(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType) {
    int poolSize = getRuntime().availableProcessors() * 2;
    ExecutorService executor = Executors.newFixedThreadPool(poolSize);
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String SQLString = escapeProcessing ? StringUtil.convertJdbcEscapeSequences(sql) : sql;
            return getResultFromClient(SQLString, params, statementType);
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        },
        executor);
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
}
