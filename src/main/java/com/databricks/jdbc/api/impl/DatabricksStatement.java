package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.*;
import static com.databricks.jdbc.common.EnvironmentVariables.*;
import static java.lang.String.format;

import com.databricks.jdbc.api.IDatabricksResultSet;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.impl.batch.DatabricksBatchExecutor;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.common.util.*;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.MoreExecutors;
import java.sql.*;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import org.apache.http.entity.InputStreamEntity;

public class DatabricksStatement implements IDatabricksStatement, IDatabricksStatementInternal {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksStatement.class);

  /** Same-Thread-ExecutorService for handling execution of statements. */
  private final ExecutorService executor = MoreExecutors.newDirectExecutorService();

  private int timeoutInSeconds;
  protected final DatabricksConnection connection;
  DatabricksResultSet resultSet;
  private StatementId statementId;
  private boolean isClosed;
  private boolean closeOnCompletion;
  private SQLWarning warnings = null;
  private long maxRows = DEFAULT_RESULT_ROW_LIMIT;
  private int maxFieldSize = 0;
  private boolean escapeProcessing = DEFAULT_ESCAPE_PROCESSING;
  private InputStreamEntity inputStream = null;
  private boolean allowInputStreamForUCVolume = false;
  private final DatabricksBatchExecutor databricksBatchExecutor;
  private boolean noMoreResults = false; // JDBC end-of-results indicator
  private long updateCount = -1; // Update count for DML statements, -1 for SELECT or no results

  public DatabricksStatement(DatabricksConnection connection) throws DatabricksValidationException {
    this.connection = connection;
    this.resultSet = null;
    this.statementId = null;
    this.isClosed = false;
    this.timeoutInSeconds = DEFAULT_STATEMENT_TIMEOUT_SECONDS;
    this.databricksBatchExecutor =
        new DatabricksBatchExecutor(this, connection.getConnectionContext().getMaxBatchSize());
  }

  public DatabricksStatement(DatabricksConnection connection, StatementId statementId)
      throws DatabricksValidationException {
    this.connection = connection;
    this.statementId = statementId;
    this.resultSet = null;
    this.isClosed = false;
    this.timeoutInSeconds = DEFAULT_STATEMENT_TIMEOUT_SECONDS;
    this.databricksBatchExecutor =
        new DatabricksBatchExecutor(this, connection.getConnectionContext().getMaxBatchSize());
  }

  @Override
  public ResultSet executeQuery(String sql) throws SQLException {
    // TODO (PECO-1731): Can it fail fast without executing SQL query?
    checkIfClosed();
    ResultSet rs = executeInternal(sql, new HashMap<>(), StatementType.QUERY);
    if (!shouldReturnResultSetWithConfig(sql)) {
      String errorMessage =
          "A ResultSet was expected but not generated from query. However, query "
              + "execution was successful.";
      throw new DatabricksSQLException(errorMessage, DatabricksDriverErrorCode.RESULT_SET_ERROR);
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
  public long executeLargeUpdate(String sql) throws SQLException {
    LOGGER.debug("public long executeLargeUpdate(String sql = {})", sql);
    checkIfClosed();
    executeInternal(sql, new HashMap<>(), StatementType.UPDATE);
    return resultSet.getUpdateCount();
  }

  @Override
  public void close() throws SQLException {
    LOGGER.debug("public void close()");
    close(true);
  }

  @Override
  public void close(boolean removeFromSession) throws DatabricksSQLException {
    LOGGER.debug("public void close(boolean removeFromSession)");
    if (isClosed) {
      if (resultSet != null) {
        this.resultSet.close();
        this.resultSet = null;
      }
    } else if (statementId == null) {
      String warningMsg = "The statement you are trying to close does not have an ID yet.";
      LOGGER.warn(warningMsg);
      warnings = WarningUtil.addWarning(warnings, warningMsg);
    } else {
      this.connection.getSession().getDatabricksClient().closeStatement(statementId);
      if (resultSet != null) {
        this.resultSet.close();
        this.resultSet = null;
      }

      if (removeFromSession) {
        this.connection.closeStatement(this);
      }
      DatabricksThreadContextHolder.clearStatementInfo();
    }

    shutDownExecutor();
    this.updateCount = -1; // Reset update count when statement is closed
    this.isClosed = true;
  }

  @Override
  public int getMaxFieldSize() throws SQLException {
    LOGGER.debug("public int getMaxFieldSize()");
    return maxFieldSize;
  }

  @Override
  public void setMaxFieldSize(int max) throws SQLException {
    LOGGER.debug(String.format("public void setMaxFieldSize(int max = {%s})", max));
    maxFieldSize = max;
  }

  @Override
  public int getMaxRows() throws DatabricksSQLException {
    LOGGER.debug("public int getMaxRows()");
    checkIfClosed();
    return (int) maxRows;
  }

  @Override
  public long getLargeMaxRows() throws DatabricksSQLException {
    LOGGER.debug("public void getLargeMaxRows()");
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
  public void setLargeMaxRows(long max) throws SQLException {
    LOGGER.debug("public void setLargeMaxRows(long max = {})", max);
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
      DatabricksThreadContextHolder.clearStatementInfo();
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
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not implemented in DatabricksStatement - setCursorName(String name)");
  }

  @Override
  public boolean execute(String sql) throws SQLException {
    checkIfClosed();
    resultSet = executeInternal(sql, new HashMap<>(), StatementType.SQL);
    return shouldReturnResultSetWithConfig(sql);
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
    // Return SUCCESS_NO_INFO if update count exceeds int range, per JDBC spec
    return updateCount > Integer.MAX_VALUE ? Statement.SUCCESS_NO_INFO : (int) updateCount;
  }

  @Override
  public long getLargeUpdateCount() throws SQLException {
    LOGGER.debug("public long getLargeUpdateCount()");
    checkIfClosed();
    return updateCount;
  }

  @Override
  public boolean getMoreResults() throws SQLException {
    LOGGER.debug("public boolean getMoreResults()");
    checkIfClosed();
    // We only produce a single result. Advancing means: go past the last result.
    if (!noMoreResults) {
      noMoreResults = true; // mark end-of-results so getUpdateCount() returns -1
      updateCount = -1; // Reset update count to -1 when no more results
      // Per JDBC, advancing implicitly closes current ResultSet
      if (resultSet != null) {
        try {
          resultSet.close();
        } catch (SQLException ignore) {
        }
      }
    }
    return false; // no next ResultSet in this driver
  }

  @Override
  public void setFetchDirection(int direction) throws SQLException {
    LOGGER.debug(String.format("public void setFetchDirection(int direction = {%s})", direction));
    checkIfClosed();
    if (direction != ResultSet.FETCH_FORWARD) {
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
    long[] largeUpdateCount = executeLargeBatch();
    int[] updateCount = new int[largeUpdateCount.length];

    for (int i = 0; i < largeUpdateCount.length; i++) {
      updateCount[i] = (int) largeUpdateCount[i];
    }

    return updateCount;
  }

  /** {@inheritDoc} */
  @Override
  public long[] executeLargeBatch() throws SQLException {
    LOGGER.debug("public long[] executeLargeBatch()");
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
          "Method not supported: executeUpdate(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public long executeLargeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
    LOGGER.debug(
        "public long executeLargeUpdate(String sql = {}, int autoGeneratedKeys = {})",
        sql,
        autoGeneratedKeys);
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return executeLargeUpdate(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported: executeLargeUpdate(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeUpdate(String sql, int[] columnIndexes)");
  }

  @Override
  public long executeLargeUpdate(String sql, int[] columnIndexes) throws SQLException {
    LOGGER.debug(
        "public long executeLargeUpdate(String sql = {}, int[] columnIndexes = {})",
        sql,
        columnIndexes);
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeLargeUpdate(String sql, int[] columnIndexes)");
  }

  @Override
  public int executeUpdate(String sql, String[] columnNames) throws SQLException {
    LOGGER.debug("public int executeUpdate(String sql, String[] columnNames)");
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeUpdate(String sql, String[] columnNames)");
  }

  @Override
  public long executeLargeUpdate(String sql, String[] columnNames) throws SQLException {
    LOGGER.debug(
        "public long executeLargeUpdate(String sql = {}, String[] columnNames = {})",
        sql,
        columnNames);
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: executeLargeUpdate(String sql, String[] columnNames)");
  }

  @Override
  public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
    checkIfClosed();
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return execute(sql);
    } else {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Method not supported: execute(String sql, int autoGeneratedKeys)");
    }
  }

  @Override
  public boolean execute(String sql, int[] columnIndexes) throws SQLException {
    checkIfClosed();
    throw new DatabricksSQLFeatureNotSupportedException(
        "Method not supported: execute(String sql, int[] columnIndexes)");
  }

  @Override
  public boolean execute(String sql, String[] columnNames) throws SQLException {
    checkIfClosed();
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
            "Class {%s} cannot be wrapped from {%s}", getClass().getName(), iface.getName()),
        DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
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
  public void setStatementId(StatementId statementId) {
    LOGGER.debug("void setStatementId(Statement statementId = {})", statementId);
    this.statementId = statementId;
  }

  @Override
  public StatementId getStatementId() {
    return this.statementId;
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
      throw new DatabricksSQLException(
          "Volume operation not supported for Input Stream",
          DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }

  @Override
  public InputStreamEntity getInputStreamForUCVolume() throws DatabricksSQLException {
    if (isAllowedInputStreamForVolumeOperation()) {
      return inputStream;
    }
    return null;
  }

  @Override
  public ResultSet executeAsync(String sql) throws SQLException {
    LOGGER.debug("ResultSet executeAsync() for statement {%s}", sql);
    checkIfClosed();
    IDatabricksClient client = connection.getSession().getDatabricksClient();
    DatabricksThreadContextHolder.setStatementType(StatementType.SQL);
    return client.executeStatementAsync(
        sql,
        connection.getSession().getComputeResource(),
        Collections.emptyMap(),
        connection.getSession(),
        this);
  }

  @Override
  public ResultSet getExecutionResult() throws SQLException {
    LOGGER.debug("ResultSet getExecutionResult() for statementId {%s}", statementId);
    checkIfClosed();

    if (statementId == null) {
      throw new DatabricksSQLException(
          "No execution available for statement", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
    return connection
        .getSession()
        .getDatabricksClient()
        .getStatementResult(statementId, connection.getSession(), this);
  }

  @Override
  public String enquoteLiteral(String val) throws SQLException {
    LOGGER.debug("String enquoteLiteral(String val = {})", val);
    checkIfClosed();
    return IDatabricksStatement.super.enquoteLiteral(val);
  }

  @Override
  public String enquoteIdentifier(String identifier, boolean alwaysQuote)
      throws DatabricksSQLException {
    LOGGER.debug(
        "String enquoteIdentifier(String identifier = {}, boolean alwaysQuote = {})",
        identifier,
        alwaysQuote);
    checkIfClosed();
    try {
      return IDatabricksStatement.super.enquoteIdentifier(identifier, alwaysQuote);
    } catch (SQLException e) {
      throw new DatabricksSQLException(
          e.getMessage(), DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }

  @Override
  public String enquoteNCharLiteral(String val) throws SQLException {
    LOGGER.debug("String enquoteNCharLiteral(String val = {})", val);
    checkIfClosed();
    return IDatabricksStatement.super.enquoteNCharLiteral(val);
  }

  @Override
  public boolean isSimpleIdentifier(String identifier) throws SQLException {
    LOGGER.debug("String isSimpleIdentifier(String identifier = {})", identifier);
    checkIfClosed();
    return IDatabricksStatement.super.isSimpleIdentifier(identifier);
  }

  static String trimCommentsAndWhitespaces(String query) {
    if (query == null || query.trim().isEmpty()) {
      throw new DatabricksDriverException(
          "Query cannot be null or empty", DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }

    // Trim and remove comments and whitespaces.
    String trimmedQuery = query.trim().replaceAll("(?m)--.*$", "");
    trimmedQuery = trimmedQuery.replaceAll("/\\*.*?\\*/", "");
    trimmedQuery = trimmedQuery.replaceAll("\\s+", " ").trim();

    return trimmedQuery;
  }

  @VisibleForTesting
  static boolean shouldReturnResultSet(String query, List<String> nonRowcountQueryPrefixes) {
    String trimmedQuery = trimCommentsAndWhitespaces(query);

    // Check configured non-rowcount prefixes first
    String upperQuery = trimmedQuery.toUpperCase();
    if (nonRowcountQueryPrefixes.stream().anyMatch(upperQuery::startsWith)) {
      return true;
    }

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
        || LIST_PATTERN.matcher(trimmedQuery).find()
        || BEGIN_PATTERN_FOR_SQL_SCRIPT.matcher(trimmedQuery).find()
        || CALL_PATTERN.matcher(trimmedQuery).find();

    // Otherwise, it should not return a ResultSet
  }

  protected boolean shouldReturnResultSetWithConfig(String query) {
    return shouldReturnResultSet(
        query, connection.getConnectionContext().getNonRowcountQueryPrefixes());
  }

  static boolean isSelectQuery(String query) {
    String trimmedQuery = trimCommentsAndWhitespaces(query);
    return SELECT_PATTERN.matcher(trimmedQuery).find();
  }

  static boolean isInsertQuery(String query) {
    if (query == null || query.trim().isEmpty()) {
      return false;
    }
    String trimmedQuery = trimCommentsAndWhitespaces(query);
    return INSERT_PATTERN.matcher(trimmedQuery).find();
  }

  DatabricksResultSet executeInternal(
      String sql,
      Map<Integer, ImmutableSqlParameter> params,
      StatementType statementType,
      boolean closeStatement)
      throws SQLException {
    String stackTraceMessage =
        format(
            "DatabricksResultSet executeInternal(String sql = %s, Map<Integer, ImmutableSqlParameter> params = {%s}, StatementType statementType = {%s})",
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
        try {
          close(); // Close the statement
        } catch (SQLException sqlExceptionForClose) {
          LOGGER.error(
              sqlExceptionForClose,
              String.format(
                  "Error occurred while closing statement after timeout. StatementId %s",
                  statementId));
        }
      }
      String timeoutErrorMessage =
          String.format(
              "Statement execution timed-out. ErrorMessage %s, statementId %s",
              stackTraceMessage, statementId);
      LOGGER.error(timeoutErrorMessage);
      futureResultSet.cancel(true); // Cancel execution run
      throw new DatabricksTimeoutException(
          timeoutErrorMessage, e, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
    } catch (InterruptedException | ExecutionException e) {
      Throwable cause = e;
      // Look for underlying SQLException (includes DatabricksSQLException and other SQL exceptions)
      while (cause.getCause() != null) {
        cause = cause.getCause();
        if (cause instanceof SQLException) {
          throw (SQLException) cause;
        }
      }
      String errMsg =
          String.format(
              "Error occurred during statement execution: %s. Error : %s", sql, e.getMessage());
      LOGGER.error(e, errMsg);
      throw new DatabricksSQLException(
          errMsg, e, DatabricksDriverErrorCode.EXECUTE_STATEMENT_FAILED);
    }
    LOGGER.debug("Result retrieved successfully {}", resultSet.toString());
    return resultSet;
  }

  DatabricksResultSet executeInternal(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType)
      throws SQLException {
    noMoreResults = false; // reset before each execution
    DatabricksThreadContextHolder.setStatementType(statementType);
    DatabricksResultSet result = executeInternal(sql, params, statementType, true);

    // Update the updateCount field based on the result
    if (result != null && result.hasUpdateCount()) {
      updateCount = result.getUpdateCount();
    } else {
      updateCount = -1; // SELECT query or no update count
    }

    return result;
  }

  CompletableFuture<DatabricksResultSet> getFutureResult(
      String sql, Map<Integer, ImmutableSqlParameter> params, StatementType statementType) {
    return CompletableFuture.supplyAsync(
        () -> {
          try {
            String SQLString = escapeProcessing ? StringUtil.convertJdbcEscapeSequences(sql) : sql;
            // Remove empty ESCAPE clauses that cause syntax errors in Databricks
            SQLString = StringUtil.removeRedundantEscapeClause(SQLString);
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
      throw new DatabricksSQLException(
          "Statement is closed", DatabricksDriverErrorCode.STATEMENT_CLOSED);
    }
  }

  /**
   * Marks the statement as closed without attempting to close it on the server or clean up local
   * resources. This should be used when the server has already indicated the statement is closed.
   *
   * <p>This method sets the closed flag to prevent further operations, but defers resource cleanup
   * (result set, executor) to the {@link #close(boolean)} method. When {@code close()} is
   * subsequently called, it will detect the statement is already closed and skip the server-side
   * close operation while still cleaning up local resources.
   *
   * @see #close(boolean)
   */
  public void markAsClosed() {
    LOGGER.debug("Marking statement {} as closed (server already closed)", statementId);
    this.connection.closeStatement(this);
    DatabricksThreadContextHolder.clearStatementInfo();
    this.isClosed = true;
  }

  /**
   * Shuts down the ExecutorService used for asynchronous execution.
   *
   * <p>Initiates an orderly shutdown of the executor, waiting up to 60 seconds for currently
   * executing tasks to terminate. If the executor does not terminate within the timeout, it is
   * forcefully shut down.
   */
  private void shutDownExecutor() {
    executor.shutdown();
    try {
      if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
        executor.shutdownNow();
      }
    } catch (InterruptedException e) {
      executor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public String toString() {
    return (new ToStringer(DatabricksStatement.class))
        .add("statementId", this.statementId)
        .toString();
  }
}
