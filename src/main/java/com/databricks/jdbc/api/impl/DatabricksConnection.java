package com.databricks.jdbc.api.impl;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP;

import com.databricks.jdbc.api.*;
import com.databricks.jdbc.api.IDatabricksStatement;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionInternal;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.common.util.UserAgentManager;
import com.databricks.jdbc.common.util.ValidationUtil;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.impl.common.SessionId;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.*;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryClientFactory;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/** Implementation for Databricks specific connection. */
public class DatabricksConnection implements IDatabricksConnection, IDatabricksConnectionInternal {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksConnection.class);
  private final IDatabricksSession session;
  private final Set<IDatabricksStatementInternal> statementSet = ConcurrentHashMap.newKeySet();
  private SQLWarning warnings = null;
  private final IDatabricksConnectionContext connectionContext;

  /**
   * Creates an instance of Databricks connection for given connection context.
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksConnection(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    this.connectionContext = connectionContext;
    DatabricksThreadContextHolder.setConnectionContext(connectionContext);
    this.session = new DatabricksSession(connectionContext);
  }

  @VisibleForTesting
  public DatabricksConnection(
      IDatabricksConnectionContext connectionContext, IDatabricksClient testDatabricksClient)
      throws DatabricksSQLException {
    this.connectionContext = connectionContext;
    DatabricksThreadContextHolder.setConnectionContext(connectionContext);
    this.session = new DatabricksSession(connectionContext, testDatabricksClient);
    UserAgentManager.setUserAgent(connectionContext);
    TelemetryHelper.updateTelemetryAppName(connectionContext, null);
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
  public String getConnectionId() throws SQLException {
    if (session.getSessionInfo() == null) {
      LOGGER.error("Session not initialized");
      throw new DatabricksValidationException("Session not initialized");
    }
    return SessionId.create(Objects.requireNonNull(session.getSessionInfo())).toString();
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
    LOGGER.debug(
        String.format("public PreparedStatement prepareStatement(String sql = {%s})", sql));
    DatabricksPreparedStatement statement = new DatabricksPreparedStatement(this, sql);
    statementSet.add(statement);
    return statement;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    LOGGER.debug(String.format("public CallableStatement prepareCall= {%s})", sql));
    throw new DatabricksSQLFeatureNotImplementedException(
        "Callable statements are not implemented in OSS JDBC");
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    LOGGER.debug(String.format("public String nativeSQL(String sql{%s})", sql));
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks OSS JDBC does not support conversion to native query.");
  }

  /**
   * Sets the auto-commit mode for this connection to the given state.
   *
   * <p>When auto-commit is enabled (the default), each SQL statement is executed as an individual
   * transaction and is committed immediately upon completion.
   *
   * <p>When auto-commit is disabled, SQL statements are grouped into transactions that must be
   * explicitly committed via {@link #commit()} or rolled back via {@link #rollback()}. When
   * auto-commit is disabled, a new transaction is automatically started:
   *
   * <ul>
   *   <li>Immediately after executing SET AUTOCOMMIT = FALSE
   *   <li>After each COMMIT or ROLLBACK statement
   * </ul>
   *
   * <p><b>Thread Safety</b>
   *
   * <p><b>This method is not thread-safe.</b> The {@code Connection} object should not be shared
   * across multiple threads. Concurrent access may lead to undefined behavior and server-side
   * transaction aborts due to sequence ID mismatches. Each thread should obtain its own {@code
   * Connection} from a connection pool.
   *
   * <p><b>Example Usage</b>
   *
   * <pre>{@code
   * // Disable auto-commit to start a transaction
   * connection.setAutoCommit(false);
   *
   * try {
   *     // Execute multiple statements as part of one transaction
   *     statement.executeUpdate("UPDATE accounts SET balance = balance - 100 WHERE id = 1");
   *     statement.executeUpdate("UPDATE accounts SET balance = balance + 100 WHERE id = 2");
   *
   *     // Commit the transaction
   *     connection.commit();
   * } catch (SQLException e) {
   *     // Rollback on error
   *     connection.rollback();
   *     throw e;
   * }
   * }</pre>
   *
   * @param autoCommit {@code true} to enable auto-commit mode; {@code false} to disable it
   * @throws DatabricksSQLException if the connection is closed
   * @throws DatabricksTransactionException if the auto-commit mode cannot be changed due to an
   *     active transaction or invalid state
   * @see #getAutoCommit()
   * @see #commit()
   * @see #rollback()
   */
  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    LOGGER.debug("setAutoCommit({})", autoCommit);
    throwExceptionIfConnectionIsClosed();

    // Backward compatibility: honor ignoreTransactions flag (deprecated)
    if (connectionContext.getIgnoreTransactions()) {
      LOGGER.warn(
          "ignoreTransactions flag is set - setAutoCommit is no-op (deprecated behavior). "
              + "Please remove this flag to enable transaction support.");
      return;
    }

    // Execute SET AUTOCOMMIT command
    Statement statement = null;
    try {
      statement = createStatement();
      String sql = "SET AUTOCOMMIT = " + (autoCommit ? "TRUE" : "FALSE");
      statement.execute(sql);

      // Success: update local cache
      session.setAutoCommit(autoCommit);

    } catch (SQLException e) {
      LOGGER.error(e, "Error {} while setting autoCommit to {}", e.getMessage(), autoCommit);
      throw new DatabricksTransactionException(
          e.getMessage(), e, DatabricksDriverErrorCode.TRANSACTION_SET_AUTOCOMMIT_ERROR);

    } finally {
      closeStatementSafely(statement);
    }
  }

  /**
   * Retrieves the current auto-commit mode for this connection.
   *
   * <p>On a newly created connection, returns {@code true} (JDBC default) without making a server
   * round-trip.
   *
   * <p>After {@link #setAutoCommit(boolean)} is called, returns the cached value from the session.
   *
   * <p>If the connection property {@code FetchAutoCommitFromServer=1} is set, this method will
   * query the server using {@code SET AUTOCOMMIT} SQL command to retrieve the current auto-commit
   * state, ensuring the returned value matches the server state. This is useful for debugging or
   * when strict state verification is needed.
   *
   * @return true if auto-commit mode is enabled; false otherwise
   * @throws DatabricksSQLException if the connection is closed
   * @throws DatabricksSQLException if querying the server fails (when FetchAutoCommitFromServer=1)
   * @see #setAutoCommit(boolean)
   */
  @Override
  public boolean getAutoCommit() throws SQLException {
    LOGGER.debug("getAutoCommit()");
    throwExceptionIfConnectionIsClosed();

    // If FetchAutoCommitFromServer is enabled, query the server for current state
    if (connectionContext.getFetchAutoCommitFromServer()) {
      return fetchAutoCommitStateFromServer();
    }

    // Default: return cached value
    return session.getAutoCommit();
  }

  /**
   * Fetches the auto-commit state from the server by executing SET AUTOCOMMIT query.
   *
   * @return true if auto-commit is enabled on the server; false otherwise
   * @throws SQLException if the query fails
   */
  private boolean fetchAutoCommitStateFromServer() throws SQLException {
    Statement statement = null;
    try {
      statement = createStatement();
      // Execute SET AUTOCOMMIT without a value to query the current state
      ResultSet rs = statement.executeQuery("SET AUTOCOMMIT");

      if (rs.next()) {
        // The result should contain the value = "true" or "false"
        String value = rs.getString(1); // Column 1: value

        LOGGER.debug(
            "Fetched autoCommit state from server: value={}. Updating session cache.", value);

        boolean autoCommitState = "true".equalsIgnoreCase(value);

        // Update the session cache with the server value
        session.setAutoCommit(autoCommitState);

        rs.close();
        return autoCommitState;
      } else {
        throw new DatabricksSQLException(
            "Failed to fetch autoCommit state from server: no result returned",
            DatabricksDriverErrorCode.TRANSACTION_SET_AUTOCOMMIT_ERROR);
      }

    } catch (SQLException e) {
      LOGGER.error(e, "Error {} while fetching autoCommit state from server", e.getMessage());
      throw new DatabricksSQLException(
          "Failed to fetch autoCommit state from server: " + e.getMessage(),
          e,
          DatabricksDriverErrorCode.TRANSACTION_SET_AUTOCOMMIT_ERROR);

    } finally {
      closeStatementSafely(statement);
    }
  }

  /**
   * Makes all changes made since the previous commit/rollback permanent.
   *
   * <p>This method should be used only when auto-commit mode has been disabled.
   *
   * <p>When auto-commit is FALSE:
   *
   * <ul>
   *   <li>Commits the current transaction
   *   <li>A new transaction begins automatically
   * </ul>
   *
   * <p>When auto-commit is TRUE:
   *
   * <ul>
   *   <li>This operation throws {@link DatabricksTransactionException} (if ignoreTransactions flag
   *       is not set)
   * </ul>
   *
   * @throws DatabricksSQLException if the connection is closed
   * @throws DatabricksTransactionException for transaction-specific errors such as
   *     MULTI_STATEMENT_TRANSACTION_NO_ACTIVE_TRANSACTION or
   *     MULTI_STATEMENT_TRANSACTION_ROLLBACK_REQUIRED_AFTER_ABORT
   * @see #setAutoCommit(boolean)
   * @see #rollback()
   */
  @Override
  public void commit() throws SQLException {
    LOGGER.debug("commit()");
    throwExceptionIfConnectionIsClosed();

    // Backward compatibility: honor ignoreTransactions flag (deprecated)
    if (connectionContext.getIgnoreTransactions()) {
      LOGGER.warn(
          "ignoreTransactions flag is set - commit is no-op (deprecated behavior). "
              + "Please remove this flag to enable transaction support.");
      return;
    }

    // Execute COMMIT command
    Statement statement = null;
    try {
      statement = createStatement();
      statement.execute("COMMIT");
      // Note: Server auto-starts new transaction if autocommit=false

    } catch (SQLException e) {
      LOGGER.error(e, "Error {} while committing transaction", e.getMessage());
      throw new DatabricksTransactionException(
          e.getMessage(), e, DatabricksDriverErrorCode.TRANSACTION_COMMIT_ERROR);

    } finally {
      closeStatementSafely(statement);
    }
  }

  /**
   * Undoes all changes made in the current transaction.
   *
   * <p>This method should be used only when auto-commit mode has been disabled.
   *
   * <p>When auto-commit is FALSE:
   *
   * <ul>
   *   <li>Rolls back the current transaction
   *   <li>A new transaction begins automatically (per autocommit design)
   * </ul>
   *
   * <p>When auto-commit is TRUE:
   *
   * <ul>
   *   <li>ROLLBACK is a safe no-op (does not throw exception)
   *   <li>This is more forgiving than COMMIT, which throws an exception when there's no active
   *       transaction
   * </ul>
   *
   * <p><b>Note:</b> ROLLBACK is designed to be safe to call even when there is no active
   * transaction. It can be used to recover from error states without needing to check transaction
   * status first.
   *
   * @throws DatabricksSQLException if the connection is closed
   * @throws DatabricksTransactionException for transaction-specific errors (rare - ROLLBACK is
   *     typically very forgiving)
   * @see #setAutoCommit(boolean)
   * @see #commit()
   */
  @Override
  public void rollback() throws SQLException {
    LOGGER.debug("rollback()");
    throwExceptionIfConnectionIsClosed();

    // Backward compatibility: honor ignoreTransactions flag (deprecated)
    if (connectionContext.getIgnoreTransactions()) {
      LOGGER.warn(
          "ignoreTransactions flag is set - rollback is no-op (deprecated behavior). "
              + "Please remove this flag to enable transaction support.");
      return;
    }

    // Execute ROLLBACK command
    Statement statement = null;
    try {
      statement = createStatement();
      statement.execute("ROLLBACK");
      // Note: Server auto-starts new transaction if autocommit=false
      // Note: ROLLBACK is more forgiving - typically succeeds even on unexpected states

    } catch (SQLException e) {
      LOGGER.error(e, "Error {} while rolling back transaction", e.getMessage());
      throw new DatabricksTransactionException(
          e.getMessage(), e, DatabricksDriverErrorCode.TRANSACTION_ROLLBACK_ERROR);

    } finally {
      closeStatementSafely(statement);
    }
  }

  @Override
  public void close() throws DatabricksSQLException {
    LOGGER.debug("public void close()");
    for (IDatabricksStatementInternal statement : statementSet) {
      statement.close(false);
      statementSet.remove(statement);
    }
    this.session.close();
    TelemetryClientFactory.getInstance().closeTelemetryClient(connectionContext);
    DatabricksClientConfiguratorManager.getInstance().removeInstance(connectionContext);
    DatabricksDriverFeatureFlagsContextFactory.removeInstance(connectionContext);
    DatabricksHttpClientFactory.getInstance().removeClient(connectionContext);
    DatabricksThreadContextHolder.clearAllContext();
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
    LOGGER.debug("public void setReadOnly(boolean readOnly)");
    throwExceptionIfConnectionIsClosed();
    if (readOnly) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Databricks OSS JDBC does not support readOnly mode.");
    }
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    LOGGER.debug("public boolean isReadOnly()");
    throwExceptionIfConnectionIsClosed();
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // If enableMultipleCatalogSupport is disabled, setCatalog does nothing
    if (!connectionContext.getEnableMultipleCatalogSupport()) {
      LOGGER.debug("setCatalog ignored - enableMultipleCatalogSupport is disabled");
      return;
    }
    Statement statement = this.createStatement();
    statement.execute("SET CATALOG " + catalog);
    this.session.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    LOGGER.debug("public String getCatalog()");
    if (session.getCatalog() == null) {
      fetchCurrentSchemaAndCatalog();
    }
    return this.session.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    LOGGER.debug("public void setTransactionIsolation(int level = {})", level);
    throwExceptionIfConnectionIsClosed();
    if (level != Connection.TRANSACTION_REPEATABLE_READ) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Setting of the given transaction isolation is not supported");
    }
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    LOGGER.debug("public int getTransactionIsolation()");
    throwExceptionIfConnectionIsClosed();
    return Connection.TRANSACTION_REPEATABLE_READ;
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
    throw new DatabricksSQLFeatureNotImplementedException(
        "Callable statements are not implemented in OSS JDBC");
  }

  @Override
  public Map<String, Class<?>> getTypeMap() {
    LOGGER.debug("public Map<String, Class<?>> getTypeMap()");
    return new HashMap<>();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    LOGGER.debug("public void setTypeMap(Map<String, Class<?>> map)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks OSS JDBC does not support setting of type map in connection");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    if (holdability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new DatabricksSQLFeatureNotSupportedException(
          "Databricks OSS JDBC only supports holdability of CLOSE_CURSORS_AT_COMMIT");
    }
  }

  @Override
  public int getHoldability() throws SQLException {
    LOGGER.debug("public int getHoldability()");
    return ResultSet.CLOSE_CURSORS_AT_COMMIT;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    LOGGER.debug("public Savepoint setSavepoint()");
    if (!connectionContext.getIgnoreTransactions()) {
      throw new DatabricksSQLFeatureNotImplementedException(
          "Not implemented in DatabricksConnection - setSavepoint()");
    }
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    LOGGER.debug("public Savepoint setSavepoint(String name = {})", name);
    if (!connectionContext.getIgnoreTransactions()) {
      throw new DatabricksSQLFeatureNotImplementedException(
          "Not implemented in DatabricksConnection - setSavepoint(String name)");
    }
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void rollback(Savepoint savepoint)");
    if (!connectionContext.getIgnoreTransactions()) {
      throw new DatabricksSQLFeatureNotImplementedException(
          "Not implemented in DatabricksConnection - rollback(Savepoint savepoint)");
    }
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    LOGGER.debug("public void releaseSavepoint(Savepoint savepoint)");
    if (!connectionContext.getIgnoreTransactions()) {
      throw new DatabricksSQLFeatureNotImplementedException(
          "Not implemented in DatabricksConnection - releaseSavepoint(Savepoint savepoint)");
    }
  }

  @Override
  public Statement createStatement(
      int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    if (resultSetType != ResultSet.TYPE_FORWARD_ONLY
        || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY
        || resultSetHoldability != ResultSet.CLOSE_CURSORS_AT_COMMIT) {
      throw new DatabricksSQLFeatureNotImplementedException(
          "Databricks OSS JDBC only supports resultSetType as ResultSet.TYPE_FORWARD_ONLY, resultSetConcurrency as ResultSet.CONCUR_READ_ONLY and resultSetHoldability as ResultSet.CLOSE_CURSORS_AT_COMMIT");
    }
    return createStatement();
  }

  @Override
  public PreparedStatement prepareStatement(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    if (isClosed()) {
      throw new DatabricksSQLException(
          "Connection is closed", DatabricksDriverErrorCode.CONNECTION_CLOSED);
    }
    if (resultSetHoldability == getHoldability()) {
      return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks OSS JDBC only supports holdability of CLOSE_CURSORS_AT_COMMIT");
  }

  @Override
  public CallableStatement prepareCall(
      String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability)
      throws SQLException {
    throw new DatabricksSQLFeatureNotImplementedException(
        "Callable statements are not implemented in OSS JDBC");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    if (isClosed()) {
      throw new DatabricksSQLException(
          "Connection is closed", DatabricksDriverErrorCode.CONNECTION_CLOSED);
    }
    if (autoGeneratedKeys == Statement.NO_GENERATED_KEYS) {
      return prepareStatement(sql);
    }
    throw new DatabricksSQLFeatureNotSupportedException(
        "Databricks OSS JDBC does not support auto generated keys");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not supported in DatabricksConnection - prepareStatement(String sql, int[] columnIndexes)");
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not supported in DatabricksConnection - prepareStatement(String sql, String[] columnNames)");
  }

  @Override
  public Clob createClob() throws SQLException {
    LOGGER.debug("public Clob createClob()");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createClob()");
  }

  @Override
  public Blob createBlob() throws SQLException {
    LOGGER.debug("public Blob createBlob()");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createBlob()");
  }

  @Override
  public NClob createNClob() throws SQLException {
    LOGGER.debug("public NClob createNClob()");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createNClob()");
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    LOGGER.debug("public SQLXML createSQLXML()");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createSQLXML()");
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    ValidationUtil.checkIfNonNegative(timeout, "timeout");
    if (isClosed()) {
      return false;
    }
    if (connectionContext.getEnableSQLValidationForIsValid()) {
      try (Statement stmt = createStatement()) {
        stmt.setQueryTimeout(timeout);
        // This is a lightweight query to check if the connection is valid
        stmt.execute("SELECT VERSION()");
        return true;
      } catch (Exception e) {
        LOGGER.debug("Validation failed for isValid(): {}", e.getMessage());
        return false;
      }
    }
    return true;
  }

  /**
   * Sets a client info property/session config
   *
   * @param name The name of the property to set
   * @param value The value to set
   * @throws SQLClientInfoException If the property cannot be set due to validation errors or if the
   *     property name is not recognized
   */
  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    if (ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.keySet().stream()
        .map(String::toLowerCase)
        .anyMatch(s -> s.equalsIgnoreCase(name))) {
      Map<String, ClientInfoStatus> failedProperties = new HashMap<>();
      setSessionConfig(name, value, failedProperties);
      if (!failedProperties.isEmpty()) {
        String errorMessage = getFailedPropertiesExceptionMessage(failedProperties);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLClientInfoException(
            errorMessage, failedProperties, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
      }
    } else {
      if (DatabricksJdbcConstants.ALLOWED_CLIENT_INFO_PROPERTIES.stream()
          .map(String::toLowerCase)
          .anyMatch(s -> s.equalsIgnoreCase(name))) {
        this.session.setClientInfoProperty(
            name.toLowerCase(), value); // insert properties in lower case
      } else {
        String errorMessage =
            String.format(
                "Setting client info for %s failed with %s",
                name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY);
        LOGGER.error(errorMessage);
        throw new DatabricksSQLClientInfoException(
            errorMessage,
            Map.of(name, ClientInfoStatus.REASON_UNKNOWN_PROPERTY),
            DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
      }
    }
  }

  /**
   * Sets multiple client info properties from the provided Properties object.
   *
   * @param properties The properties containing client info to set
   * @throws SQLClientInfoException If any property cannot be set
   */
  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    LOGGER.debug("public void setClientInfo(Properties properties)");
    for (Map.Entry<Object, Object> entry : properties.entrySet()) {
      setClientInfo((String) entry.getKey(), (String) entry.getValue());
    }
  }

  /**
   * Retrieves the value of the specified client info property. case-insensitive
   *
   * @param name The name of the client info property to retrieve
   * @return The value of the specified client info property, or null if not found
   * @throws SQLException If a database access error occurs
   */
  @Override
  public String getClientInfo(String name) throws SQLException {
    // Return session/client conf if set
    String value = session.getConfigValue(name);
    if (value != null) {
      return value;
    }
    return ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.getOrDefault(
        name.toUpperCase(), null); // Conf Map stores keys in upper case
  }

  /**
   * Retrieves all client and session properties as a Properties object. Keys are in lower case.
   *
   * <p>The returned Properties object contains default session configurations, user-defined session
   * configurations, and client info properties.
   *
   * @return A Properties object containing all client info properties
   * @throws SQLException If a database access error occurs
   */
  @Override
  public Properties getClientInfo() throws SQLException {
    LOGGER.debug("public Properties getClientInfo()");

    Properties properties = new Properties();

    // add default session configs
    ALLOWED_SESSION_CONF_TO_DEFAULT_VALUES_MAP.forEach(
        (key, value) -> properties.setProperty(key.toLowerCase(), value));

    // update session configs if set by user
    properties.putAll(session.getSessionConfigs());
    // add client info properties
    properties.putAll(session.getClientInfoProperties());

    return properties;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    LOGGER.debug("public Array createArrayOf(String typeName, Object[] elements)");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createArrayOf(String typeName, Object[] elements)");
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    LOGGER.debug("public Struct createStruct(String typeName, Object[] attributes)");
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - createStruct(String typeName, Object[] attributes)");
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    Statement statement = this.createStatement();
    statement.execute("USE SCHEMA " + schema);
    session.setSchema(schema);
  }

  @Override
  public String getSchema() throws SQLException {
    LOGGER.debug("public String getSchema()");
    if (session.getSchema() == null) {
      fetchCurrentSchemaAndCatalog();
    }
    return session.getSchema();
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    LOGGER.debug("public void abort(Executor executor)");
    executor.execute(
        () -> {
          try {
            this.close();
          } catch (Exception e) {
            LOGGER.error(
                "Error closing connection resources, but marking the connection as closed.", e);
            this.session.forceClose();
          }
        });
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {
    LOGGER.debug("public void setNetworkTimeout(Executor executor, int milliseconds)");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not supported in DatabricksConnection - setNetworkTimeout(Executor executor, int milliseconds)");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    LOGGER.debug("public int getNetworkTimeout()");
    throw new DatabricksSQLFeatureNotSupportedException(
        "Not supported in DatabricksConnection - getNetworkTimeout()");
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    LOGGER.debug("public <T> T unwrap(Class<T> iface)");
    if (iface.isInstance(this)) {
      return (T) this;
    }
    String errorMessage =
        String.format(
            "Class {%s} cannot be wrapped from {%s}", this.getClass().getName(), iface.getName());
    LOGGER.error(errorMessage);
    throw new DatabricksValidationException(errorMessage);
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
  public void beginRequest() {
    LOGGER.debug("public void beginRequest()");
    LOGGER.warn("public void beginRequest() is a no-op method");
  }

  @Override
  public void endRequest() {
    LOGGER.debug("public void endRequest()");
    LOGGER.warn("public void endRequest() is a no-op method");
  }

  @Override
  public boolean setShardingKeyIfValid(
      ShardingKey shardingKey, ShardingKey superShardingKey, int timeout)
      throws DatabricksSQLFeatureNotImplementedException {
    LOGGER.debug(
        "public boolean setShardingKeyIfValid(ShardingKey shardingKey = {},ShardingKey superShardingKey = {}, int timeout = {})",
        shardingKey,
        superShardingKey,
        timeout);
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - setShardingKeyIfValid(ShardingKey shardingKey, ShardingKey superShardingKey, int timeout)");
  }

  @Override
  public boolean setShardingKeyIfValid(ShardingKey shardingKey, int timeout)
      throws DatabricksSQLFeatureNotImplementedException {
    LOGGER.debug(
        "public boolean setShardingKeyIfValid(ShardingKey shardingKey = {}, int timeout = {})",
        shardingKey,
        timeout);
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - setShardingKeyIfValid(ShardingKey shardingKey, int timeout)");
  }

  @Override
  public void setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey)
      throws DatabricksSQLFeatureNotImplementedException {
    LOGGER.debug(
        "public void setShardingKey(ShardingKey shardingKey = {}, ShardingKey superShardingKey = {})",
        shardingKey,
        superShardingKey);
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - setShardingKey(ShardingKey shardingKey, ShardingKey superShardingKey)");
  }

  @Override
  public void setShardingKey(ShardingKey shardingKey)
      throws DatabricksSQLFeatureNotImplementedException {
    LOGGER.debug("public void setShardingKey(ShardingKey shardingKey = {})", shardingKey);
    throw new DatabricksSQLFeatureNotImplementedException(
        "Not implemented in DatabricksConnection - setShardingKey(ShardingKey shardingKey)");
  }

  @Override
  public Connection getConnection() {
    return this;
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
      this.session.setSessionConfig(key.toLowerCase(), value); // insert properties in lower case
    } catch (SQLException e) {
      ClientInfoStatus status = determineClientInfoStatus(key, value, e);
      failedProperties.put(key, status);
    }
  }

  private void throwExceptionIfConnectionIsClosed() throws SQLException {
    if (this.isClosed()) {
      throw new DatabricksSQLException(
          "Connection closed!", DatabricksDriverErrorCode.CONNECTION_CLOSED);
    }
  }

  private void fetchCurrentSchemaAndCatalog() throws DatabricksSQLException {
    try {
      DatabricksStatement statement = (DatabricksStatement) this.createStatement();
      ResultSet rs = statement.executeQuery("SELECT CURRENT_CATALOG(), CURRENT_SCHEMA()");
      if (rs.next()) {
        session.setCatalog(rs.getString(1));
        session.setSchema(rs.getString(2));
      }
    } catch (SQLException e) {
      String errorMessage =
          String.format("Error fetching current schema and catalog %s", e.getMessage());
      LOGGER.error(e, errorMessage);
      throw new DatabricksSQLException(
          errorMessage, DatabricksDriverErrorCode.CATALOG_OR_SCHEMA_FETCH_ERROR);
    }
  }

  /**
   * Safely closes a statement, logging any errors but not throwing exceptions.
   *
   * <p>This helper method is used to ensure statements are always closed in finally blocks without
   * masking the original exception.
   *
   * @param statement the statement to close, may be null
   */
  private void closeStatementSafely(Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        LOGGER.error(e, "Error closing statement: {}", e.getMessage());
      }
    }
  }
}
