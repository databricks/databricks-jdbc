package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.StatementId;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.client.thrift.generated.TFetchResultsResp;
import com.databricks.jdbc.model.core.ExternalLink;
import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimed;
import com.databricks.sdk.core.DatabricksConfig;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/** Interface for Databricks client which abstracts the integration with Databricks server. */
public interface IDatabricksClient {

  /**
   * Creates a new session for given warehouse-Id, catalog and session.
   *
   * @param computeResource underlying SQL-warehouse or all-purpose cluster
   * @param catalog for the session
   * @param schema for the session
   * @param sessionConf session configuration
   * @return created session
   */
  ImmutableSessionInfo createSession(
      IDatabricksComputeResource computeResource,
      String catalog,
      String schema,
      Map<String, String> sessionConf)
      throws DatabricksSQLException;

  /**
   * Deletes a session for given session-Id
   *
   * @param sessionInfo for which the session should be deleted
   */
  void deleteSession(ImmutableSessionInfo sessionInfo) throws DatabricksSQLException;

  /**
   * Executes a statement in Databricks server
   *
   * @param sql SQL statement that needs to be executed
   * @param computeResource underlying SQL-warehouse or all-purpose cluster
   * @param parameters SQL parameters for the statement
   * @param statementType type of statement (metadata, update or generic SQL)
   * @param session underlying session
   * @param parentStatement statement instance if called from a statement
   * @return response for statement execution
   */
  @DatabricksMetricsTimed
  DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException;

  /**
   * Executes a statement in Databricks server asynchronously
   *
   * @param sql SQL statement that needs to be executed
   * @param computeResource underlying SQL-warehouse or all-purpose cluster
   * @param parameters SQL parameters for the statement
   * @param session underlying session
   * @param parentStatement statement instance if called from a statement
   * @return response for statement execution
   */
  DatabricksResultSet executeStatementAsync(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException;

  /**
   * Closes a statement in Databricks server
   *
   * @param statementId statement which should be closed
   */
  void closeStatement(StatementId statementId) throws DatabricksSQLException;

  /**
   * Cancels a statement in Databricks server
   *
   * @param statementId statement which should be aborted
   */
  void cancelStatement(StatementId statementId) throws DatabricksSQLException;

  /**
   * Fetches result for underlying statement-Id
   *
   * @param statementId statement which should be checked for status
   * @param session underlying session
   * @param parentStatement statement instance
   */
  DatabricksResultSet getStatementResult(
      StatementId statementId,
      IDatabricksSession session,
      IDatabricksStatementInternal parentStatement)
      throws SQLException;

  /**
   * Fetches the chunk details for given chunk index and statement-Id.
   *
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   */
  Collection<ExternalLink> getResultChunks(StatementId statementId, long chunkIndex)
      throws DatabricksSQLException;

  IDatabricksConnectionContext getConnectionContext();

  /**
   * Update the access token based on new value provided by the customer
   *
   * @param newAccessToken new access token value
   */
  void resetAccessToken(String newAccessToken);

  TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws DatabricksSQLException;

  /** Retrieves underlying DatabricksConfig */
  DatabricksConfig getDatabricksConfig();
}
