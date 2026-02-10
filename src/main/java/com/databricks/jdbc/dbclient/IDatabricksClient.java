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
import com.databricks.jdbc.model.core.ChunkLinkFetchResult;
import com.databricks.jdbc.model.core.ResultData;
import com.databricks.jdbc.telemetry.latency.DatabricksMetricsTimed;
import com.databricks.sdk.core.DatabricksConfig;
import java.sql.SQLException;
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
  @DatabricksMetricsTimed
  ImmutableSessionInfo createSession(
      IDatabricksComputeResource computeResource,
      String catalog,
      String schema,
      Map<String, String> sessionConf)
      throws SQLException;

  /**
   * Deletes a session for given session-Id
   *
   * @param sessionInfo for which the session should be deleted
   */
  @DatabricksMetricsTimed
  void deleteSession(ImmutableSessionInfo sessionInfo) throws SQLException;

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
  @DatabricksMetricsTimed
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
  @DatabricksMetricsTimed
  void closeStatement(StatementId statementId) throws DatabricksSQLException;

  /**
   * Cancels a statement in Databricks server
   *
   * @param statementId statement which should be aborted
   */
  @DatabricksMetricsTimed
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
   * Fetches the chunk links for given chunk index and statement-Id.
   *
   * <p>For SEA clients, the chunkIndex is used to identify which chunk to fetch. For Thrift
   * clients, the rowOffset is used with FETCH_ABSOLUTE orientation to seek to the correct position.
   *
   * <p>The returned {@link ChunkLinkFetchResult} contains the chunk links and continuation
   * information:
   *
   * <ul>
   *   <li>SEA: hasMore derived from last link's nextChunkIndex
   *   <li>Thrift: hasMore from server's hasMoreRows flag, nextRowOffset for continuation
   * </ul>
   *
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   * @param chunkStartRowOffset the row offset where the chunk starts in the result set
   */
  ChunkLinkFetchResult getResultChunks(
      StatementId statementId, long chunkIndex, long chunkStartRowOffset) throws SQLException;

  /**
   * Fetches the result data for given chunk index and statement-Id.
   *
   * @param statementId statement-Id for which chunk data should be fetched
   * @param chunkIndex chunkIndex for which chunk data should be fetched
   * @return ResultData containing the chunk's data array and metadata
   */
  ResultData getResultChunksData(StatementId statementId, long chunkIndex)
      throws DatabricksSQLException;

  IDatabricksConnectionContext getConnectionContext();

  /**
   * Update the access token based on new value provided by the customer
   *
   * @param newAccessToken new access token value
   */
  void resetAccessToken(String newAccessToken);

  TFetchResultsResp getMoreResults(IDatabricksStatementInternal parentStatement)
      throws SQLException;

  /** Retrieves underlying DatabricksConfig */
  DatabricksConfig getDatabricksConfig();
}
