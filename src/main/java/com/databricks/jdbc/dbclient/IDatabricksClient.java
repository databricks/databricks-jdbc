package com.databricks.jdbc.dbclient;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.api.callback.IDatabricksStatementHandle;
import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.core.ExternalLink;
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
   * @param session for which the session should be deleted
   * @param computeResource underlying SQL-warehouse or all-purpose cluster
   */
  void deleteSession(IDatabricksSession session, IDatabricksComputeResource computeResource)
      throws DatabricksSQLException;

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
  DatabricksResultSet executeStatement(
      String sql,
      IDatabricksComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatementHandle parentStatement)
      throws SQLException;

  /**
   * Closes a statement in Databricks server
   *
   * @param statementId statement which should be closed
   */
  void closeStatement(String statementId) throws DatabricksSQLException;

  /**
   * Cancels a statement in Databricks server
   *
   * @param statementId statement which should be aborted
   */
  void cancelStatement(String statementId) throws DatabricksSQLException;

  /**
   * Fetches the chunk details for given chunk index and statement-Id.
   *
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   */
  Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex)
      throws DatabricksSQLException;

  IDatabricksConnectionContext getConnectionContext();

  /**
   * Update the access token based on new value provided by the customer
   *
   * @param newAccessToken new access token value
   */
  void resetAccessToken(String newAccessToken);
}
