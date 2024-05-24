package com.databricks.jdbc.client;

import com.databricks.jdbc.client.sqlexec.ExternalLink;
import com.databricks.jdbc.core.*;
import com.databricks.jdbc.core.types.ComputeResource;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

/** Interface for Databricks client which abstracts the integration with Databricks server. */
public interface DatabricksClient {

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
      ComputeResource computeResource,
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
  void deleteSession(IDatabricksSession session, ComputeResource computeResource)
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
      ComputeResource computeResource,
      Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType,
      IDatabricksSession session,
      IDatabricksStatement parentStatement)
      throws SQLException;

  /**
   * Closes a statement in Databricks server
   *
   * @param statementId statement which should be closed
   * @return response for statement execution
   */
  void closeStatement(String statementId) throws DatabricksSQLException;

  /**
   * Fetches the chunk details for given chunk index and statement-Id.
   *
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   */
  Collection<ExternalLink> getResultChunks(String statementId, long chunkIndex)
      throws DatabricksSQLException;
}
