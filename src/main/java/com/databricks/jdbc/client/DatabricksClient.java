package com.databricks.jdbc.client;

import com.databricks.jdbc.client.sqlexec.Session;
import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.core.IDatabricksStatement;
import com.databricks.jdbc.core.ImmutableSqlParameter;
import com.databricks.sdk.service.sql.ExternalLink;

import java.sql.SQLException;
import java.util.Map;
import java.util.Optional;

/**
 * Interface for Databricks client which abstracts the integration with Databricks server.
 */
public interface DatabricksClient {

  /**
   * Creates a new session for given warehouse-Id.
   * @param warehouseId for which a session should be created
   * @return created session
   */
  Session createSession(String warehouseId);

  /**
   * Deletes a session for given session-Id
   * @param sessionId for which the session should be deleted
   * @param warehouseId underlying warehouse-Id
   */
  void deleteSession(String sessionId, String warehouseId);

  /**
   * Executes a statement in Databricks server
   * @param statement SQL statement that needs to be executed
   * @param warehouseId warehouse-Id which should be used for statement execution
   * @param parameters SQL parameters for the statement
   * @param statementType type of statement (metadata, update or generic SQL)
   * @param session underlying session
   * @param statement statement instance if called from a statement
   * @return response for statement execution
   */
  DatabricksResultSet executeStatement(
      String sql, String warehouseId, Map<Integer, ImmutableSqlParameter> parameters,
      StatementType statementType, IDatabricksSession session, IDatabricksStatement parentStatement)
      throws SQLException;

  /**
   * Closes a statement in Databricks server
   * @param statementId statement which should be closed
   * @return response for statement execution
   */
  void closeStatement(String statementId);

  /**
   * Fetches the chunk details for given chunk index and statement-Id.
   * @param statementId statement-Id for which chunk should be fetched
   * @param chunkIndex chunkIndex for which chunk should be fetched
   */
  Optional<ExternalLink> getResultChunk(String statementId, long chunkIndex);
}
