package com.databricks.jdbc.client;

import com.databricks.jdbc.core.DatabricksResultSet;
import com.databricks.jdbc.core.IDatabricksResultSet;
import com.databricks.sdk.service.sql.ExecuteStatementRequest;
import com.databricks.sdk.service.sql.ExecuteStatementResponse;
import com.databricks.sdk.service.sql.Session;

import java.sql.SQLException;

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
   */
  void deleteSession(String sessionId);

  /**
   * Executes a statement in Databricks server
   * @param statement SQL statement that needs to be executed
   * @param sessionId underlying session-Id
   * @param warehouseId warehouse-Id which should be used for statement execution
   * @return response for statement execution
   */
  DatabricksResultSet executeStatement(String statement, String sessionId, String warehouseId, boolean isInternal) throws SQLException;

  /**
   * Closes a statement in Databricks server
   * @param statementId statement which should be closed
   * @return response for statement execution
   */
  void closeStatement(String statementId);
}
