package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;

import javax.annotation.Nullable;

/**
 * Session interface to represent an open connection to Databricks server.
 */
public interface IDatabricksSession {

  /**
   * Get the unique session-Id associated with the session.
   * @return session-Id
   */
  @Nullable
  String getSessionId();

  /**
   * Get the warehouse associated with the session.
   * @return warehouse-Id
   */
  String getWarehouseId();

  /**
   * Checks if session is open and valid.
   * @return true if session is open
   */
  boolean isOpen();

  /**
   * Opens a new session.
   */
  void open();

  /**
   * Closes the session.
   */
  void close();

  /**
   * Returns the client for connecting to Databricks server
   * @return
   */
  DatabricksClient getDatabricksClient();

  String getCatalog();

  String getSchema();

  void setCatalog(String catalog);

  void setSchema(String schema);
}
