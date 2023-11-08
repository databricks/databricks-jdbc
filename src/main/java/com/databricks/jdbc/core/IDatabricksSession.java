package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import javax.annotation.Nullable;

/** Session interface to represent an open connection to Databricks server. */
public interface IDatabricksSession {

  /**
   * Get the unique session-Id associated with the session.
   *
   * @return session-Id
   */
  @Nullable
  String getSessionId();

  /**
   * Get the warehouse associated with the session.
   *
   * @return warehouse-Id
   */
  String getWarehouseId();

  /**
   * Checks if session is open and valid.
   *
   * @return true if session is open
   */
  boolean isOpen();

  /** Opens a new session. */
  void open();

  /** Closes the session. */
  void close();

  /** Returns the client for connecting to Databricks server */
  DatabricksClient getDatabricksClient();

  /** Returns the metadata client */
  DatabricksMetadataClient getDatabricksMetadataClient();

  /** Returns default catalog associated with the session */
  String getCatalog();

  /** Returns default schema associated with the session */
  String getSchema();

  /** Sets the default catalog */
  void setCatalog(String catalog);

  /** Sets the default schema */
  void setSchema(String schema);
}
