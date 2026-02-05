package com.databricks.jdbc.api.internal;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import java.sql.SQLException;
import java.util.Map;
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

  @Nullable
  ImmutableSessionInfo getSessionInfo();

  /**
   * Get the warehouse associated with the session.
   *
   * @return warehouse-Id
   */
  IDatabricksComputeResource getComputeResource() throws DatabricksSQLException;

  /**
   * Checks if session is open and valid.
   *
   * @return true if session is open
   */
  boolean isOpen();

  /** Opens a new session. */
  void open() throws SQLException;

  /** Closes the session. */
  void close() throws SQLException;

  /**
   * Returns the client for connecting to Databricks server
   *
   * @return the Databricks client
   */
  IDatabricksClient getDatabricksClient();

  /**
   * Returns the metadata client
   *
   * @return the Databricks metadata client
   */
  IDatabricksMetadataClient getDatabricksMetadataClient();

  /**
   * Returns default catalog associated with the session
   *
   * @return the default catalog
   */
  String getCatalog();

  /**
   * Returns the compression algorithm used on results data
   *
   * @return the compression codec
   */
  CompressionCodec getCompressionCodec();

  /**
   * Returns default schema associated with the session
   *
   * @return the default schema
   */
  String getSchema();

  /**
   * Sets the default catalog
   *
   * @param catalog the catalog to set
   */
  void setCatalog(String catalog);

  /**
   * Sets the default schema
   *
   * @param schema the schema to set
   */
  void setSchema(String schema);

  /** Extracts session to a string */
  String toString();

  /**
   * Returns the session configs
   *
   * @return map of session configuration key-value pairs
   */
  Map<String, String> getSessionConfigs();

  /**
   * Sets the session config
   *
   * @param name the configuration name
   * @param value the configuration value
   */
  void setSessionConfig(String name, String value);

  /** Returns the client info properties */
  Map<String, String> getClientInfoProperties();

  String getConfigValue(String name);

  /** Sets the client info property */
  void setClientInfoProperty(String name, String value);

  /** Returns the associated connection context for the session */
  IDatabricksConnectionContext getConnectionContext();

  /** Gets the current catalog from the database */
  String getCurrentCatalog() throws DatabricksSQLException;

  void setEmptyMetadataClient();

  void forceClose();

  /**
   * Sets the auto-commit mode for this session.
   *
   * @param autoCommit true to enable auto-commit; false to disable
   */
  void setAutoCommit(boolean autoCommit);

  /**
   * Retrieves the current auto-commit mode for this session.
   *
   * @return true if auto-commit mode is enabled; false otherwise
   */
  boolean getAutoCommit();
}
