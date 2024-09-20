package com.databricks.jdbc.api;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.common.CompressionType;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
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
  void open() throws DatabricksSQLException;

  /** Closes the session. */
  void close() throws DatabricksSQLException;

  /** Returns the client for connecting to Databricks server */
  IDatabricksClient getDatabricksClient();

  /** Returns the metadata client */
  IDatabricksMetadataClient getDatabricksMetadataClient();

  /** Returns default catalog associated with the session */
  String getCatalog();

  /** Returns the compression algorithm used on results data */
  CompressionType getCompressionType();

  /** Returns default schema associated with the session */
  String getSchema();

  /** Sets the default catalog */
  void setCatalog(String catalog);

  /** Sets the default schema */
  void setSchema(String schema);

  /** Extracts session to a string */
  String toString();

  /** Returns the session configs */
  Map<String, String> getSessionConfigs();

  /** Sets the session config */
  void setSessionConfig(String name, String value);

  /** Returns the client info properties */
  Map<String, String> getClientInfoProperties();

  /** Sets the client info property */
  void setClientInfoProperty(String name, String value);

  /** Returns the associated connection context for the session */
  IDatabricksConnectionContext getConnectionContext();

  void setEmptyMetadataClient();

  /** Returns the metrics exporter for the session */
  DatabricksMetrics getMetricsExporter();
}
