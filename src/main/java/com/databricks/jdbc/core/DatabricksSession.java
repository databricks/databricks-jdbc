package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.thrift.DatabricksThriftClient;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.core.types.Warehouse;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Implementation for Session interface, which maintains an underlying session in SQL Gateway. */
public class DatabricksSession implements IDatabricksSession {

  private static final Logger LOGGER = LoggerFactory.getLogger(DatabricksSession.class);
  private final DatabricksClient databricksClient;
  private final DatabricksMetadataClient databricksMetadataClient;
  private final ComputeResource computeResource;

  private boolean isSessionOpen;
  private ImmutableSessionInfo session;

  // For context based commands
  private String catalog;

  private String schema;

  private Map<String, String> sessionConfigs;
  private CompressionType compressionType;

  private IDatabricksConnectionContext connectionContext;

  /**
   * Creates an instance of Databricks session for given connection context
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksSession(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    if (connectionContext.isAllPurposeCluster()) {
      this.databricksClient = new DatabricksThriftClient(connectionContext);
    } else {
      this.databricksClient = new DatabricksSdkClient(connectionContext);
    }
    this.databricksMetadataClient =
        new DatabricksMetadataSdkClient((DatabricksSdkClient) databricksClient);
    this.isSessionOpen = false;
    this.session = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.compressionType = connectionContext.getCompressionType();
    this.connectionContext = connectionContext;
  }

  /** Construct method to be used for mocking in a test case. */
  @VisibleForTesting
  DatabricksSession(
      IDatabricksConnectionContext connectionContext, DatabricksClient databricksClient)
      throws DatabricksSQLException {
    this.databricksClient = databricksClient;
    this.databricksMetadataClient =
        new DatabricksMetadataSdkClient((DatabricksSdkClient) databricksClient);
    this.isSessionOpen = false;
    this.session = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.compressionType = connectionContext.getCompressionType();
    this.connectionContext = connectionContext;
  }

  @Override
  @Nullable
  public String getSessionId() {
    LOGGER.debug("public String getSessionId()");
    return isSessionOpen ? session.sessionId() : null;
  }

  @Override
  public ComputeResource getComputeResource() throws DatabricksSQLException {
    LOGGER.debug("public String getWarehouseId()");
    return this.computeResource;
  }

  @Override
  public CompressionType getCompressionType() {
    LOGGER.debug("public String getWarehouseId()");
    return compressionType;
  }

  @Override
  public boolean isOpen() {
    LOGGER.debug("public boolean isOpen()");
    // TODO: check for expired sessions
    return isSessionOpen;
  }

  @Override
  public void open() {
    LOGGER.debug("public void open()");
    // TODO: check for expired sessions
    synchronized (this) {
      if (!isSessionOpen) {
        // TODO: handle errors
        this.session =
            databricksClient.createSession(
                this.computeResource, this.catalog, this.schema, this.sessionConfigs);
        this.isSessionOpen = true;
      }
    }
  }

  @Override
  public void close() {
    LOGGER.debug("public void close()");
    // TODO: check for any pending query executions
    synchronized (this) {
      if (isSessionOpen) {
        // TODO: handle closed connections by server
        if (computeResource instanceof Warehouse) {
          databricksClient.deleteSession(this.session.sessionId(), computeResource);
        } else {

        }
        this.session = null;
        this.isSessionOpen = false;
      }
    }
  }

  @Override
  public DatabricksClient getDatabricksClient() {
    LOGGER.debug("public DatabricksClient getDatabricksClient()");
    return databricksClient;
  }

  @Override
  public DatabricksMetadataClient getDatabricksMetadataClient() {
    LOGGER.debug("public DatabricksClient getDatabricksMetadataClient()");
    return databricksMetadataClient;
  }

  @Override
  public String getCatalog() {
    LOGGER.debug("public String getCatalog()");
    return catalog;
  }

  @Override
  public void setCatalog(String catalog) {
    LOGGER.debug("public void setCatalog(String catalog = {})", catalog);
    this.catalog = catalog;
  }

  @Override
  public String getSchema() {
    LOGGER.debug("public String getSchema()");
    return schema;
  }

  @Override
  public void setSchema(String schema) {
    LOGGER.debug("public void setSchema(String schema = {})", schema);
    this.schema = schema;
  }

  @Override
  public String toString() {
    return (new ToStringer(DatabricksSession.class))
        .add("compute", this.computeResource.toString())
        .add("catalog", this.catalog)
        .add("schema", this.schema)
        .add("sessionID", this.getSessionId())
        .toString();
  }

  @Override
  public Map<String, String> getSessionConfigs() {
    LOGGER.debug("public Map<String, String> getSessionConfigs()");
    return sessionConfigs;
  }

  @Override
  public void setSessionConfig(String name, String value) {
    LOGGER.debug("public void setSessionConfig(String name = {}, String value = {})", name, value);
    sessionConfigs.put(name, value);
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return this.connectionContext;
  }
}
