package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksNewMetadataSdkClient;
import com.databricks.jdbc.client.impl.sdk.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/** Implementation for Session interface, which maintains an underlying session in SQL Gateway. */
public class DatabricksSession implements IDatabricksSession {

  private static final Logger LOGGER = LogManager.getLogger(DatabricksSession.class);
  private final DatabricksClient databricksClient;
  private final DatabricksMetadataClient databricksMetadataClient;
  private final ComputeResource computeResource;

  private boolean isSessionOpen;
  private ImmutableSessionInfo sessionInfo;

  // For context based commands
  private String catalog;

  private String schema;

  private Map<String, String> sessionConfigs;

  private Map<String, String> clientInfoProperties;
  private CompressionType compressionType;

  private IDatabricksConnectionContext connectionContext;

  /**
   * Creates an instance of Databricks session for given connection context
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksSession(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    if (connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      this.databricksClient = new DatabricksThriftServiceClient(connectionContext);
      this.databricksMetadataClient = null;
    } else {
      this.databricksClient = new DatabricksSdkClient(connectionContext);
      if (connectionContext.getUseLegacyMetadata()) {
        this.databricksMetadataClient =
            new DatabricksMetadataSdkClient((DatabricksSdkClient) databricksClient);
      } else {
        this.databricksMetadataClient =
            new DatabricksNewMetadataSdkClient((DatabricksSdkClient) databricksClient);
      }
    }
    this.isSessionOpen = false;
    this.sessionInfo = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.clientInfoProperties = new HashMap<>();
    this.compressionType = connectionContext.getCompressionType();
    this.connectionContext = connectionContext;
  }

  /** Constructor method to be used for mocking in a test case. */
  @VisibleForTesting
  DatabricksSession(
      IDatabricksConnectionContext connectionContext, DatabricksClient databricksClient)
      throws DatabricksSQLException {
    this.databricksClient = databricksClient;
    if (databricksClient instanceof DatabricksThriftServiceClient) {
      this.databricksMetadataClient = null;
    } else {
      this.databricksMetadataClient =
          new DatabricksMetadataSdkClient((DatabricksSdkClient) databricksClient);
    }
    this.isSessionOpen = false;
    this.sessionInfo = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.clientInfoProperties = new HashMap<>();
    this.compressionType = connectionContext.getCompressionType();
    this.connectionContext = connectionContext;
  }

  @Nullable
  @Override
  public String getSessionId() {
    LOGGER.debug("public String getSessionId()");
    return (isSessionOpen) ? sessionInfo.sessionId() : null;
  }

  @Override
  @Nullable
  public ImmutableSessionInfo getSessionInfo() {
    LOGGER.debug("public String getSessionInfo()");
    return sessionInfo;
  }

  @Override
  public ComputeResource getComputeResource() {
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
  public void open() throws DatabricksSQLException {
    LOGGER.debug("public void open()");
    // TODO: check for expired sessions
    synchronized (this) {
      if (!isSessionOpen) {
        // TODO: handle errors
        this.sessionInfo =
            databricksClient.createSession(
                this.computeResource, this.catalog, this.schema, this.sessionConfigs);
        this.isSessionOpen = true;
      }
    }
  }

  @Override
  public void close() throws DatabricksSQLException {
    LOGGER.debug("public void close()");
    // TODO: check for any pending query executions
    synchronized (this) {
      if (isSessionOpen) {
        // TODO: handle closed connections by server
        databricksClient.deleteSession(this, computeResource);
        this.sessionInfo = null;
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
    if (this.connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      return (DatabricksMetadataClient) databricksClient;
    }
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
  public Map<String, String> getClientInfoProperties() {
    LOGGER.debug("public Map<String, String> getClientInfoProperties()");
    return clientInfoProperties;
  }

  @Override
  public void setClientInfoProperty(String name, String value) {
    LOGGER.debug(
        "public void setClientInfoProperty(String name = {}, String value = {})", name, value);
    clientInfoProperties.put(name, value);
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return this.connectionContext;
  }
}
