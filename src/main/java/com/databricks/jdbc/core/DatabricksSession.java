package com.databricks.jdbc.core;

import com.databricks.jdbc.client.DatabricksClient;
import com.databricks.jdbc.client.DatabricksMetadataClient;
import com.databricks.jdbc.client.impl.sqlexec.DatabricksMetadataSdkClient;
import com.databricks.jdbc.client.impl.sqlexec.DatabricksNewMetadataSdkClient;
import com.databricks.jdbc.client.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.client.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.databricks.jdbc.telemetry.annotation.DatabricksMetricsTimedProcessor;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/** Implementation for Session interface, which maintains an underlying session in SQL Gateway. */
public class DatabricksSession implements IDatabricksSession {
  private DatabricksClient databricksClient;

  private DatabricksMetadataClient databricksMetadataSdkClient;
  private DatabricksMetadataClient databricksNewMetadataSdkClient;
  private DatabricksMetadataClient databricksMetadataClient;
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
      this.databricksMetadataSdkClient =
          new DatabricksMetadataSdkClient((DatabricksSdkClient) databricksClient);
      this.databricksNewMetadataSdkClient =
          new DatabricksNewMetadataSdkClient((DatabricksSdkClient) databricksClient);
    }

    this.databricksClient = DatabricksMetricsTimedProcessor.createProxy(this.databricksClient);

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

  @Override
  public void setMetadataClient(boolean useLegacyMetadataClient) {
    if (connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      return;
    }
    this.databricksMetadataClient =
        useLegacyMetadataClient
            ? this.databricksMetadataSdkClient
            : this.databricksNewMetadataSdkClient;
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
    LoggingUtil.log(LogLevel.DEBUG, "public String getSessionId()");
    return (isSessionOpen) ? sessionInfo.sessionId() : null;
  }

  @Override
  @Nullable
  public ImmutableSessionInfo getSessionInfo() {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSessionInfo()");
    return sessionInfo;
  }

  @Override
  public ComputeResource getComputeResource() {
    LoggingUtil.log(LogLevel.DEBUG, "public String getWarehouseId()");
    return this.computeResource;
  }

  @Override
  public CompressionType getCompressionType() {
    LoggingUtil.log(LogLevel.DEBUG, "public String getWarehouseId()");
    return compressionType;
  }

  @Override
  public boolean isOpen() {
    LoggingUtil.log(LogLevel.DEBUG, "public boolean isOpen()");
    // TODO: check for expired sessions
    return isSessionOpen;
  }

  @Override
  public void open() throws DatabricksSQLException {
    LoggingUtil.log(LogLevel.DEBUG, "public void open()");
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
    LoggingUtil.log(LogLevel.DEBUG, "public void close()");
    // TODO: check for any pending query executions
    synchronized (this) {
      if (isSessionOpen) {
        // TODO: handle closed connections by server
        databricksClient.deleteSession(this, computeResource);
        this.sessionInfo = null;
        this.isSessionOpen = false;
        if (!connectionContext.isFakeServiceTest()) {
          this.connectionContext.getMetricsExporter().close();
        }
      }
    }
  }

  @Override
  public DatabricksClient getDatabricksClient() {
    LoggingUtil.log(LogLevel.DEBUG, "public DatabricksClient getDatabricksClient()");
    return databricksClient;
  }

  @Override
  public DatabricksMetadataClient getDatabricksMetadataClient() {
    LoggingUtil.log(LogLevel.DEBUG, "public DatabricksClient getDatabricksMetadataClient()");
    if (this.connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      return (DatabricksMetadataClient) databricksClient;
    }
    return databricksMetadataClient;
  }

  @Override
  public String getCatalog() {
    LoggingUtil.log(LogLevel.DEBUG, "public String getCatalog()");
    return catalog;
  }

  @Override
  public void setCatalog(String catalog) {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setCatalog(String catalog = {%s})", catalog));
    this.catalog = catalog;
  }

  @Override
  public String getSchema() {
    LoggingUtil.log(LogLevel.DEBUG, "public String getSchema()");
    return schema;
  }

  @Override
  public void setSchema(String schema) {
    LoggingUtil.log(
        LogLevel.DEBUG, String.format("public void setSchema(String schema = {%s})", schema));
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
    LoggingUtil.log(LogLevel.DEBUG, "public Map<String, String> getSessionConfigs()");
    return sessionConfigs;
  }

  @Override
  public void setSessionConfig(String name, String value) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void setSessionConfig(String name = {%s}, String value = {%s})", name, value));
    sessionConfigs.put(name, value);
  }

  @Override
  public Map<String, String> getClientInfoProperties() {
    LoggingUtil.log(LogLevel.DEBUG, "public Map<String, String> getClientInfoProperties()");
    return clientInfoProperties;
  }

  @Override
  public void setClientInfoProperty(String name, String value) {
    LoggingUtil.log(
        LogLevel.DEBUG,
        String.format(
            "public void setClientInfoProperty(String name = {%s}, String value = {%s})",
            name, value));
    clientInfoProperties.put(name, value);
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return this.connectionContext;
  }
}
