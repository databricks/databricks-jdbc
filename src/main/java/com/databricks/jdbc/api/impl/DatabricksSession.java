package com.databricks.jdbc.api.impl;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.IDatabricksSession;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.DatabricksJdbcUrlParams;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksMetadataClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksEmptyMetadataClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksMetadataSdkClient;
import com.databricks.jdbc.dbclient.impl.sqlexec.DatabricksSdkClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.support.ToStringer;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Implementation of {@link IDatabricksSession}, which maintains an underlying session in SQL
 * Gateway.
 */
public class DatabricksSession implements IDatabricksSession {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksSession.class);
  private IDatabricksClient databricksClient;
  private IDatabricksMetadataClient databricksMetadataClient;
  private final IDatabricksComputeResource computeResource;
  private boolean isSessionOpen;
  private ImmutableSessionInfo sessionInfo;

  /** For context based commands */
  private String catalog;

  private String schema;
  private final Map<String, String> sessionConfigs;
  private final Map<String, String> clientInfoProperties;
  private final CompressionCodec compressionCodec;
  private final IDatabricksConnectionContext connectionContext;

  /**
   * Creates an instance of Databricks session for given connection context
   *
   * @param connectionContext underlying connection context
   */
  public DatabricksSession(IDatabricksConnectionContext connectionContext)
      throws DatabricksSQLException {
    if (connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      this.databricksClient = new DatabricksThriftServiceClient(connectionContext);
    } else {
      this.databricksClient = new DatabricksSdkClient(connectionContext);
      this.databricksMetadataClient = new DatabricksMetadataSdkClient(databricksClient);
    }
    this.isSessionOpen = false;
    this.sessionInfo = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.clientInfoProperties = new HashMap<>();
    this.compressionCodec = connectionContext.getCompressionCodec();
    this.connectionContext = connectionContext;
  }

  /** Constructor method to be used for mocking in a test case. */
  @VisibleForTesting
  public DatabricksSession(
      IDatabricksConnectionContext connectionContext, IDatabricksClient testDatabricksClient) {
    this.databricksClient = testDatabricksClient;
    if (databricksClient instanceof DatabricksSdkClient) {
      this.databricksMetadataClient = new DatabricksMetadataSdkClient(databricksClient);
    }
    this.isSessionOpen = false;
    this.sessionInfo = null;
    this.computeResource = connectionContext.getComputeResource();
    this.catalog = connectionContext.getCatalog();
    this.schema = connectionContext.getSchema();
    this.sessionConfigs = connectionContext.getSessionConfigs();
    this.clientInfoProperties = new HashMap<>();
    this.compressionCodec = connectionContext.getCompressionCodec();
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
  public IDatabricksComputeResource getComputeResource() {
    LOGGER.debug("public String getComputeResource()");
    return this.computeResource;
  }

  @Override
  public CompressionCodec getCompressionCodec() {
    LOGGER.debug("public String getCompressionType()");
    return compressionCodec;
  }

  @Override
  public boolean isOpen() {
    LOGGER.debug("public boolean isOpen()");
    // TODO (PECO-1949): Check for expired sessions
    return isSessionOpen;
  }

  @Override
  public void open() throws DatabricksSQLException {
    LOGGER.debug("public void open()");
    synchronized (this) {
      if (!isSessionOpen) {
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
    synchronized (this) {
      if (isSessionOpen) {
        databricksClient.deleteSession(this, computeResource);
        this.sessionInfo = null;
        this.isSessionOpen = false;
      }
    }
  }

  @Override
  public IDatabricksClient getDatabricksClient() {
    LOGGER.debug("public IDatabricksClient getDatabricksClient()");
    return databricksClient;
  }

  @Override
  public IDatabricksMetadataClient getDatabricksMetadataClient() {
    LOGGER.debug("public IDatabricksClient getDatabricksMetadataClient()");
    if (this.connectionContext.getClientType() == DatabricksClientType.THRIFT) {
      return (IDatabricksMetadataClient) databricksClient;
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
    LOGGER.debug(String.format("public void setCatalog(String catalog = {%s})", catalog));
    this.catalog = catalog;
  }

  @Override
  public String getSchema() {
    LOGGER.debug("public String getSchema()");
    return schema;
  }

  @Override
  public void setSchema(String schema) {
    LOGGER.debug(String.format("public void setSchema(String schema = {%s})", schema));
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
    LOGGER.debug(
        String.format(
            "public void setSessionConfig(String name = {%s}, String value = {%s})", name, value));
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
        String.format(
            "public void setClientInfoProperty(String name = {%s}, String value = {%s})",
            name, value));
    if (name.equalsIgnoreCase(DatabricksJdbcUrlParams.AUTH_ACCESS_TOKEN.getParamName())) {
      // refresh the access token if provided a new value in client info
      this.databricksClient.resetAccessToken(value);
    } else {
      clientInfoProperties.put(name, value);
    }
  }

  @Override
  public IDatabricksConnectionContext getConnectionContext() {
    return this.connectionContext;
  }

  @Override
  public void setEmptyMetadataClient() {
    databricksMetadataClient = new DatabricksEmptyMetadataClient();
  }
}
