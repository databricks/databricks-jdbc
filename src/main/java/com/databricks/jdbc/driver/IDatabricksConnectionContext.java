package com.databricks.jdbc.driver;

import com.databricks.jdbc.client.DatabricksClientType;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.core.DatabricksParsingException;
import com.databricks.jdbc.core.types.CompressionType;
import com.databricks.jdbc.core.types.ComputeResource;
import com.databricks.jdbc.telemetry.DatabricksMetrics;
import com.databricks.sdk.core.ProxyConfig;
import java.util.List;
import java.util.Map;

public interface IDatabricksConnectionContext {

  enum AuthFlow {
    TOKEN_PASSTHROUGH,
    CLIENT_CREDENTIALS,
    BROWSER_BASED_AUTHENTICATION
  }

  enum AuthMech {
    OTHER,
    PAT,
    OAUTH;

    public static AuthMech parseAuthMech(String authMech) {
      int authMechValue = Integer.parseInt(authMech);
      switch (authMechValue) {
        case 3:
          return AuthMech.PAT;
        case 11:
          return AuthMech.OAUTH;
        default:
          throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Returns host-Url for Databricks server as parsed from JDBC connection in format
   * https://server:port
   *
   * @return Databricks host-Url
   */
  String getHostUrl() throws DatabricksParsingException;

  /**
   * Returns warehouse-Id as parsed from JDBC connection Url
   *
   * @return warehouse-Id
   */
  ComputeResource getComputeResource();

  /**
   * Returns the auth token (personal access token/OAuth token etc)
   *
   * @return auth token
   */
  String getToken();

  String getHostForOAuth();

  String getClientId() throws DatabricksParsingException;

  String getClientSecret();

  List<String> getOAuthScopesForU2M() throws DatabricksParsingException;

  AuthMech getAuthMech();

  AuthFlow getAuthFlow();

  LogLevel getLogLevel();

  String getLogPathString();

  int getLogFileSize();

  int getLogFileCount();

  String getClientUserAgent();

  CompressionType getCompressionType();

  String getCatalog();

  String getSchema();

  Map<String, String> getSessionConfigs();

  boolean isAllPurposeCluster();

  String getHttpPath();

  String getProxyHost();

  int getProxyPort();

  String getProxyUser();

  String getProxyPassword();

  Boolean getUseProxy();

  ProxyConfig.ProxyAuthType getProxyAuthType();

  Boolean getUseSystemProxy();

  Boolean getUseCloudFetchProxy();

  String getCloudFetchProxyHost();

  int getCloudFetchProxyPort();

  String getCloudFetchProxyUser();

  String getCloudFetchProxyPassword();

  ProxyConfig.ProxyAuthType getCloudFetchProxyAuthType();

  String getEndpointURL() throws DatabricksParsingException;

  int getAsyncExecPollInterval();

  Boolean shouldEnableArrow();

  DatabricksClientType getClientType();

  Boolean getUseLegacyMetadata();

  /** Returns the number of threads to be used for fetching data from cloud storage */
  int getCloudFetchThreadPoolSize();

  Boolean getDirectResultMode();

  DatabricksMetrics getMetricsExporter();

  Boolean shouldRetryTemporarilyUnavailableError();

  Boolean shouldRetryRateLimitError();

  int getTemporarilyUnavailableRetryTimeout();

  int getRateLimitRetryTimeout();

  int getIdleHttpConnectionExpiry();

  boolean supportManyParameters();

  boolean isFakeServiceTest();

  boolean enableTelemetry();

  String getConnectionURL();
}
