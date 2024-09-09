package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.OAuthRefreshCredentialsProvider;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.LogLevel;
import com.databricks.jdbc.common.util.LoggingUtil;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.FileInputStream;
import java.security.KeyStore;

/**
 * This class is responsible for configuring the Databricks config based on the connection context.
 * The databricks config is then used to create the SDK or Thrift client.
 */
public class ClientConfigurator {
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;

  public ClientConfigurator(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.databricksConfig = new DatabricksConfig();
    CommonsHttpClient.Builder httpClientBuilder = new CommonsHttpClient.Builder();
    setupProxyConfig(httpClientBuilder);
    setupSSLConfig(httpClientBuilder);
    setupAuthConfig();
    this.databricksConfig.setHttpClient(httpClientBuilder.build()).resolve();
  }

  private void setupSSLConfig(CommonsHttpClient.Builder httpClientBuilder) {
    if (this.connectionContext.getSSLTrustStore() == null) {
      return;
    }
    TrustManagerFactory trustManagerFactory;
    try {
      KeyStore trustStore = KeyStore.getInstance(this.connectionContext.getSSLTrustStoreType());
      try (FileInputStream trustStoreStream =
          new FileInputStream(this.connectionContext.getSSLTrustStore())) {
        char[] password = null;
        if (this.connectionContext.getSSLTrustStorePassword() != null) {
          password = this.connectionContext.getSSLTrustStorePassword().toCharArray();
        }
        trustStore.load(trustStoreStream, password);
        trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
      }
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustManagerFactory.getTrustManagers(), null);


      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
      Registry<ConnectionSocketFactory> socketFactoryRegistry =
              RegistryBuilder.<ConnectionSocketFactory>create()
                      .register("https", sslSocketFactory)
                      .register("http", new PlainConnectionSocketFactory())
                      .build();
      PoolingHttpClientConnectionManager connManager = new PoolingHttpClientConnectionManager(socketFactoryRegistry);
      connManager.setMaxTotal(100);
      httpClientBuilder.withConnectionManager(connManager);
    } catch (Exception e) {
      throw new DatabricksException("Error while loading truststore", e);
    }
  }

  /** Setup proxy settings in the databricks config. */
  public void setupProxyConfig(CommonsHttpClient.Builder httpClientBuilder) {
    ProxyConfig proxyConfig =
        new ProxyConfig().setUseSystemProperties(connectionContext.getUseSystemProxy());
    if (connectionContext.getUseProxy()) {
      proxyConfig
          .setHost(connectionContext.getProxyHost())
          .setPort(connectionContext.getProxyPort());
    }
    if (connectionContext.getUseProxy() || connectionContext.getUseSystemProxy()) {
      proxyConfig
          .setUsername(connectionContext.getProxyUser())
          .setPassword(connectionContext.getProxyPassword())
          .setProxyAuthType(connectionContext.getProxyAuthType());
    }
    httpClientBuilder.withProxyConfig(proxyConfig);
  }

  public WorkspaceClient getWorkspaceClient() {
    return new WorkspaceClient(databricksConfig);
  }

  /** Setup the workspace authentication settings in the databricks config. */
  public void setupAuthConfig() {
    IDatabricksConnectionContext.AuthMech authMech = connectionContext.getAuthMech();
    try {
      switch (authMech) {
        case OAUTH:
          setupOAuthConfig();
          break;
        case PAT:
        default:
          setupAccessTokenConfig();
      }
    } catch (DatabricksParsingException e) {
      String errorMessage = "Error while parsing auth config";
      LoggingUtil.log(LogLevel.ERROR, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /** Setup the OAuth authentication settings in the databricks config. */
  public void setupOAuthConfig() throws DatabricksParsingException {
    // TODO(Madhav): Revisit these to set JDBC values
    switch (this.connectionContext.getAuthFlow()) {
      case TOKEN_PASSTHROUGH:
        if (connectionContext.getOAuthRefreshToken() != null) {
          setupU2MRefreshConfig();
        } else {
          setupAccessTokenConfig();
        }
        break;
      case CLIENT_CREDENTIALS:
        setupM2MConfig();
        break;
      case BROWSER_BASED_AUTHENTICATION:
        setupU2MConfig();
        break;
    }
  }

  /** Setup the OAuth U2M authentication settings in the databricks config. */
  public void setupU2MConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.U2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret())
        .setOAuthRedirectUrl(DatabricksJdbcConstants.U2M_AUTH_REDIRECT_URL);
    if (!databricksConfig.isAzure()) {
      databricksConfig.setScopes(connectionContext.getOAuthScopesForU2M());
    }
  }

  /** Setup the PAT authentication settings in the databricks config. */
  public void setupAccessTokenConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getToken());
  }

  /** Setup the OAuth U2M refresh token authentication settings in the databricks config. */
  public void setupU2MRefreshConfig() throws DatabricksParsingException {
    CredentialsProvider provider = new OAuthRefreshCredentialsProvider(connectionContext);
    databricksConfig
        .setHost(connectionContext.getHostForOAuth())
        .setAuthType(provider.authType())
        .setCredentialsProvider(provider)
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
  }

  /** Setup the OAuth M2M authentication settings in the databricks config. */
  public void setupM2MConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.M2M_AUTH_TYPE)
        .setHost(connectionContext.getHostForOAuth())
        .setClientId(connectionContext.getClientId())
        .setClientSecret(connectionContext.getClientSecret());
    if (connectionContext.useJWTAssertion()) {
      databricksConfig.setCredentialsProvider(
          new PrivateKeyClientCredentialProvider(connectionContext));
    }
  }

  public DatabricksConfig getDatabricksConfig() {
    return this.databricksConfig;
  }
}
