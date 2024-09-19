package com.databricks.jdbc.dbclient.impl.common;

import static com.databricks.jdbc.common.DatabricksJdbcConstants.EMPTY_STRING;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.OAuthRefreshCredentialsProvider;
import com.databricks.jdbc.auth.PrivateKeyClientCredentialProvider;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.WorkspaceClient;
import com.databricks.sdk.core.CredentialsProvider;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.ProxyConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import java.io.FileInputStream;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

/**
 * This class is responsible for configuring the Databricks config based on the connection context.
 * The databricks config is then used to create the SDK or Thrift client.
 */
public class ClientConfigurator {

  public static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ClientConfigurator.class);
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

  /**
   * Setup the SSL configuration in the httpClientBuilder.
   *
   * @param httpClientBuilder The builder to which the SSL configuration should be added.
   */
  private void setupSSLConfig(CommonsHttpClient.Builder httpClientBuilder) {
    if (this.connectionContext.getSSLTrustStore() == null) {
      return;
    }
    PoolingHttpClientConnectionManager connManager =
        new PoolingHttpClientConnectionManager(
            getConnectionSocketFactoryRegistry(this.connectionContext));
    // This is consistent with the value in the SDK
    connManager.setMaxTotal(100);
    httpClientBuilder.withConnectionManager(connManager);
  }

  /**
   * This function returns the registry of connection socket factories based on the truststore in
   * the connection context.
   *
   * @param connectionContext The connection context to use to get the truststore.
   * @return The registry of connection socket factories.
   */
  public static Registry<ConnectionSocketFactory> getConnectionSocketFactoryRegistry(
      IDatabricksConnectionContext connectionContext) {
    try {
      TrustManagerFactory trustManagerFactory;
      try (FileInputStream trustStoreStream =
          new FileInputStream(connectionContext.getSSLTrustStore())) {
        char[] password = null;
        if (connectionContext.getSSLTrustStorePassword() != null) {
          password = connectionContext.getSSLTrustStorePassword().toCharArray();
        }
        KeyStore trustStore = KeyStore.getInstance(connectionContext.getSSLTrustStoreType());
        trustStore.load(trustStoreStream, password);
        trustManagerFactory =
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustStore);
      }
      SSLContext sslContext = SSLContext.getInstance(DatabricksJdbcConstants.TLS);
      sslContext.init(null, trustManagerFactory.getTrustManagers(), null);

      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register(DatabricksJdbcConstants.HTTPS, sslSocketFactory)
          .register(DatabricksJdbcConstants.HTTP, new PlainConnectionSocketFactory())
          .build();
    } catch (Exception e) {
      String errorMessage = "Error while loading truststore to set up SSL configuration.";
      LOGGER.error(errorMessage, e);
      throw new DatabricksException(errorMessage, e);
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
          .setProxyAuthType(connectionContext.getProxyAuthType())
          .setNonProxyHosts(
              convertNonProxyHostConfigToBeSystemPropertyCompliant(
                  connectionContext.getNonProxyHosts()));
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
      LOGGER.error(errorMessage);
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
          setupOAuthAccessTokenConfig();
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

  public void setupOAuthAccessTokenConfig() throws DatabricksParsingException {
    databricksConfig
        .setAuthType(DatabricksJdbcConstants.ACCESS_TOKEN_AUTH_TYPE)
        .setHost(connectionContext.getHostUrl())
        .setToken(connectionContext.getPassThroughAccessToken());
  }

  public void resetAccessTokenInConfig(String newAccessToken) {
    databricksConfig.setToken(newAccessToken);
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

  /**
   * Currently, the ODBC driver takes in nonProxyHosts as a comma separated list of suffix of
   * non-proxy hosts i.e. suffix1|suffix2|suffix3. Whereas, the SDK takes in nonProxyHosts as a list
   * of patterns separated by '|'. This pattern conforms to the system property format in the Java
   * Proxy Guide.
   *
   * @param nonProxyHosts Comma separated list of suffix of non-proxy hosts
   * @return nonProxyHosts in system property compliant format from <a
   *     href="https://docs.oracle.com/javase/8/docs/technotes/guides/net/proxies.html">Java Proxy
   *     Guide</a>
   */
  public static String convertNonProxyHostConfigToBeSystemPropertyCompliant(String nonProxyHosts) {
    if (nonProxyHosts == null || nonProxyHosts.isEmpty()) {
      return EMPTY_STRING;
    }
    if (nonProxyHosts.contains(DatabricksJdbcConstants.PIPE)) {
      // Already in system property compliant format
      return nonProxyHosts;
    }
    return Arrays.stream(nonProxyHosts.split(DatabricksJdbcConstants.COMMA))
        .map(
            suffix -> {
              if (suffix.startsWith(DatabricksJdbcConstants.FULL_STOP)) {
                return DatabricksJdbcConstants.ASTERISK + suffix;
              }
              return suffix;
            })
        .collect(Collectors.joining(DatabricksJdbcConstants.PIPE));
  }

  public DatabricksConfig getDatabricksConfig() {
    return this.databricksConfig;
  }
}
