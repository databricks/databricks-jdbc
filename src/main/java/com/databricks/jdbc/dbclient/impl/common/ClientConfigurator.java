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
import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.cert.*;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;
import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
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
    try {
      PoolingHttpClientConnectionManager connManager =
          new PoolingHttpClientConnectionManager(
              getConnectionSocketFactoryRegistry(this.connectionContext));
      connManager.setMaxTotal(100);
      httpClientBuilder.withConnectionManager(connManager);
    } catch (Exception e) {
      throw new DatabricksException("Error while loading truststore", e);
    }
  }

  public static Registry<ConnectionSocketFactory> getConnectionSocketFactoryRegistry(
      IDatabricksConnectionContext connectionContext) {
    // if truststore is not provided, null will use default truststore
    KeyStore trustStore = null;
    if (connectionContext.getSSLTrustStore() != null) {
      // Flow to provide custom SSL truststore
      try {
        try (FileInputStream trustStoreStream =
            new FileInputStream(connectionContext.getSSLTrustStore())) {
          char[] password = null;
          if (connectionContext.getSSLTrustStorePassword() != null) {
            password = connectionContext.getSSLTrustStorePassword().toCharArray();
          }
          trustStore = KeyStore.getInstance(connectionContext.getSSLTrustStoreType());
          trustStore.load(trustStoreStream, password);
        }
      } catch (Exception e) {
        throw new DatabricksException("Error while loading truststore", e);
      }
    }
    Set<TrustAnchor> trustAnchors = getTrustAnchorsFromTrustStore(trustStore);

    // Build custom TrustManager based on above SSL trust store and certificate revocation settings
    // from context
    try {
      PKIXBuilderParameters pkixBuilderParameters =
          new PKIXBuilderParameters(trustAnchors, new X509CertSelector());
      pkixBuilderParameters.setRevocationEnabled(connectionContext.checkCertificateRevocation());
      if (connectionContext.acceptUndeterminedCertificateRevocation()) {
        CertPathValidator certPathValidator = CertPathValidator.getInstance("PKIX");
        PKIXRevocationChecker revocationChecker =
            (PKIXRevocationChecker) certPathValidator.getRevocationChecker();
        revocationChecker.setOptions(
            Set.of(
                PKIXRevocationChecker.Option.SOFT_FAIL,
                PKIXRevocationChecker.Option.NO_FALLBACK,
                PKIXRevocationChecker.Option.PREFER_CRLS));
        pkixBuilderParameters.addCertPathChecker(revocationChecker);
      }
      CertPathTrustManagerParameters trustManagerParameters =
          new CertPathTrustManagerParameters(pkixBuilderParameters);
      TrustManagerFactory customTrustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      customTrustManagerFactory.init(trustManagerParameters);
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, customTrustManagerFactory.getTrustManagers(), null);
      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("https", sslSocketFactory)
          .register("http", new PlainConnectionSocketFactory())
          .build();
    } catch (Exception e) {
      throw new DatabricksException("Error while building trust manager parameters", e);
    }
  }

  public static Set<TrustAnchor> getTrustAnchorsFromTrustStore(KeyStore trustStore) {
    try {
      TrustManagerFactory trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);
      X509TrustManager trustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];
      X509Certificate[] certs = trustManager.getAcceptedIssuers();
      return Arrays.stream(certs)
          .map(cert -> new TrustAnchor(cert, null))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      throw new DatabricksException("Error while getting trust anchors from trust store", e);
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
