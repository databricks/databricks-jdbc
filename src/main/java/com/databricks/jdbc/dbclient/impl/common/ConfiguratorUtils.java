package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import java.io.FileInputStream;
import java.security.InvalidAlgorithmParameterException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
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

/** This class contains the utility functions for configuring a client. */
public class ConfiguratorUtils {
  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(ConfiguratorUtils.class);

  /**
   * @param connectionContext The connection context to use to get the truststore and properties.
   * @return The connection manager based on the truststore and properties set in the connection
   */
  public static PoolingHttpClientConnectionManager getBaseConnectionManager(
      IDatabricksConnectionContext connectionContext) {
    if (connectionContext.getSSLTrustStore() == null
        && connectionContext.checkCertificateRevocation()
        && !connectionContext.acceptUndeterminedCertificateRevocation()) {
      return new PoolingHttpClientConnectionManager();
    }
    Registry<ConnectionSocketFactory> socketFactoryRegistry =
        ConfiguratorUtils.getConnectionSocketFactoryRegistry(connectionContext);
    return new PoolingHttpClientConnectionManager(socketFactoryRegistry);
  }

  /**
   * This function returns the registry of connection socket factories based on the truststore and
   * properties set in the connection context.
   *
   * @param connectionContext The connection context to use to get the truststore, certificate
   *     revocation settings.
   * @return The registry of connection socket factories.
   */
  public static Registry<ConnectionSocketFactory> getConnectionSocketFactoryRegistry(
      IDatabricksConnectionContext connectionContext) {
    // if truststore is not provided, null will use default truststore
    KeyStore trustStore = loadTruststoreOrNull(connectionContext);
    Set<TrustAnchor> trustAnchors = getTrustAnchorsFromTrustStore(trustStore);
    // Build custom TrustManager based on above SSL trust store and certificate revocation settings
    // from context
    try {
      TrustManagerFactory customTrustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      // Custom trust store and certificate revocation parameters are provided
      CertPathTrustManagerParameters trustManagerParameters =
          buildTrustManagerParameters(
              trustAnchors,
              connectionContext.checkCertificateRevocation(),
              connectionContext.acceptUndeterminedCertificateRevocation());
      customTrustManagerFactory.init(trustManagerParameters);
      SSLContext sslContext = SSLContext.getInstance(DatabricksJdbcConstants.TLS);
      sslContext.init(null, customTrustManagerFactory.getTrustManagers(), null);
      SSLConnectionSocketFactory sslSocketFactory = new SSLConnectionSocketFactory(sslContext);
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register(DatabricksJdbcConstants.HTTPS, sslSocketFactory)
          .register(DatabricksJdbcConstants.HTTP, new PlainConnectionSocketFactory())
          .build();
    } catch (Exception e) {
      String errorMessage = "Error while building trust manager parameters";
      LOGGER.error(e, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /**
   * @param connectionContext The connection context to use to get the truststore.
   * @return The truststore loaded from the connection context or null if the truststore is not set.
   */
  public static KeyStore loadTruststoreOrNull(IDatabricksConnectionContext connectionContext) {
    if (connectionContext.getSSLTrustStore() == null) {
      return null;
    }
    // Flow to provide custom SSL truststore
    try {
      try (FileInputStream trustStoreStream =
          new FileInputStream(connectionContext.getSSLTrustStore())) {
        char[] password = null;
        if (connectionContext.getSSLTrustStorePassword() != null) {
          password = connectionContext.getSSLTrustStorePassword().toCharArray();
        }
        KeyStore trustStore = KeyStore.getInstance(connectionContext.getSSLTrustStoreType());
        trustStore.load(trustStoreStream, password);
        return trustStore;
      }
    } catch (Exception e) {
      String errorMessage = "Error while loading truststore";
      LOGGER.error(e, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /**
   * @param trustAnchors The trust anchors to use in the trust manager.
   * @param checkCertificateRevocation Whether to check certificate revocation.
   * @param acceptUndeterminedCertificateRevocation Whether to accept undetermined certificate
   * @return The trust manager parameters based on the input parameters.
   */
  public static CertPathTrustManagerParameters buildTrustManagerParameters(
      Set<TrustAnchor> trustAnchors,
      boolean checkCertificateRevocation,
      boolean acceptUndeterminedCertificateRevocation) {
    try {
      PKIXBuilderParameters pkixBuilderParameters =
          new PKIXBuilderParameters(trustAnchors, new X509CertSelector());
      pkixBuilderParameters.setRevocationEnabled(checkCertificateRevocation);
      CertPathValidator certPathValidator =
          CertPathValidator.getInstance(DatabricksJdbcConstants.PKIX);
      PKIXRevocationChecker revocationChecker =
          (PKIXRevocationChecker) certPathValidator.getRevocationChecker();
      if (acceptUndeterminedCertificateRevocation) {
        revocationChecker.setOptions(
            Set.of(
                PKIXRevocationChecker.Option.SOFT_FAIL,
                PKIXRevocationChecker.Option.NO_FALLBACK,
                PKIXRevocationChecker.Option.PREFER_CRLS));
      }
      if (checkCertificateRevocation) {
        pkixBuilderParameters.addCertPathChecker(revocationChecker);
      }
      return new CertPathTrustManagerParameters(pkixBuilderParameters);
    } catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException e) {
      String errorMessage = "Error while building trust manager parameters";
      LOGGER.error(e, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }

  /**
   * @param trustStore The trust store from which to get the trust anchors.
   * @return The set of trust anchors from the trust store.
   */
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
      String errorMessage = "Error while getting trust anchors from trust store";
      LOGGER.error(e, errorMessage);
      throw new DatabricksException(errorMessage, e);
    }
  }
}
