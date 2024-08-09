package com.databricks.jdbc.client.impl.helper;

import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.google.common.collect.Sets;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.security.*;
import java.security.cert.*;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;

public class JdbcSSLSocketFactoryHandler {
  public IDatabricksConnectionContext connectionContext;

  public JdbcSSLSocketFactoryHandler(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
  }

  public Optional<SSLConnectionSocketFactory> getCustomSSLSocketFactory() {
    boolean checkCertificateRevocation = connectionContext.checkCertificateRevocation();
    boolean acceptUndeterminedCertificateRevocation = connectionContext.acceptUndeterminedCertificateRevocation();
    if (checkCertificateRevocation && !acceptUndeterminedCertificateRevocation) {
      // If we are checking certificate revocation and do not accept undetermined certificate revocation,
      // we should not use a custom SSLConnectionSocketFactory
      return Optional.empty();
    }
    // Create an SSLContext with the custom TrustManager
    SSLContext sslContext;
    try {
      sslContext = SSLContext.getInstance("TLS");
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      PKIXBuilderParameters pkixParams = new PKIXBuilderParameters();
      pkixParams.setRevocationEnabled(false);
      CertPathTrustManagerParameters trustManagerParameters = new CertPathTrustManagerParameters(pkixParams);
      trustManagerFactory.init(trustManagerParameters);
      sslContext.init(null, trustManagerFactory.getTrustManagers(), new java.security.SecureRandom());
    } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
      throw new RuntimeException("Failed to create SSLContext", e);
    } catch (InvalidAlgorithmParameterException | CertificateException | IOException e) {
      throw new RuntimeException(e);
    }

    // Create the SSLConnectionSocketFactory using the SSLContext
    return Optional.of(new SSLConnectionSocketFactory(
            sslContext,
            NoopHostnameVerifier.INSTANCE
    ));
  }

  // Custom TrustManager class
  static class CustomX509TrustManager implements X509TrustManager {

    private final X509TrustManager defaultTrustManager;
    private final boolean checkCertificateRevocation;
    private final boolean acceptUndeterminedCertificateRevocation;

    public CustomX509TrustManager(boolean checkCertificateRevocation, boolean acceptUndeterminedCertificateRevocation) throws NoSuchAlgorithmException, KeyStoreException, InvalidAlgorithmParameterException, CertificateException, IOException {
      KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
      keyStore.load(null, null);
      TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(keyStore, null);
      pkixParams.setRevocationEnabled(false);
      CertPathTrustManagerParameters trustManagerParameters = new CertPathTrustManagerParameters(pkixParams);
      trustManagerFactory.init(trustManagerParameters);
      this.defaultTrustManager = (X509TrustManager) trustManagerFactory.getTrustManagers()[0];
      this.checkCertificateRevocation = checkCertificateRevocation;
      this.acceptUndeterminedCertificateRevocation = acceptUndeterminedCertificateRevocation;
      LoggingUtil.log(LogLevel.INFO, "set up of ssl factory handler");
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      defaultTrustManager.checkClientTrusted(chain, authType);
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
      // TODO: this function potentially checks more than just revocation so we should not skip everything
      if (!checkCertificateRevocation) {
        // Skip revocation check
        return;
      }
      // Only perform revocation check if CRLDP is present
      for (X509Certificate cert : chain) {
        try {
          Set<String> critExtOids = Sets.union(cert.getCriticalExtensionOIDs(), cert.getNonCriticalExtensionOIDs());
          if (critExtOids.contains("2.5.29.31")) {
            // CRLDP is present, perform default check
            defaultTrustManager.checkServerTrusted(chain, authType);
            LoggingUtil.log(LogLevel.INFO, "CRLDP present, performed revocation check: " + cert.getSubjectDN().getName());
          } else {
            // CRLDP not present, skip revocation check
            LoggingUtil.log(LogLevel.INFO, "CRLDP not present, skipping revocation check: " + cert.getSubjectDN().getName());
          }
        } catch (Exception e) {
          // Handle any exceptions
          throw new CertificateException("Failed to check CRLDP", e);
        }
      }
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return defaultTrustManager.getAcceptedIssuers();
    }
  }
}
