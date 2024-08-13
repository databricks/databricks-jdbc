package com.databricks.jdbc.client.impl.helper;

import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

import javax.net.ssl.CertPathTrustManagerParameters;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.*;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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
      // Get trust anchors from the default TrustManager
      TrustManagerFactory defaultTrustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      defaultTrustManagerFactory.init((KeyStore) null);
      X509TrustManager defaultTrustManager = (X509TrustManager) defaultTrustManagerFactory.getTrustManagers()[0];
      X509Certificate[] certs = defaultTrustManager.getAcceptedIssuers();
      Set<TrustAnchor> trustAnchor = Arrays.stream(certs)
              .map(cert -> new TrustAnchor(cert, null))
              .collect(Collectors.toSet());

      // Build custom TrustManager based on certificate revocation settings
      PKIXBuilderParameters pkixBuilderParameters = new PKIXBuilderParameters(trustAnchor, new X509CertSelector());
      pkixBuilderParameters.setRevocationEnabled(checkCertificateRevocation);
      if (acceptUndeterminedCertificateRevocation) {
        CertPathValidator certPathValidator = CertPathValidator.getInstance("PKIX");
        PKIXRevocationChecker revocationChecker = (PKIXRevocationChecker) certPathValidator.getRevocationChecker();
        revocationChecker.setOptions(Set.of(PKIXRevocationChecker.Option.SOFT_FAIL));
        pkixBuilderParameters.addCertPathChecker(revocationChecker);
      }
      CertPathTrustManagerParameters trustManagerParameters = new CertPathTrustManagerParameters(pkixBuilderParameters);
      TrustManagerFactory customTrustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      customTrustManagerFactory.init(trustManagerParameters);

      // Initialize SSLContext with custom TrustManager
      sslContext.init(null, customTrustManagerFactory.getTrustManagers(), new SecureRandom());
    } catch (Exception e) {
      throw new RuntimeException("Failed to create SSLContext", e);
    }

    // Create the SSLConnectionSocketFactory using the SSLContext
    return Optional.of(new SSLConnectionSocketFactory(
            sslContext,
            NoopHostnameVerifier.INSTANCE
    ));
  }
}
