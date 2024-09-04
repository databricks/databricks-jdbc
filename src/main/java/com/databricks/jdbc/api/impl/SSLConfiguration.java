package com.databricks.jdbc.api.impl;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.*;

public class SSLConfiguration {

  public static SSLContext configureSslContext(
      String keystorePath,
      String keystorePassword,
      String keystoreType,
      String truststorePath,
      String truststorePassword,
      String truststoreType,
      boolean allowSelfSignedCerts)
      throws Exception {

    // Initialize KeyManagerFactory if KeyStore path is provided
    KeyManagerFactory keyManagerFactory = null;
    if (keystorePath != null && !keystorePath.isEmpty()) {
      KeyStore keyStore =
          KeyStore.getInstance(keystoreType != null ? keystoreType : KeyStore.getDefaultType());
      try (FileInputStream keyStoreInputStream = new FileInputStream(keystorePath)) {
        keyStore.load(keyStoreInputStream, keystorePassword.toCharArray());
      }
      keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(keyStore, keystorePassword.toCharArray());
    }

    // Initialize TrustManagerFactory if TrustStore path is provided
    TrustManagerFactory trustManagerFactory = null;
    if (truststorePath != null && !truststorePath.isEmpty()) {
      KeyStore trustStore =
          KeyStore.getInstance(truststoreType != null ? truststoreType : KeyStore.getDefaultType());
      try (FileInputStream trustStoreInputStream = new FileInputStream(truststorePath)) {
        trustStore.load(trustStoreInputStream, truststorePassword.toCharArray());
      }
      trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init(trustStore);
    }

    TrustManager[] trustManagers;
    if (allowSelfSignedCerts) {
      // Allow self-signed certificates
      trustManagers =
          new TrustManager[] {
            new X509TrustManager() {
              public X509Certificate[] getAcceptedIssuers() {
                return null;
              }

              public void checkClientTrusted(X509Certificate[] certs, String authType) {}

              public void checkServerTrusted(X509Certificate[] certs, String authType) {}
            }
          };
    } else if (trustManagerFactory != null) {
      trustManagers = trustManagerFactory.getTrustManagers();
    } else {
      // Default to system's default TrustStore
      trustManagerFactory =
          TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      trustManagerFactory.init((KeyStore) null);
      trustManagers = trustManagerFactory.getTrustManagers();
    }

    // Initialize SSLContext with the key managers and trust managers
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
        trustManagers,
        new SecureRandom());

    return sslContext;
  }
}
