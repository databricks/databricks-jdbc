package com.databricks.jdbc.driver;

import java.io.FileInputStream;
import java.security.KeyStore;
import java.security.SecureRandom;
import javax.net.ssl.*;

public class SSLConfiguration {

  public static SSLContext configureSslContext(
      String keystorePath, String trustStorePath, String keystorePassword) throws Exception {
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try (FileInputStream inputStream = new FileInputStream(keystorePath)) {
      keyStore.load(inputStream, keystorePassword.toCharArray());
    }

    KeyManagerFactory keyManagerFactory =
        KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    keyManagerFactory.init(keyStore, keystorePassword.toCharArray());

    // Currently setting up a naive trust manager that trusts all certificates
    // TODO (PECO-1470): Change Trust Manager configuration to allow only trusted certificates
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    try (FileInputStream inputStream = new FileInputStream(trustStorePath)) {
      trustStore.load(inputStream, keystorePassword.toCharArray());
    }

    trustManagerFactory.init(trustStore);
    //    TrustManager[] trustAllCerts =
    //        new TrustManager[] {
    //          new X509TrustManager() {
    //            public X509Certificate[] getAcceptedIssuers() {
    //              return null;
    //            }
    //
    //            public void checkClientTrusted(X509Certificate[] certs, String authType) {}
    //
    //            public void checkServerTrusted(X509Certificate[] certs, String authType) {}
    //          }
    //        };

    // Initialize SSLContext
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        keyManagerFactory.getKeyManagers(),
        trustManagerFactory.getTrustManagers(),
        new SecureRandom());
    return sslContext;
  }
}
