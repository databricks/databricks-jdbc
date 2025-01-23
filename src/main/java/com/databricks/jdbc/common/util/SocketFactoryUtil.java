package com.databricks.jdbc.common.util;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;

public class SocketFactoryUtil {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(SocketFactoryUtil.class);

  /**
   * <b>NOTE: </b> Only for testing purposes and should never be used in production.
   *
   * <p>Builds a registry of connection socket factories that trusts all SSL certificates.
   *
   * @return A registry of connection socket factories.
   */
  public static Registry<ConnectionSocketFactory> getTrustAllSocketFactoryRegistry() {
    LOGGER.warn(
        "This driver is configured to trust all SSL certificates. This is insecure and should be never used in production.");
    LOGGER.debug("Entering the getTrustAllSocketFactoryRegistry method");

    try {
      // Create a TrustManager that trusts all certificates
      TrustManager[] trustAllCerts =
          new TrustManager[] {
            new X509TrustManager() {
              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return null; // Accept all issuers
              }

              @Override
              public void checkClientTrusted(X509Certificate[] certs, String authType) {
                // No-op: Trust all client certificates
              }

              @Override
              public void checkServerTrusted(X509Certificate[] certs, String authType) {
                // No-op: Trust all server certificates
              }
            }
          };

      // Initialize the SSLContext with trust-all settings
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, trustAllCerts, new SecureRandom());

      // Disable hostname verification
      HostnameVerifier allHostsValid = (hostname, session) -> true;

      // Configure SSLConnectionSocketFactory with the trust-all SSLContext
      SSLConnectionSocketFactory sslSocketFactory =
          new SSLConnectionSocketFactory(sslContext, allHostsValid);

      // Build and return the registry
      return RegistryBuilder.<ConnectionSocketFactory>create()
          .register("https", sslSocketFactory)
          .register("http", new PlainConnectionSocketFactory())
          .build();

    } catch (Exception e) {
      String errorMessage = "Error while setting up trust-all SSL context.";
      LOGGER.error(errorMessage, e);
      throw new DatabricksException(errorMessage, e);
    }
  }
}
