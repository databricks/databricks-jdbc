package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.logging.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class SSLTest {

  private static final Logger LOGGER = Logger.getLogger(SSLTest.class.getName());
  private static String patToken;
  private static String host;
  private static String httpPath;
  private static String httpProxyUrl;
  private static String httpsProxyUrl;
  private static String trustStorePath;
  private static String trustStorePassword;

  @BeforeAll
  public static void setupEnv() {
    patToken = System.getenv("DATABRICKS_TOKEN");
    host = System.getenv("DATABRICKS_HOST");
    httpPath = System.getenv("DATABRICKS_HTTP_PATH");
    httpProxyUrl = System.getenv("HTTP_PROXY_URL");
    httpsProxyUrl = System.getenv("HTTPS_PROXY_URL");
    trustStorePath = System.getenv("TRUSTSTORE_PATH");
    trustStorePassword = System.getenv("TRUSTSTORE_PASSWORD");
  }

  private String buildJdbcUrl(
      boolean useThriftClient,
      boolean useProxy,
      boolean useHttpsProxy,
      boolean allowSelfSignedCerts,
      boolean useSystemTrustStore,
      boolean useCustomTrustStore) {

    String defaultProxyHost = "localhost";
    String defaultProxyPort = "3128";
    if (httpProxyUrl != null && httpProxyUrl.startsWith("http")) {
      String trimmed = httpProxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = trimmed.split(":");
      if (parts.length > 1) {
        defaultProxyHost = parts[0];
        defaultProxyPort = parts[1];
      }
    }

    String defaultHttpsProxyHost = "localhost";
    String defaultHttpsProxyPort = "3129";
    if (httpsProxyUrl != null && httpsProxyUrl.startsWith("http")) {
      String trimmed = httpsProxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = trimmed.split(":");
      if (parts.length > 1) {
        defaultHttpsProxyHost = parts[0];
        defaultHttpsProxyPort = parts[1];
      }
    }

    StringBuilder sb = new StringBuilder();
    sb.append("jdbc:databricks://")
        .append(host)
        .append("/default")
        .append(";httpPath=")
        .append(httpPath)
        .append(";AuthMech=3")
        .append(";usethriftclient=")
        .append(useThriftClient ? "true" : "false")
        .append(";");

    if (useProxy) {
      sb.append("useproxy=1;")
          .append("ProxyHost=")
          .append(defaultProxyHost)
          .append(";")
          .append("ProxyPort=")
          .append(defaultProxyPort)
          .append(";");
    } else {
      sb.append("useproxy=0;");
    }

    if (useHttpsProxy) {
      sb.append("ProxyHost=")
          .append(defaultHttpsProxyHost)
          .append(";")
          .append("ProxyPort=")
          .append(defaultHttpsProxyPort)
          .append(";");
    }

    sb.append("AllowSelfSignedCerts=")
        .append(allowSelfSignedCerts ? "1" : "0")
        .append(";")
        .append("UseSystemTrustStore=")
        .append(useSystemTrustStore ? "1" : "0")
        .append(";");

    if (useCustomTrustStore && trustStorePath != null && !trustStorePath.isEmpty()) {
      sb.append("SSLTrustStore=").append(trustStorePath).append(";");

      if (trustStorePassword != null && !trustStorePassword.isEmpty()) {
        sb.append("SSLTrustStorePwd=").append(trustStorePassword).append(";");
        // Add trust store type when we know it
        sb.append("SSLTrustStoreType=").append("JKS").append(";");
      }
    }

    sb.append("ssl=1;");
    return sb.toString();
  }

  private void verifyConnect(String jdbcUrl) throws Exception {
    LOGGER.info("Attempting to connect with URL: " + jdbcUrl);

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "token", patToken)) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");
      assertTrue(rs.next(), "Should get at least one row");
      assertEquals(1, rs.getInt(1), "Value should be 1");
      LOGGER.info("Success!");
    }
  }

  @Test
  public void testDirectConnectionDefaultSSL() {
    LOGGER.info("Scenario: Direct connection with default SSL settings");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, false, false, false, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        fail("Direct connection test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  @Test
  public void testHttpProxyDefaultSSL() {
    LOGGER.info("Scenario: HTTP Proxy with default SSL settings");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, true, false, false, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        fail("HTTP proxy test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  @Test
  public void testWithSystemTrustStore() {
    LOGGER.info("Scenario: Testing with UseSystemTrustStore=1");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, true, false, false, true, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        fail("UseSystemTrustStore=1 test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  @Test
  public void testDirectConnectionSystemTrustStoreFallback() {
    LOGGER.info(
        "Scenario: UseSystemTrustStore=1 with no system property -> fallback to cacerts (direct)");

    // ensure the property is *unset* for this test run
    String savedProp = System.getProperty("javax.net.ssl.trustStore");
    try {
      System.clearProperty("javax.net.ssl.trustStore");

      for (boolean thrift : new boolean[] {true, false}) {
        String url = buildJdbcUrl(thrift, false, false, false, true, false);
        try {
          verifyConnect(url);
        } catch (Exception e) {
          fail(
              "Fallback‑to‑cacerts direct connect failed (thrift="
                  + thrift
                  + "): "
                  + e.getMessage());
        }
      }
    } finally {
      // restore original system state
      if (savedProp != null) {
        System.setProperty("javax.net.ssl.trustStore", savedProp);
      }
    }
  }

  @Test
  public void testIgnoreSystemPropertyWhenUseSystemTrustStoreDisabled() {
    LOGGER.info(
        "Scenario: bogus javax.net.ssl.trustStore present but UseSystemTrustStore=0 (driver must ignore)");

    String savedProp = System.getProperty("javax.net.ssl.trustStore");
    try {
      System.setProperty("javax.net.ssl.trustStore", "/path/that/does/not/exist.jks");

      for (boolean thrift : new boolean[] {true, false}) {
        String url = buildJdbcUrl(thrift, false, false, false, false, false);
        try {
          verifyConnect(url);
        } catch (Exception e) {
          fail(
              "Driver failed to ignore bogus system trust store (thrift="
                  + thrift
                  + "): "
                  + e.getMessage());
        }
      }
    } finally {
      // restore original value
      if (savedProp != null) {
        System.setProperty("javax.net.ssl.trustStore", savedProp);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }
    }
  }
}
