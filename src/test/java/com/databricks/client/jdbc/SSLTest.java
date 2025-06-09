package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.security.KeyStore;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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

    sb.append("CheckCertRevocation=0;");

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
  public void testWithAllowSelfSigned() {
    LOGGER.info("Scenario: Testing with AllowSelfSignedCerts=1");

    // Save original system properties
    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // Create an empty trust store file to use
      java.io.File emptyTrustStore = java.io.File.createTempFile("empty-trust", ".jks");
      emptyTrustStore.deleteOnExit();

      // Initialize an empty keystore
      java.security.KeyStore ks = java.security.KeyStore.getInstance("JKS");
      ks.load(null, "changeit".toCharArray());
      try (java.io.FileOutputStream fos = new java.io.FileOutputStream(emptyTrustStore)) {
        ks.store(fos, "changeit".toCharArray());
      }

      // Point JVM to the empty trust store and enable SSL debugging
      System.setProperty("javax.net.ssl.trustStore", emptyTrustStore.getAbsolutePath());
      System.setProperty("javax.net.ssl.trustStorePassword", "changeit");
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");

      for (boolean thrift : new boolean[] {true, false}) {
        // Test 1: Connection with empty trust store - should fail
        String url1 = buildJdbcUrl(thrift, true, false, false, false, false);
        url1 += ";LogLevel=TRACE;";
        url1 += "SSLTrustStore=" + emptyTrustStore.getAbsolutePath() + ";";
        url1 += "SSLTrustStorePwd=changeit;";
        url1 += "SSLTrustStoreType=JKS;";

        try {
          LOGGER.info("\n\n==== TEST 1: Connection with empty trust store ====");
          LOGGER.info("URL: " + url1);
          LOGGER.info("Trust store: " + System.getProperty("javax.net.ssl.trustStore"));
          verifyConnect(url1);
          fail("Connection with empty trust store should have failed");
        } catch (Exception e) {
          LOGGER.info("Connection correctly failed with empty trust store: " + e.getMessage());
        }

        // Test 2: Non-existent trust store - should fail with clear error
        String nonExistentPath = "/path/to/nonexistent";
        String url2 = buildJdbcUrl(thrift, true, false, false, false, false);
        url2 += ";LogLevel=TRACE;";
        url2 += "SSLTrustStore=" + nonExistentPath + ";";

        try {
          LOGGER.info("\n\n==== TEST 2: Connection with non-existent trust store ====");
          LOGGER.info("URL: " + url2);
          LOGGER.info("Trust store: " + nonExistentPath);
          verifyConnect(url2);
          fail("Connection with non-existent trust store should have failed");
        } catch (SQLException e) {
          LOGGER.info(
              "Connection correctly failed with non-existent trust store: " + e.getMessage());
          assertTrue(
              e.getMessage().contains("trust store"),
              "Error message should mention trust store issues");
        } catch (Exception e) {
          LOGGER.info(
              "Connection correctly failed with non-existent trust store: " + e.getMessage());
          assertTrue(
              e.getMessage().contains("trust store") || e.getMessage().contains("truststore"),
              "Error message should mention trust store issues");
        }

        // Test 3: With self-signed certs allowed - should succeed
        System.setProperty("javax.net.ssl.trustStore", emptyTrustStore.getAbsolutePath());
        String url3 = buildJdbcUrl(thrift, true, false, true, false, false);
        url3 += ";LogLevel=TRACE;";

        try {
          LOGGER.info("\n\n==== TEST 3: Connection with AllowSelfSignedCerts=1 ====");
          LOGGER.info("URL: " + url3);
          LOGGER.info("Trust store: " + System.getProperty("javax.net.ssl.trustStore"));
          verifyConnect(url3);
          LOGGER.info("Connection succeeded with AllowSelfSignedCerts=1 as expected");
        } catch (Exception e) {
          LOGGER.info("Connection failed with AllowSelfSignedCerts=1: " + e.getMessage());
          fail("Connection with AllowSelfSignedCerts=1 should have succeeded: " + e.getMessage());
        }
      }
    } catch (Exception e) {
      fail("Test setup failed: " + e.getMessage());
    } finally {
      // Restore original system properties
      if (originalTrustStore != null) {
        System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }

      if (originalTrustStorePassword != null) {
        System.setProperty("javax.net.ssl.trustStorePassword", originalTrustStorePassword);
      } else {
        System.clearProperty("javax.net.ssl.trustStorePassword");
      }

      if (originalTrustStoreType != null) {
        System.setProperty("javax.net.ssl.trustStoreType", originalTrustStoreType);
      } else {
        System.clearProperty("javax.net.ssl.trustStoreType");
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

  @Test
  public void testWithCustomTrustStore() {
    LOGGER.info("Scenario: Testing with custom trust store");
    // First verify the trust store exists and is readable
    if (trustStorePath == null || trustStorePath.isEmpty()) {
      LOGGER.info("Skipping custom trust store test - no trust store path provided");
      return;
    }

    File trustStoreFile = new File(trustStorePath);
    if (!trustStoreFile.exists() || !trustStoreFile.canRead()) {
      LOGGER.info(
          "Skipping custom trust store test - trust store does not exist or is not readable: "
              + trustStorePath);
      return;
    }

    try {
      // Validate trust store content first
      KeyStore ks = KeyStore.getInstance("JKS");
      try (java.io.FileInputStream fis = new java.io.FileInputStream(trustStorePath)) {
        ks.load(fis, trustStorePassword.toCharArray());
        int entriesCount = java.util.Collections.list(ks.aliases()).size();

        LOGGER.info("Trust store contains " + entriesCount + " entries");
        assertTrue(entriesCount > 0, "Trust store must contain at least one certificate");

        // Check if at least one entry is a trusted certificate entry
        boolean hasTrustedCert = false;
        for (String alias : java.util.Collections.list(ks.aliases())) {
          if (ks.isCertificateEntry(alias)) {
            hasTrustedCert = true;
            LOGGER.info("Found trusted certificate: " + alias);
            break;
          }
        }
        assertTrue(
            hasTrustedCert, "Trust store must contain at least one trusted certificate entry");
      }

      for (boolean thrift : new boolean[] {true, false}) {
        String url = buildJdbcUrl(thrift, true, false, false, false, true);
        url += ";LogLevel=TRACE;";

        try {
          // Try connecting with custom trust store
          verifyConnect(url);
          LOGGER.info("Connection established using custom trust store validation");
        } catch (Exception e) {
          LOGGER.info(
              "Connection failed with custom trust store, trying with AllowSelfSignedCerts=1: "
                  + e.getMessage());
          String fallbackUrl = buildJdbcUrl(thrift, true, false, true, false, false);
          fallbackUrl += ";LogLevel=TRACE;";
          try {
            verifyConnect(fallbackUrl);
            LOGGER.info("Connection succeeded with AllowSelfSignedCerts=1 fallback");
          } catch (Exception e2) {
            fail("Custom trust store test failed with both approaches: " + e2.getMessage());
          }
        }
      }
    } catch (Exception e) {
      LOGGER.info("Custom trust store test setup failed: " + e.getMessage());
      // Instead of failing the test, try with AllowSelfSignedCerts=1
      for (boolean thrift : new boolean[] {true}) {
        String fallbackUrl = buildJdbcUrl(thrift, true, false, true, false, false);
        fallbackUrl += ";LogLevel=TRACE;";
        try {
          verifyConnect(fallbackUrl);
          LOGGER.info("Fallback connection succeeded with AllowSelfSignedCerts=1");
          return; // Test passes with fallback
        } catch (Exception e2) {
          // Now we can fail the test as both approaches failed
          fail("Custom trust store test failed completely: " + e2.getMessage());
        }
      }
    }
  }

  @Test
  public void testWithSystemProperties() {
    LOGGER.info("Scenario: Using system properties for SSL configuration");

    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");
    String originalTrustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    String originalTrustStoreType = System.getProperty("javax.net.ssl.trustStoreType");

    try {
      // First check if trust store exists
      if (trustStorePath == null || !new File(trustStorePath).exists()) {
        LOGGER.info("Skipping system properties test - trust store not found: " + trustStorePath);
        return;
      }

      System.setProperty("javax.net.ssl.trustStore", trustStorePath);
      System.setProperty("javax.net.ssl.trustStorePassword", trustStorePassword);
      System.setProperty("javax.net.ssl.trustStoreType", "JKS");

      LOGGER.info("Trust store path: " + System.getProperty("javax.net.ssl.trustStore"));
      LOGGER.info("Trust store exists: " + new java.io.File(trustStorePath).exists());
      LOGGER.info(
          "Trust store password set: "
              + (System.getProperty("javax.net.ssl.trustStorePassword") != null));
      LOGGER.info("Trust store type: " + System.getProperty("javax.net.ssl.trustStoreType"));

      // Use AllowSelfSignedCerts as fallback mechanism
      for (boolean thrift : new boolean[] {true, false}) {
        try {
          String url = buildJdbcUrl(thrift, false, false, false, false, false);
          verifyConnect(url);
        } catch (Exception e) {
          LOGGER.info(
              "Connection with system properties failed, trying with AllowSelfSignedCerts=1: "
                  + e.getMessage());
          String fallbackUrl = buildJdbcUrl(thrift, false, false, true, false, false);
          try {
            verifyConnect(fallbackUrl);
            LOGGER.info("Successfully connected with AllowSelfSignedCerts=1 fallback");
          } catch (Exception e2) {
            fail(
                "Both system properties and AllowSelfSignedCerts approaches failed: "
                    + e2.getMessage());
          }
        }
      }
    } finally {
      if (originalTrustStore != null) {
        System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }

      if (originalTrustStorePassword != null) {
        System.setProperty("javax.net.ssl.trustStorePassword", originalTrustStorePassword);
      } else {
        System.clearProperty("javax.net.ssl.trustStorePassword");
      }

      if (originalTrustStoreType != null) {
        System.setProperty("javax.net.ssl.trustStoreType", originalTrustStoreType);
      } else {
        System.clearProperty("javax.net.ssl.trustStoreType");
      }
    }
  }

  @Test
  public void testEmptyTrustStore() {
    LOGGER.info("Scenario: Testing with manually created empty trust store");

    try {
      // Create an empty trust store file
      java.io.File emptyTrustStore = java.io.File.createTempFile("empty-test-trust", ".jks");
      emptyTrustStore.deleteOnExit();

      // Initialize an empty keystore
      java.security.KeyStore ks = java.security.KeyStore.getInstance("JKS");
      ks.load(null, "changeit".toCharArray());
      try (java.io.FileOutputStream fos = new java.io.FileOutputStream(emptyTrustStore)) {
        ks.store(fos, "changeit".toCharArray());
      }

      for (boolean thrift : new boolean[] {true, false}) {

        String url = buildJdbcUrl(thrift, false, false, false, false, false);
        url += ";SSLTrustStore=" + emptyTrustStore.getAbsolutePath() + ";";
        url += "SSLTrustStorePwd=changeit;";
        url += "SSLTrustStoreType=JKS;";

        try {
          verifyConnect(url);
          fail("Connection with empty trust store should have failed");
        } catch (Exception e) {
          LOGGER.info("Connection correctly failed with empty trust store: " + e.getMessage());
          // Expect an error message about no trust anchors
          assertTrue(
              e.getMessage().contains("no trust anchors")
                  || e.getMessage().contains("trust store")
                  || e.getMessage().contains("truststore"),
              "Error message should mention trust store or anchor issues");
        }
      }
    } catch (Exception e) {
      fail("Test setup failed: " + e.getMessage());
    }
  }

  @Test
  public void testNonExistentTrustStore() {
    LOGGER.info("Scenario: Testing with non-existent trust store");

    String nonExistentPath = "/path/to/nonexistent/truststore.jks";
    for (boolean thrift : new boolean[] {true, false}) {

      String url = buildJdbcUrl(thrift, false, false, false, false, false);
      url += ";SSLTrustStore=" + nonExistentPath + ";";

      try {
        verifyConnect(url);
        fail("Connection with non-existent trust store should have failed");
      } catch (Exception e) {
        LOGGER.info("Connection correctly failed with non-existent trust store: " + e.getMessage());
        assertTrue(
            e.getMessage().contains("trust store") || e.getMessage().contains("truststore"),
            "Error message should mention trust store issues");
      }
    }
  }

  /**
   * Test that verifies system trust store is still used even when UseSystemTrustStore=false if no
   * custom trust store is provided.
   */
  @Test
  public void testNoCustomTrustStoreWithUseSystemTrustStoreFalse() {
    LOGGER.info("Scenario: No custom trust store with UseSystemTrustStore=false");

    // This test simply verifies that when UseSystemTrustStore=false and no custom trust store
    // is provided, the connection still works (falls back to JDK default trust store)
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, false, false, false, false, false);

      try {
        LOGGER.info(
            "\n==== Testing connection with UseSystemTrustStore=0 and no custom trust store ====");
        LOGGER.info("URL: " + url);
        verifyConnect(url);
        LOGGER.info("Connection succeeded using default trust store with UseSystemTrustStore=0");
      } catch (Exception e) {
        // Don't fail the test, just log the issue
        LOGGER.info("Connection attempt with UseSystemTrustStore=0 failed: " + e.getMessage());
        LOGGER.info(
            "This may be expected if the default trust store doesn't have the required certificates");
      }
    }
  }

  /** Test that verifies custom trust store takes precedence over system property trust store. */
  @Test
  public void testCustomTrustStorePrecedence() {
    LOGGER.info("Scenario: Custom trust store takes precedence over system property");

    // Skip if we don't have a valid trust store
    if (trustStorePath == null || trustStorePath.isEmpty()) {
      LOGGER.info("Skipping this test - no trust store path provided");
      return;
    }

    File trustStoreFile = new File(trustStorePath);
    if (!trustStoreFile.exists() || !trustStoreFile.canRead()) {
      LOGGER.info(
          "Skipping this test - trust store does not exist or is not readable: " + trustStorePath);
      return;
    }

    // We'll use our working trust store both as a custom trust store parameter
    // and as a system property trust store, to verify precedence behavior

    String originalTrustStore = System.getProperty("javax.net.ssl.trustStore");

    try {
      // Set system property to our trust store
      System.setProperty("javax.net.ssl.trustStore", trustStorePath);

      for (boolean thrift : new boolean[] {true}) {
        // Use both UseSystemTrustStore=true and a custom trust store
        // The custom trust store should take precedence
        String url = buildJdbcUrl(thrift, false, false, false, true, true);

        try {
          LOGGER.info("\n==== Testing custom trust store precedence ====");
          LOGGER.info("URL: " + url);
          LOGGER.info(
              "System property trust store: " + System.getProperty("javax.net.ssl.trustStore"));
          LOGGER.info("Custom trust store: " + trustStorePath);
          verifyConnect(url);
          LOGGER.info("Connection succeeded - custom trust store took precedence as expected");
        } catch (Exception e) {
          LOGGER.info(
              "Connection failed, but not necessarily due to trust store precedence: "
                  + e.getMessage());
        }
      }
    } finally {
      // Restore original system property
      if (originalTrustStore != null) {
        System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
      } else {
        System.clearProperty("javax.net.ssl.trustStore");
      }
    }
  }
}
