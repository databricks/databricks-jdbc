package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class ProxyTest {

  private static String patToken;
  private static String host;
  private static String httpPath;
  private static String proxyUrl;
  private static String cfProxyUrl;
  private static String proxyUser;
  private static String proxyPass;

  @BeforeAll
  public static void setupEnv() {
    patToken = System.getenv("DATABRICKS_TOKEN");
    host = System.getenv("DATABRICKS_HOST");
    httpPath = System.getenv("DATABRICKS_HTTP_PATH");
    proxyUrl = System.getenv("PROXY_URL");
    cfProxyUrl = System.getenv("CF_PROXY_URL");
    proxyUser = System.getenv("PROXY_USER");
    proxyPass = System.getenv("PROXY_PASS");

    System.out.println("=== Environment ===");
    System.out.println("PAT Token present? " + (patToken != null && !patToken.isEmpty()));
    System.out.println("Host: " + host);
    System.out.println("HttpPath: " + httpPath);
    System.out.println("ProxyUrl: " + proxyUrl);
    System.out.println("CFProxyUrl: " + cfProxyUrl);
    System.out.println("ProxyUser: " + proxyUser);
  }

  private String buildJdbcUrl(
      boolean useThriftClient,
      boolean useProxy,
      boolean useCfProxy,
      boolean basicAuth,
      boolean breakProxy) {
    String defaultProxyHost = "localhost";
    String defaultProxyPort = "3128";
    if (proxyUrl != null && proxyUrl.startsWith("http")) {
      String trimmed = proxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = trimmed.split(":");
      if (parts.length > 1) {
        defaultProxyHost = parts[0];
        defaultProxyPort = parts[1];
      }
    }

    String defaultCfProxyHost = "localhost";
    String defaultCfProxyPort = "8889";
    if (cfProxyUrl != null && cfProxyUrl.startsWith("http")) {
      String trimmed = cfProxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = trimmed.split(":");
      if (parts.length > 1) {
        defaultCfProxyHost = parts[0];
        defaultCfProxyPort = parts[1];
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
        .append(";useproxy=")
        .append(useProxy ? "1" : "0")
        .append(";usecfproxy=")
        .append(useCfProxy ? "1" : "0")
        .append(";");

    if (useProxy && !breakProxy) {
      sb.append("ProxyHost=")
          .append(defaultProxyHost)
          .append(";")
          .append("ProxyPort=")
          .append(defaultProxyPort)
          .append(";");
    } else if (useProxy && breakProxy) {
      // induce failure by using invalid port
      sb.append("ProxyHost=").append(defaultProxyHost).append(";").append("ProxyPort=9999;");
    }

    // If we're using Cloud Fetch proxy
    if (useCfProxy) {
      sb.append("CFProxyHost=")
          .append(defaultCfProxyHost)
          .append(";")
          .append("CFProxyPort=")
          .append(defaultCfProxyPort)
          .append(";");
    }

    // TODO(PECO-2187): Add auth for proxy testing
    if (basicAuth) {
      sb.append("ProxyUser=")
          .append(proxyUser)
          .append(";")
          .append("ProxyPassword=")
          .append(proxyPass)
          .append(";");
    }

    return sb.toString();
  }

  /** Utility to verify we can connect and do "SELECT 1" */
  private void verifyConnect(String jdbcUrl) throws Exception {
    System.out.println("Attempting to connect with URL: " + jdbcUrl);
    try (Connection conn = DriverManager.getConnection(jdbcUrl, "token", patToken)) {
      Statement stmt = conn.createStatement();
      ResultSet rs = stmt.executeQuery("SELECT 1");
      assertTrue(rs.next(), "Should get at least one row");
      assertEquals(1, rs.getInt(1), "Value should be 1");
      System.out.println("Success!");
    }
  }

  // ----------------------------------------------------------------
  // SCENARIOS
  // ----------------------------------------------------------------

  /** Scenario 1: No Proxy */
  @Test
  public void testNoProxy() {
    System.out.println("Scenario: No Proxy");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, false, false, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        e.printStackTrace();
        fail("No-proxy test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  /**
   * Scenario 2: Single Proxy for JDBC only (useproxy=1, usecfproxy=0) - Test both SEA and Thrift.
   */
  @Test
  public void testSingleProxy() {
    System.out.println("Scenario: Single Proxy (JDBC only)");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, true, false, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Single-proxy test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  /**
   * Scenario 3: Separate Proxies for JDBC and CF (useproxy=1, usecfproxy=1). - Test both SEA and
   * Thrift.
   */
  @Test
  public void testSeparateProxies() {
    System.out.println("Scenario: Separate Proxies (JDBC + CF)");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, true, true, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        e.printStackTrace();
        fail("Separate-proxies test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }

  /** Scenario 4: Failure case with broken proxy port. */
  @Test
  public void testFailureCase() {
    System.out.println("Scenario: Proxy failure (invalid port)");
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, true, false, false, true);
      Exception ex =
          assertThrows(
              Exception.class,
              () -> {
                verifyConnect(url);
              });
      System.out.println("Caught expected failure with thrift=" + thrift + ": " + ex.getMessage());
    }
  }

  /** Scenario 5: Using system proxy without JDBC params */
  @Test
  public void testSystemProxy() {
    System.out.println("Scenario: Test using system proxy");
    String defaultProxyHost = "localhost";
    String defaultProxyPort = "3128";
    if (proxyUrl != null && proxyUrl.startsWith("http")) {
      String trimmed = proxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = trimmed.split(":");
      if (parts.length > 1) {
        defaultProxyHost = parts[0];
        defaultProxyPort = parts[1];
      }
    }
    System.setProperty("https.proxyHost", defaultProxyHost);
    System.setProperty("https.proxyPort", defaultProxyPort);
    for (boolean thrift : new boolean[] {true, false}) {
      String url = buildJdbcUrl(thrift, false, false, false, false);
      try {
        verifyConnect(url);
      } catch (Exception e) {
        e.printStackTrace();
        fail("System-proxy test failed (thrift=" + thrift + "): " + e.getMessage());
      }
    }
  }
}
