package com.databricks.client.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Optional;
import org.junit.jupiter.api.Test;

public class ProxyTest {
  @Test
  public void testProxyConnectivity() {
    String proxyUrl = System.getProperty("proxyUrl");
    String patToken = System.getProperty("patToken");
    String host = System.getProperty("host");
    String httpPath = System.getProperty("httpPath");

    if (proxyUrl != null && !proxyUrl.isEmpty()) {
      // parse "http://localhost:3128" => host=localhost, port=3128
      String stripped = proxyUrl.replace("http://", "").replace("https://", "");
      String[] parts = stripped.split(":");
      String proxyHost = parts[0];
      String proxyPort = (parts.length > 1) ? parts[1] : "3128";

      System.setProperty("http.proxyHost", proxyHost);
      System.setProperty("http.proxyPort", proxyPort);
      System.setProperty("https.proxyHost", proxyHost);
      System.setProperty("https.proxyPort", proxyPort);
    }

    System.out.println(
        "ProxyTest: PAT token is "
            + Optional.ofNullable(patToken).map(t -> "set").orElse("not set"));
    System.out.println("ProxyTest: Host = " + host);
    System.out.println("ProxyTest: HTTP Path = " + httpPath);

    String jdbcUrl =
        "jdbc:databricks://"
            + host
            + "/default;transportMode=http;ssl=1;AuthMech=3;httpPath="
            + httpPath
            + ";";

    try (Connection conn = DriverManager.getConnection(jdbcUrl, "token", patToken)) {
      System.out.println("JDBC connection through proxy succeeded.");
      Statement statement = conn.createStatement();
      assertDoesNotThrow(
          () -> {
            ResultSet r = statement.executeQuery("SELECT 1");
            r.next();
            assertEquals(1, r.getInt(1));
          });

    } catch (Exception e) {
      e.printStackTrace();
      fail("JDBC proxy test failed: " + e.getMessage());
    }
  }
}
