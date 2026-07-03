package com.databricks.jdbc.comparator.config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A connection cache that creates and reuses JDBC connections by URL.
 *
 * <p>Connections are cached by their full URL string, so identical URLs always return the same
 * connection. Call {@link #close()} to clean up all cached connections.
 */
public class ConnectionManager implements AutoCloseable {
  private final String token;
  private final Map<String, Connection> cache = new LinkedHashMap<>();

  public ConnectionManager(String token) {
    this.token = token;
  }

  /** Returns a cached or newly created connection for the given URL. */
  public Connection getConnection(String url) throws SQLException {
    Connection conn = cache.get(url);
    if (conn != null && !conn.isClosed()) {
      return conn;
    }
    conn = DriverManager.getConnection(url, "token", token);
    cache.put(url, conn);
    return conn;
  }

  /**
   * Opens a NEW connection for the given URL that is NOT cached — the caller owns its lifecycle and
   * must close it. Used by suites whose cases mutate/destroy connection state and therefore need a
   * dedicated, throwaway connection rather than the shared cached one.
   */
  public Connection openUncached(String url) throws SQLException {
    return DriverManager.getConnection(url, "token", token);
  }

  /**
   * Opens a NEW, uncached connection for an arbitrary URL and token — used by suites that build a
   * deliberately-broken URL/token to capture the resulting failure. The caller owns closing it.
   */
  public Connection openUncached(String url, String tokenOverride) throws SQLException {
    return DriverManager.getConnection(url, "token", tokenOverride);
  }

  /** The shared PAT. */
  public String getToken() {
    return token;
  }

  /** Returns all active connection URLs, useful for report headers. */
  public List<String> getActiveUrls() {
    return new ArrayList<>(cache.keySet());
  }

  /** Closes all cached connections. */
  @Override
  public void close() {
    for (Map.Entry<String, Connection> entry : cache.entrySet()) {
      try {
        Connection conn = entry.getValue();
        if (conn != null && !conn.isClosed()) {
          conn.close();
        }
      } catch (SQLException e) {
        System.err.println("Failed to close connection for URL: " + entry.getKey());
        e.printStackTrace();
      }
    }
    cache.clear();
  }
}
