package com.databricks.jdbc.comparator.config;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Opens fresh, uncached connections for suites whose cases mutate or destroy connection/session
 * state ({@code setCatalog}, {@code setAutoCommit}, {@code cancel}, {@code close}, {@code
 * setClientInfo}, {@code USE …}). Such a case run on the shared cached connection would poison
 * every later test; each must instead operate on its own short-lived connection and close it in a
 * {@code finally}.
 *
 * <p>A factory is bound to a single {@link ConnectionConfig} — {@link #openFresh} uses that
 * config's resolved LEFT/RIGHT URL and the shared token. The returned connection is NOT cached by
 * {@link ConnectionManager}, so closing it is the caller's responsibility and does not affect the
 * shared connections other suites use.
 */
public interface ConnectionFactory {

  /**
   * Opens a new, uncached connection for the named side.
   *
   * @param side "LEFT" or "RIGHT" (case-insensitive)
   */
  Connection openFresh(String side) throws SQLException;

  /**
   * The resolved (healthy) JDBC URL for the named side. Exposed so suites that deliberately open
   * broken connections (e.g. NEGATIVE_CONNECTION) can start from a good URL and corrupt one piece
   * of it (host, warehouse, a connection param) while leaving the rest valid.
   *
   * @param side "LEFT" or "RIGHT" (case-insensitive)
   */
  String urlFor(String side);

  /** The shared PAT used for all connections. Exposed for suites that test bad-token auth. */
  String token();

  /**
   * Opens a new, uncached connection for an arbitrary URL + token — the escape hatch for suites
   * that build a deliberately-broken URL/token and need to capture the resulting failure. The
   * caller owns closing whatever is returned.
   */
  Connection open(String url, String token) throws SQLException;
}
