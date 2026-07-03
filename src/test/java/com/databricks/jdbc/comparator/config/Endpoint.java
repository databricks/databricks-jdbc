package com.databricks.jdbc.comparator.config;

/**
 * One side of a comparator run — an SQL endpoint identified by an httpPath, a transport, and a
 * human-readable label. Two {@code Endpoint}s (LEFT and RIGHT) define the comparison axis.
 *
 * <p>Properties are resolved by {@link #fromSystemProperties} with the prefix {@code LEFT_} or
 * {@code RIGHT_}. Resolution precedence for the path:
 *
 * <ol>
 *   <li>{@code <SIDE>_HTTP_PATH} — full path, escape hatch.
 *   <li>{@code <SIDE>_CLUSTER} — {@code orgId:clusterId} → {@code
 *       /sql/protocolv1/o/<orgId>/<clusterId>}.
 *   <li>{@code <SIDE>_WAREHOUSE} — {@code <warehouseId>} → {@code
 *       /sql/1.0/warehouses/<warehouseId>}.
 * </ol>
 *
 * <p>If none of the above is set on either side, callers should fall back to the legacy
 * single-warehouse / dual-transport mode via {@link #legacyPair(String)}.
 *
 * <p>Transport ({@code sea} or {@code thrift}) comes from {@code <SIDE>_TRANSPORT} (default {@code
 * sea}). Label comes from {@code <SIDE>_LABEL} (default {@code <SIDE>-<TRANSPORT>}).
 */
public final class Endpoint {

  public static final String SEA = "sea";
  public static final String THRIFT = "thrift";

  private final String httpPath;
  private final String transport;
  private final String label;

  public Endpoint(String httpPath, String transport, String label) {
    this.httpPath = httpPath;
    this.transport = normalizeTransport(transport);
    this.label = label;
  }

  public String getHttpPath() {
    return httpPath;
  }

  public String getTransport() {
    return transport;
  }

  public String getLabel() {
    return label;
  }

  /**
   * Builds the full JDBC URL by appending {@code httpPath} and {@code useThriftClient} to the base
   * host URL (which must NOT already contain either of those parameters).
   */
  public String toUrl(String baseHostUrl) {
    return baseHostUrl
        + ";httpPath="
        + httpPath
        + ";useThriftClient="
        + (THRIFT.equals(transport) ? "1" : "0");
  }

  /**
   * Resolves an endpoint from system properties prefixed with {@code <side>_}. Returns {@code null}
   * if no path-bearing property is set for that side.
   */
  public static Endpoint fromSystemProperties(String side) {
    String prefix = side.toUpperCase() + "_";
    String httpPath = resolvePath(prefix);
    if (httpPath == null) return null;
    String transport = systemPropertyOrDefault(prefix + "TRANSPORT", SEA);
    String label =
        systemPropertyOrDefault(
            prefix + "LABEL", side.toUpperCase() + "-" + transport.toUpperCase());
    return new Endpoint(httpPath, transport, label);
  }

  /**
   * Legacy single-warehouse pair: {@code (warehouse, thrift)} on the left, {@code (warehouse, sea)}
   * on the right. Used when neither {@code LEFT_*} nor {@code RIGHT_*} is set.
   */
  public static Endpoint[] legacyPair(String warehouseId) {
    String path = "/sql/1.0/warehouses/" + warehouseId;
    return new Endpoint[] {
      new Endpoint(path, THRIFT, "Thrift"), new Endpoint(path, SEA, "SEA"),
    };
  }

  private static String resolvePath(String prefix) {
    String httpPath = System.getProperty(prefix + "HTTP_PATH");
    if (httpPath != null && !httpPath.isEmpty()) return httpPath;

    String cluster = System.getProperty(prefix + "CLUSTER");
    if (cluster != null && !cluster.isEmpty()) {
      int colon = cluster.indexOf(':');
      if (colon <= 0 || colon == cluster.length() - 1) {
        throw new IllegalArgumentException(
            prefix + "CLUSTER must be of the form orgId:clusterId, got: " + cluster);
      }
      return "/sql/protocolv1/o/"
          + cluster.substring(0, colon)
          + "/"
          + cluster.substring(colon + 1);
    }

    String warehouse = System.getProperty(prefix + "WAREHOUSE");
    if (warehouse != null && !warehouse.isEmpty()) {
      return "/sql/1.0/warehouses/" + warehouse;
    }
    return null;
  }

  private static String systemPropertyOrDefault(String key, String defaultValue) {
    String v = System.getProperty(key);
    return (v == null || v.isEmpty()) ? defaultValue : v;
  }

  private static String normalizeTransport(String transport) {
    if (transport == null) return SEA;
    String lower = transport.toLowerCase();
    if (!SEA.equals(lower) && !THRIFT.equals(lower)) {
      throw new IllegalArgumentException("transport must be 'sea' or 'thrift', got: " + transport);
    }
    return lower;
  }
}
