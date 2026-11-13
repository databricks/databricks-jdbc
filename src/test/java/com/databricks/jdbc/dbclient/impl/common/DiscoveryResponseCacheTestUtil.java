package com.databricks.jdbc.dbclient.impl.common;

/** Lets tests outside this package reset the JVM-wide discovery cache. */
public final class DiscoveryResponseCacheTestUtil {
  private DiscoveryResponseCacheTestUtil() {}

  public static void clear() {
    DiscoveryResponseCache.clear();
  }
}
