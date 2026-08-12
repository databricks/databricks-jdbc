package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;

/** Internal controls and non-secret route evidence for the local APC-over-SEA prototype. */
public final class ApcSeaPrototype {
  public static final String ENABLED_PROPERTY = "databricks.jdbc.apc.sea.prototype.enabled";
  public static final String ROUTE_MARKER_PREFIX = "JDBC_APC_ROUTE";

  private ApcSeaPrototype() {}

  public static boolean isEnabled() {
    return Boolean.parseBoolean(System.getProperty(ENABLED_PROPERTY, "false"));
  }

  /** Emits one stable marker only after an APC session has been created successfully. */
  public static void emitRouteMarker(IDatabricksConnectionContext context) {
    if (!context.isApcPrototypeEnabled()) {
      return;
    }
    boolean noFallback = context.isApcSeaPrototype();
    System.out.printf(
        "%s route=%s compute=APC no_fallback=%s%n",
        ROUTE_MARKER_PREFIX, context.getClientType(), noFallback);
  }
}
