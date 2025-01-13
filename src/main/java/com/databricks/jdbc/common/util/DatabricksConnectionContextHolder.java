package com.databricks.jdbc.common.util;

import com.databricks.jdbc.api.IDatabricksConnectionContext;

public class DatabricksConnectionContextHolder {
  private static final ThreadLocal<IDatabricksConnectionContext> threadLocalContext =
      new ThreadLocal<>();

  public static void setConnectionContext(IDatabricksConnectionContext context) {
    threadLocalContext.set(context);
  }

  public static IDatabricksConnectionContext getConnectionContext() {
    return threadLocalContext.get();
  }

  public static void clear() {
    threadLocalContext.remove();
  }
}
