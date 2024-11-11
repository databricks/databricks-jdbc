package com.databricks.jdbc.dbclient.impl.http;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksHttpClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpClientFactory.class);
  private static final DatabricksHttpClientFactory INSTANCE = new DatabricksHttpClientFactory();
  private final ConcurrentHashMap<String, DatabricksHttpClient> instances =
      new ConcurrentHashMap<>();

  private DatabricksHttpClientFactory() {
    // Private constructor to prevent instantiation
  }

  public static DatabricksHttpClientFactory getInstance() {
    return INSTANCE;
  }

  public IDatabricksHttpClient getClient(IDatabricksConnectionContext context) {
    String contextKey = Integer.toString(context.hashCode());
    return instances.computeIfAbsent(contextKey, k -> new DatabricksHttpClient(context));
  }

  public void removeClient(IDatabricksConnectionContext context) {
    String contextKey = Integer.toString(context.hashCode());
    DatabricksHttpClient instance = instances.remove(contextKey);
    if (instance != null) {
      try {
        instance.close();
      } catch (IOException e) {
        LOGGER.debug(String.format("Caught error while closing http client. Error %s", e));
      }
    }
  }
}
