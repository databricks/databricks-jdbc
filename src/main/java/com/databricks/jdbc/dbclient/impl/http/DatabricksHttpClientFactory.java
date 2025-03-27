package com.databricks.jdbc.dbclient.impl.http;

import static java.util.AbstractMap.SimpleEntry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksHttpClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpClientFactory.class);
  private static final DatabricksHttpClientFactory INSTANCE = new DatabricksHttpClientFactory();
  private final ConcurrentHashMap<SimpleEntry<String, HttpClientType>, DatabricksHttpClient>
      instances = new ConcurrentHashMap<>();

  private DatabricksHttpClientFactory() {
    // Private constructor to prevent instantiation
  }

  public static DatabricksHttpClientFactory getInstance() {
    return INSTANCE;
  }

  public IDatabricksHttpClient getClient(IDatabricksConnectionContext context) {
    return getClient(context, HttpClientType.COMMON);
  }

  public IDatabricksHttpClient getClient(
      IDatabricksConnectionContext context, HttpClientType type) {
    return instances.computeIfAbsent(
        getClientKey(context.getConnectionUuid(), type),
        k -> new DatabricksHttpClient(context, type));
  }

  public void removeClient(IDatabricksConnectionContext context) {
    for (HttpClientType type : HttpClientType.values()) {
      removeClient(context, type);
    }
  }

  public void removeClient(IDatabricksConnectionContext context, HttpClientType type) {
    DatabricksHttpClient instance =
        instances.remove(getClientKey(context.getConnectionUuid(), type));
    if (instance != null) {
      try {
        instance.close();
      } catch (IOException e) {
        LOGGER.debug(String.format("Caught error while closing http client. Error %s", e));
      }
    }
  }

  private SimpleEntry<String, HttpClientType> getClientKey(String uuid, HttpClientType clientType) {
    return new SimpleEntry<>(uuid, clientType);
  }
}
