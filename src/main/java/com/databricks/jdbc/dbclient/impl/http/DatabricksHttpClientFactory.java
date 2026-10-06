package com.databricks.jdbc.dbclient.impl.http;

import static java.util.AbstractMap.SimpleEntry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksHttpClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpClientFactory.class);
  private static final DatabricksHttpClientFactory INSTANCE = new DatabricksHttpClientFactory();

  /**
   * Maps (connectionUuid, type) → HTTP client. On {@link #closeConnection}, real clients are
   * replaced with {@link ClosedConnectionHttpClient#INSTANCE} tombstones. {@link #getClient}'s
   * {@code computeIfAbsent} returns the tombstone for closed connections (key already exists) and
   * creates a real client for new keys. No parallel sets needed — the closed marker lives in the
   * map itself, bounded by live (uuid, type) pairs. See issue #1325.
   */
  private final ConcurrentHashMap<SimpleEntry<String, HttpClientType>, IDatabricksHttpClient>
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

  /**
   * Returns an HTTP client for the given connection and type, creating one if needed. For closed
   * connections, returns the {@link ClosedConnectionHttpClient} sentinel — callers that attempt to
   * use it get an immediate {@link com.databricks.jdbc.exception.DatabricksHttpException} with a
   * clear message. Never returns null.
   */
  public IDatabricksHttpClient getClient(
      IDatabricksConnectionContext context, HttpClientType type) {
    return instances.computeIfAbsent(
        getClientKey(context.getConnectionUuid(), type),
        k -> new DatabricksHttpClient(context, type));
  }

  /**
   * Permanently closes all HTTP clients for the given connection and replaces them with tombstone
   * sentinels that reject further use. Called from {@link
   * com.databricks.jdbc.api.impl.DatabricksConnection#close()}.
   */
  public void closeConnection(IDatabricksConnectionContext context) {
    String uuid = context.getConnectionUuid();
    for (HttpClientType type : HttpClientType.values()) {
      SimpleEntry<String, HttpClientType> key = getClientKey(uuid, type);
      IDatabricksHttpClient old = instances.put(key, ClosedConnectionHttpClient.INSTANCE);
      if (old != null && !(old instanceof ClosedConnectionHttpClient)) {
        closeQuietly(old);
      }
    }
  }

  @VisibleForTesting
  public void removeClient(IDatabricksConnectionContext context) {
    for (HttpClientType type : HttpClientType.values()) {
      removeClient(context, type);
    }
  }

  @VisibleForTesting
  public void removeClient(IDatabricksConnectionContext context, HttpClientType type) {
    IDatabricksHttpClient instance =
        instances.remove(getClientKey(context.getConnectionUuid(), type));
    if (instance != null && !(instance instanceof ClosedConnectionHttpClient)) {
      closeQuietly(instance);
    }
  }

  /** Resets all state. For test cleanup only. */
  @VisibleForTesting
  public void reset() {
    instances.forEach(
        (key, client) -> {
          if (!(client instanceof ClosedConnectionHttpClient)) {
            closeQuietly(client);
          }
        });
    instances.clear();
  }

  private static void closeQuietly(IDatabricksHttpClient client) {
    if (client instanceof DatabricksHttpClient) {
      try {
        ((DatabricksHttpClient) client).close();
      } catch (IOException e) {
        LOGGER.debug("Caught error while closing http client. Error {}", e);
      }
    }
  }

  private SimpleEntry<String, HttpClientType> getClientKey(String uuid, HttpClientType clientType) {
    return new SimpleEntry<>(uuid, clientType);
  }
}
