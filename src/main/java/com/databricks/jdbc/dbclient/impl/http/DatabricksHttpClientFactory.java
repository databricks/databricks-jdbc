package com.databricks.jdbc.dbclient.impl.http;

import static java.util.AbstractMap.SimpleEntry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksHttpClientFactory {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksHttpClientFactory.class);
  private static final DatabricksHttpClientFactory INSTANCE = new DatabricksHttpClientFactory();

  /**
   * Maps (connectionUuid, type) → live HTTP client. Entries are removed on {@link
   * #closeConnection}.
   */
  private final ConcurrentHashMap<SimpleEntry<String, HttpClientType>, IDatabricksHttpClient>
      instances = new ConcurrentHashMap<>();

  /**
   * Connection UUIDs that have been closed. Lets {@link #getClient} return the {@link
   * ClosedConnectionHttpClient} sentinel instead of recreating a client for a closed connection
   * (issue #1325). Weak keys: an entry is reclaimed once the connection's UUID becomes unreachable
   * (the closed connection can no longer be used), so this does not grow without bound.
   */
  private final Set<String> closedConnections =
      Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<String, Boolean>()));

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
    String uuid = context.getConnectionUuid();
    if (closedConnections.contains(uuid)) {
      return ClosedConnectionHttpClient.INSTANCE;
    }
    SimpleEntry<String, HttpClientType> key = getClientKey(uuid, type);
    IDatabricksHttpClient client =
        instances.computeIfAbsent(key, k -> new DatabricksHttpClient(context, type));
    if (closedConnections.contains(uuid)) {
      // Connection was closed concurrently with creation; undo so we don't leak a live client.
      if (instances.remove(key, client)) {
        closeQuietly(client);
      }
      return ClosedConnectionHttpClient.INSTANCE;
    }
    return client;
  }

  /**
   * Permanently closes all HTTP clients for the given connection, removes their entries, and marks
   * the connection closed so {@link #getClient} returns the {@link ClosedConnectionHttpClient}
   * sentinel. Called from {@link com.databricks.jdbc.api.impl.DatabricksConnection#close()}.
   */
  public void closeConnection(IDatabricksConnectionContext context) {
    String uuid = context.getConnectionUuid();
    closedConnections.add(uuid);
    for (HttpClientType type : HttpClientType.values()) {
      closeQuietly(instances.remove(getClientKey(uuid, type)));
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
    String uuid = context.getConnectionUuid();
    closedConnections.remove(uuid);
    IDatabricksHttpClient instance = instances.remove(getClientKey(uuid, type));
    if (instance != null && !(instance instanceof ClosedConnectionHttpClient)) {
      closeQuietly(instance);
    }
  }

  /** Resets all state. For test cleanup only. */
  @VisibleForTesting
  public void reset() {
    instances.forEach((key, client) -> closeQuietly(client));
    instances.clear();
    closedConnections.clear();
  }

  /** Number of live HTTP client entries currently tracked. For test/leak assertions only. */
  @VisibleForTesting
  public int liveClientCount() {
    return instances.size();
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
