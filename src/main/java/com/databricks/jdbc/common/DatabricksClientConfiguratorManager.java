package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

public class DatabricksClientConfiguratorManager {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksClientConfiguratorManager.class);
  private static final DatabricksClientConfiguratorManager INSTANCE =
      new DatabricksClientConfiguratorManager();
  private final ConcurrentHashMap<String, ConfiguratorHolder> instances = new ConcurrentHashMap<>();

  private DatabricksClientConfiguratorManager() {
    // Private constructor to prevent instantiation
  }

  public ClientConfigurator getConfigurator(IDatabricksConnectionContext context) {
    String key = context.getConnectionUuid();
    try {
      while (true) {
        // Only the cheap holder is created inside computeIfAbsent. Building the configurator
        // performs network I/O, which must not hold the map bin shared with other connections.
        ConfiguratorHolder holder = instances.computeIfAbsent(key, k -> new ConfiguratorHolder());
        ClientConfigurator configurator;
        try {
          configurator = holder.getOrCreate(() -> createConfigurator(context));
        } finally {
          if (holder.closed) {
            instances.remove(key, holder);
          }
        }
        if (configurator != null) {
          return configurator;
        }
        // Creation failed for another caller or removeInstance closed this holder; retry.
      }
    } catch (Exception ex) {
      String message =
          String.format(
              "Unexpected error while configuring databricks auth client: %s, with connection context %s",
              ex.getMessage(), context);
      LOGGER.error(ex, message);
      throw new DatabricksDriverException(message, DatabricksDriverErrorCode.AUTH_ERROR);
    }
  }

  /**
   * Returns the client configurator if it exists, otherwise returns null. This is is indetended to
   * be used only for telemetry clients to avoid infinite recursion.
   *
   * @param context the connection context
   * @return the client configurator if it exists, otherwise null
   */
  public ClientConfigurator getConfiguratorOnlyIfExists(IDatabricksConnectionContext context) {
    ConfiguratorHolder holder = instances.get(context.getConnectionUuid());
    return holder != null ? holder.peek() : null;
  }

  /** Allows recursive telemetry lookups to defer work that needs the unfinished configurator. */
  public boolean isCreatingConfiguratorOnCurrentThread(IDatabricksConnectionContext context) {
    String key = context.getConnectionUuid();
    ConfiguratorHolder holder = key != null ? instances.get(key) : null;
    return holder != null && holder.creatingThread == Thread.currentThread();
  }

  @VisibleForTesting
  void setConfigurator(
      IDatabricksConnectionContext context, ClientConfigurator clientConfigurator) {
    instances.put(context.getConnectionUuid(), new ConfiguratorHolder(clientConfigurator));
  }

  public static DatabricksClientConfiguratorManager getInstance() {
    return INSTANCE;
  }

  public void removeInstance(IDatabricksConnectionContext context) {
    ConfiguratorHolder removed = instances.remove(context.getConnectionUuid());
    if (removed != null) {
      removed.close();
    }
  }

  private static ClientConfigurator createConfigurator(IDatabricksConnectionContext context) {
    try {
      return new ClientConfigurator(context);
    } catch (DatabricksSSLException e) {
      String message =
          String.format("client configurator failed due to SSL error: %s", e.getMessage());
      LOGGER.error(e, message);
      throw new DatabricksDriverException(message, DatabricksDriverErrorCode.AUTH_ERROR);
    } catch (DatabricksValidationException e) {
      String message =
          String.format("client configurator failed due to validation error: %s", e.getMessage());
      LOGGER.error(e, message);
      throw new DatabricksDriverException(
          message, DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR);
    }
  }

  /** Per-connection slot; creation and close are serialized only for the same connection. */
  private static final class ConfiguratorHolder {
    private volatile ClientConfigurator configurator;
    private volatile boolean closed;
    private volatile Thread creatingThread;

    ConfiguratorHolder() {}

    ConfiguratorHolder(ClientConfigurator configurator) {
      this.configurator = configurator;
    }

    /** Returns the configurator, creating it on first use, or null if this holder was closed. */
    ClientConfigurator getOrCreate(Supplier<ClientConfigurator> factory) {
      ClientConfigurator existing = configurator;
      if (existing != null && !closed) {
        return existing;
      }
      synchronized (this) {
        if (closed) {
          return null;
        }
        if (configurator == null) {
          // Same-thread re-entry (e.g. via telemetry export from a failed creation) fails fast,
          // as the previous computeIfAbsent did, instead of building a second configurator.
          if (creatingThread == Thread.currentThread()) {
            throw new IllegalStateException("Recursive client configurator creation");
          }
          creatingThread = Thread.currentThread();
          try {
            configurator = factory.get();
          } catch (RuntimeException | Error e) {
            // Waiting callers must retry instead of populating the entry being removed.
            closed = true;
            throw e;
          } finally {
            creatingThread = null;
          }
        }
        return configurator;
      }
    }

    ClientConfigurator peek() {
      return closed ? null : configurator;
    }

    synchronized void close() {
      closed = true;
      if (configurator != null) {
        configurator.close();
      }
    }
  }
}
