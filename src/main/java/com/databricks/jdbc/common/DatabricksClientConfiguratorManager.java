package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DatabricksThreadContextHolder;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksDriverException;
import com.databricks.jdbc.exception.DatabricksSSLException;
import com.databricks.jdbc.exception.DatabricksValidationException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;

public class DatabricksClientConfiguratorManager {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksClientConfiguratorManager.class);
  private static final DatabricksClientConfiguratorManager INSTANCE =
      new DatabricksClientConfiguratorManager();
  private final ConfiguratorFactory configuratorFactory;
  private final JdbcLogger logger;
  private final ConcurrentHashMap<String, CompletableFuture<ClientConfigurator>> instances =
      new ConcurrentHashMap<>();
  private final ThreadLocal<Set<String>> initializingUuids = ThreadLocal.withInitial(HashSet::new);

  private DatabricksClientConfiguratorManager() {
    this(ClientConfigurator::new);
  }

  @VisibleForTesting
  DatabricksClientConfiguratorManager(ConfiguratorFactory configuratorFactory) {
    this(configuratorFactory, LOGGER);
  }

  @VisibleForTesting
  DatabricksClientConfiguratorManager(ConfiguratorFactory configuratorFactory, JdbcLogger logger) {
    this.configuratorFactory = configuratorFactory;
    this.logger = logger;
  }

  @FunctionalInterface
  interface ConfiguratorFactory {
    ClientConfigurator create(IDatabricksConnectionContext context)
        throws DatabricksSSLException, DatabricksValidationException;
  }

  public ClientConfigurator getConfigurator(IDatabricksConnectionContext context) {
    String uuid = context.getConnectionUuid();
    Set<String> initializing = initializingUuids.get();
    // Re-entry must not wait on itself or construct another telemetry-exporting exception.
    if (initializing.contains(uuid)) {
      throw new IllegalStateException(
          "Recursive configurator initialization for connection " + uuid);
    }
    try {
      CompletableFuture<ClientConfigurator> future = instances.get(uuid);
      if (future == null) {
        CompletableFuture<ClientConfigurator> initializer = new CompletableFuture<>();
        future = instances.putIfAbsent(uuid, initializer);
        if (future == null) {
          future = initializer;
          ClientConfigurator configurator = null;
          Throwable failure = null;
          try {
            initializing.add(uuid);
            configurator = configuratorFactory.create(context);
          } catch (Throwable t) {
            failure = t;
            failure = initializationFailure(context, t);
          } finally {
            try {
              if (failure == null) {
                initializer.complete(configurator);
              } else {
                instances.remove(uuid, initializer);
                initializer.completeExceptionally(failure);
              }
            } finally {
              initializing.remove(uuid);
            }
          }
        }
      }
      try {
        return future.join();
      } catch (CompletionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof Error) {
          throw (Error) cause;
        }
        if (cause instanceof RuntimeException) {
          throw (RuntimeException) cause;
        }
        throw e;
      }
    } finally {
      if (initializing.isEmpty()) {
        initializingUuids.remove();
      }
    }
  }

  private Throwable initializationFailure(IDatabricksConnectionContext context, Throwable failure) {
    if (failure instanceof DatabricksDriverException || failure instanceof Error) {
      return failure;
    }
    DatabricksDriverErrorCode errorCode = DatabricksDriverErrorCode.AUTH_ERROR;
    String message;
    if (failure instanceof DatabricksSSLException) {
      message =
          String.format("client configurator failed due to SSL error: %s", failure.getMessage());
    } else if (failure instanceof DatabricksValidationException) {
      message =
          String.format(
              "client configurator failed due to validation error: %s", failure.getMessage());
      errorCode = DatabricksDriverErrorCode.INPUT_VALIDATION_ERROR;
    } else {
      message =
          String.format(
              "Unexpected error while configuring databricks auth client: %s, with connection context %s",
              failure.getMessage(), context);
    }
    logger.error(failure, message);
    DatabricksDriverException error =
        new DatabricksDriverException(message, failure, errorCode, /* silentExceptions= */ true);
    // Cold-host feature-flag initialization can make telemetry itself re-enter a map update.
    try {
      TelemetryHelper.exportFailureLog(
          DatabricksThreadContextHolder.getConnectionContext(),
          errorCode.name(),
          message,
          TelemetryLogLevel.ERROR);
    } catch (Exception telemetryFailure) {
      logger.warn("Failed to export auth configuration error telemetry: {}", telemetryFailure);
    }
    return error;
  }

  /**
   * Returns the client configurator if it exists, otherwise returns null. This is is indetended to
   * be used only for telemetry clients to avoid infinite recursion.
   *
   * @param context the connection context
   * @return the client configurator if it exists, otherwise null
   */
  public ClientConfigurator getConfiguratorOnlyIfExists(IDatabricksConnectionContext context) {
    CompletableFuture<ClientConfigurator> future = instances.get(context.getConnectionUuid());
    if (future == null) {
      return null;
    }
    try {
      return future.getNow(null);
    } catch (CompletionException e) {
      return null;
    }
  }

  @VisibleForTesting
  void setConfigurator(
      IDatabricksConnectionContext context, ClientConfigurator clientConfigurator) {
    instances.put(
        context.getConnectionUuid(), CompletableFuture.completedFuture(clientConfigurator));
  }

  public static DatabricksClientConfiguratorManager getInstance() {
    return INSTANCE;
  }

  public void removeInstance(IDatabricksConnectionContext context) {
    String uuid = context.getConnectionUuid();
    CompletableFuture<ClientConfigurator> removed = instances.remove(uuid);
    if (removed != null) {
      removed.whenComplete(
          (configurator, failure) -> {
            if (configurator != null) {
              try {
                configurator.close();
              } catch (Exception e) {
                logger.warn("Failed to close client configurator for connection {}: {}", uuid, e);
              }
            }
          });
    }
  }
}
