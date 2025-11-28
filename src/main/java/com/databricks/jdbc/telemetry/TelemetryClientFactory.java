package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.telemetry.TelemetryHelper.DEFAULT_HOST;
import static com.databricks.jdbc.telemetry.TelemetryHelper.isTelemetryAllowedForConnection;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TelemetryClientFactory {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(TelemetryClientFactory.class);

  private static final TelemetryClientFactory INSTANCE = new TelemetryClientFactory();

  @VisibleForTesting
  final Map<String, TelemetryClientHolder> telemetryClientHolders = new ConcurrentHashMap<>();

  @VisibleForTesting
  final Map<String, TelemetryClientHolder> noauthTelemetryClientHolders = new ConcurrentHashMap<>();

  private final ExecutorService telemetryExecutorService;

  private static ThreadFactory createThreadFactory() {
    return new ThreadFactory() {
      private final AtomicInteger threadNumber = new AtomicInteger(1);

      @Override
      public Thread newThread(Runnable r) {
        Thread thread = new Thread(r, "Telemetry-Thread-" + threadNumber.getAndIncrement());
        // TODO : https://databricks.atlassian.net/browse/PECO-2716
        thread.setDaemon(true);
        return thread;
      }
    };
  }

  private TelemetryClientFactory() {
    telemetryExecutorService = Executors.newFixedThreadPool(10, createThreadFactory());
  }

  public static TelemetryClientFactory getInstance() {
    return INSTANCE;
  }

  public ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
    if (!isTelemetryAllowedForConnection(connectionContext)) {
      return NoopTelemetryClient.getInstance();
    }
    DatabricksConfig databricksConfig =
        TelemetryHelper.getDatabricksConfigSafely(connectionContext);
    if (databricksConfig != null) {
      String key = TelemetryHelper.keyOf(connectionContext);
      TelemetryClientHolder holder =
          telemetryClientHolders.compute(
              key,
              (k, existing) -> {
                if (existing == null) {
                  return new TelemetryClientHolder(
                      new TelemetryClient(
                          connectionContext, getTelemetryExecutorService(), databricksConfig),
                      1);
                }
                existing.refCount.incrementAndGet();
                return existing;
              });
      return holder.client;
    }
    // Use no-auth telemetry client if connection creation failed.
    String key = TelemetryHelper.keyOf(connectionContext);
    TelemetryClientHolder holder =
        noauthTelemetryClientHolders.compute(
            key,
            (k, existing) -> {
              if (existing == null) {
                return new TelemetryClientHolder(
                    new TelemetryClient(connectionContext, getTelemetryExecutorService()), 1);
              }
              existing.refCount.incrementAndGet();
              return existing;
            });
    return holder.client;
  }

  public void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
    String key = TelemetryHelper.keyOf(connectionContext);
    telemetryClientHolders.computeIfPresent(
        key,
        (k, holder) -> {
          if (holder.refCount.get() <= 1) {
            closeTelemetryClient(holder.client, "telemetry client");
            return null;
          }
          holder.refCount.decrementAndGet();
          return holder;
        });
    noauthTelemetryClientHolders.computeIfPresent(
        key,
        (k, holder) -> {
          if (holder.refCount.get() <= 1) {
            closeTelemetryClient(holder.client, "unauthenticated telemetry client");
            return null;
          }
          holder.refCount.decrementAndGet();
          return holder;
        });
  }

  public ExecutorService getTelemetryExecutorService() {
    return telemetryExecutorService;
  }

  static ITelemetryPushClient getTelemetryPushClient(
      Boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext,
      DatabricksConfig databricksConfig) {
    ITelemetryPushClient pushClient =
        new TelemetryPushClient(isAuthenticated, connectionContext, databricksConfig);
    if (connectionContext.isTelemetryCircuitBreakerEnabled()) {
      // If circuit breaker is enabled, use the circuit breaker client
      String host = null;
      try {
        host = connectionContext.getHostUrl();
      } catch (DatabricksParsingException e) {
        // Even though Telemetry logs should be trace or debug, we are treating this as error,
        // since host parsing is fundamental to JDBC.
        LOGGER.error(e, "Error parsing host url");
        // Fallback to a default value, we don't want to throw any exception from Telemetry
        host = DEFAULT_HOST;
      }
      pushClient = new CircuitBreakerTelemetryPushClient(pushClient, host);
    }
    return pushClient;
  }

  @VisibleForTesting
  public void reset() {
    // Close all existing clients
    telemetryClientHolders.values().forEach(holder -> holder.client.close());
    noauthTelemetryClientHolders.values().forEach(holder -> holder.client.close());

    // Clear the maps
    telemetryClientHolders.clear();
    noauthTelemetryClientHolders.clear();
  }

  private void closeTelemetryClient(ITelemetryClient client, String clientType) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        LOGGER.debug("Caught error while closing {}. Error: {}", clientType, e);
      }
    }
  }

  private static final class TelemetryClientHolder {
    final TelemetryClient client;
    final AtomicInteger refCount;

    TelemetryClientHolder(TelemetryClient client, int initialCount) {
      this.client = client;
      this.refCount = new AtomicInteger(initialCount);
    }
  }
}
