package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.telemetry.TelemetryHelper.isTelemetryAllowedForConnection;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.DatabricksConfig;
import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class TelemetryClientFactory {

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(TelemetryClientFactory.class);
  private static final String DEFAULT_HOST = "unknown-host";

  private static final TelemetryClientFactory INSTANCE = new TelemetryClientFactory();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> telemetryClients = new LinkedHashMap<>();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> noauthTelemetryClients = new LinkedHashMap<>();

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
      return telemetryClients.computeIfAbsent(
          connectionContext.getConnectionUuid(),
          k ->
              new TelemetryClient(
                  connectionContext, getTelemetryExecutorService(), databricksConfig));
    }
    // Use no-auth telemetry client if connection creation failed.
    return noauthTelemetryClients.computeIfAbsent(
        connectionContext.getConnectionUuid(),
        k -> new TelemetryClient(connectionContext, getTelemetryExecutorService()));
  }

  public void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
    closeTelemetryClient(
        telemetryClients.remove(connectionContext.getConnectionUuid()), "telemetry client");
    closeTelemetryClient(
        noauthTelemetryClients.remove(connectionContext.getConnectionUuid()),
        "unauthenticated telemetry client");
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
    telemetryClients.values().forEach(TelemetryClient::close);
    noauthTelemetryClients.values().forEach(TelemetryClient::close);

    // Clear the maps
    telemetryClients.clear();
    noauthTelemetryClients.clear();
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
}
