package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksConnection;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import java.util.LinkedHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TelemetryClientFactory {

  private static final JdbcLogger logger = JdbcLoggerFactory.getLogger(DatabricksConnection.class);

  private static final TelemetryClientFactory INSTANCE = new TelemetryClientFactory();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> telemetryClients = new LinkedHashMap<>();

  @VisibleForTesting
  final LinkedHashMap<String, TelemetryClient> noauthTelemetryClients = new LinkedHashMap<>();

  private final ExecutorService telemetryExecutorService;

  private TelemetryClientFactory() {
    telemetryExecutorService = Executors.newFixedThreadPool(10);
  }

  public static TelemetryClientFactory getInstance() {
    return INSTANCE;
  }

  public ITelemetryClient getTelemetryClient(IDatabricksConnectionContext connectionContext) {
    if (connectionContext.isTelemetryEnabled()) {
      return telemetryClients.computeIfAbsent(
          connectionContext.getConnectionUuid(),
          k -> new TelemetryClient(connectionContext, getTelemetryExecutorService()));
    }
    return NoopTelemetryClient.getInstance();
  }

  public ITelemetryClient getUnauthenticatedTelemetryClient(
      IDatabricksConnectionContext connectionContext) {
    if (connectionContext != null && connectionContext.isTelemetryEnabled()) {
      return noauthTelemetryClients.computeIfAbsent(
          connectionContext.getConnectionUuid(),
          k -> new TelemetryClient(connectionContext, false, getTelemetryExecutorService()));
    }
    return NoopTelemetryClient.getInstance();
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

  private void closeTelemetryClient(ITelemetryClient client, String clientType) {
    if (client != null) {
      try {
        client.close();
      } catch (Exception e) {
        logger.debug(String.format("Caught error while closing %s. Error: %s", clientType, e));
      }
    }
  }
}
