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
    if (connectionContext.isTelemetryEnabled()) {
      return noauthTelemetryClients.computeIfAbsent(
          connectionContext.getConnectionUuid(),
          k -> new TelemetryClient(connectionContext, false, getTelemetryExecutorService()));
    }
    return NoopTelemetryClient.getInstance();
  }

  public void closeTelemetryClient(IDatabricksConnectionContext connectionContext) {
    if (connectionContext.isTelemetryEnabled()) {
      ITelemetryClient instance = telemetryClients.remove(connectionContext.getConnectionUuid());
      if (instance != null) {
        try {
          instance.close();
        } catch (Exception e) {
          logger.debug(String.format("Caught error while closing telemetry client. Error %s", e));
        }
      }

      ITelemetryClient noauthInstance =
          noauthTelemetryClients.remove(connectionContext.getConnectionUuid());
      if (noauthInstance != null) {
        try {
          noauthInstance.close();
        } catch (Exception e) {
          logger.debug(
              String.format(
                  "Caught error while closing unauthenticated telemetry client. Error %s", e));
        }
      }
    }
  }

  public ExecutorService getTelemetryExecutorService() {
    return telemetryExecutorService;
  }
}
