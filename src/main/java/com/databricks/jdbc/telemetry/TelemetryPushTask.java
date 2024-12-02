package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

class TelemetryPushTask implements Runnable {

  private List<TelemetryFrontendLog> queueToBePushed;
  private boolean isAuthenticated;
  private IDatabricksConnectionContext connectionContext;

  TelemetryPushTask(
      List<TelemetryFrontendLog> eventsQueue,
      boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext) {
    this.queueToBePushed = eventsQueue;
    this.isAuthenticated = isAuthenticated;
    this.connectionContext = connectionContext;
  }

  @Override
  public void run() {
    TelemetryRequest request = new TelemetryRequest();
    request
        .setUploadTime(System.currentTimeMillis())
        .setProtoLogs(
            queueToBePushed.isEmpty()
                ? Optional.empty()
                : Optional.of(
                    queueToBePushed.stream().map(Object::toString).collect(Collectors.toList())));
    // Add handling for authenticated and unauthenticated push request
  }
}
