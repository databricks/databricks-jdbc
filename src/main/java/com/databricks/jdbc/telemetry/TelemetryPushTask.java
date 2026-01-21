package com.databricks.jdbc.telemetry;

import static com.databricks.jdbc.common.util.JsonUtil.getTelemetryMapper;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

class TelemetryPushTask implements Runnable {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryPushTask.class);

  private final List<TelemetryFrontendLog> queueToBePushed;
  private final ITelemetryPushClient telemetryPushClient;

  TelemetryPushTask(
      List<TelemetryFrontendLog> eventsQueue, ITelemetryPushClient telemetryPushClient) {
    this.queueToBePushed = eventsQueue;
    this.telemetryPushClient = telemetryPushClient;
  }

  @Override
  public void run() {
    LOGGER.trace("Pushing Telemetry logs of size {}", queueToBePushed.size());
    TelemetryRequest request = new TelemetryRequest();
    if (queueToBePushed.isEmpty()) {
      return;
    }
    try {
      request
          .setUploadTime(System.currentTimeMillis())
          .setProtoLogs(
              queueToBePushed.stream()
                  .map(
                      event -> {
                        try {
                          return getTelemetryMapper().writeValueAsString(event);
                        } catch (JsonProcessingException e) {
                          LOGGER.trace(
                              "Failed to serialize Telemetry event {} with error: {}", event, e);
                          return null; // Return null for failed serialization
                        }
                      })
                  .filter(Objects::nonNull) // Remove nulls from failed serialization
                  .collect(Collectors.toList()));

      telemetryPushClient.pushEvent(request);
    } catch (Exception e) {
      // Retry is already handled in HTTP client, we can return from here
      LOGGER.trace("Failed to push telemetry logs because of the error {}", e);
    }
  }
}
