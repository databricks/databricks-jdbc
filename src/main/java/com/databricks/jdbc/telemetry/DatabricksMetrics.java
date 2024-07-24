package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.MetricsConstants;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

public class DatabricksMetrics implements AutoCloseable {
  private final Map<String, Double> gaugeMetrics = new HashMap<>();
  private final Map<String, Double> counterMetrics = new HashMap<>();
  private final ObjectMapper objectMapper = new ObjectMapper();
  private Boolean hasInitialExportOccurred = false;
  private String workspaceId = null;
  private DatabricksHttpClient telemetryClient;
  private boolean enableTelemetry = false;

  private void setWorkspaceId(String workspaceId) {
    this.workspaceId = workspaceId;
  }

  public enum MetricsType {
    GAUGE,
    COUNTER
  }

  private void scheduleExportMetrics() {
    Timer metricsTimer = new Timer();
    TimerTask task =
        new TimerTask() {
          @Override
          public void run() {
            try {
              sendRequest(gaugeMetrics, MetricsType.GAUGE);
              sendRequest(counterMetrics, MetricsType.COUNTER);
            } catch (Exception e) {
              // Commenting out the exception for now - failing silently
              // System.out.println(e.getMessage());
            }
          }
        };

    // Schedule the task to run after the specified interval infinitely
    metricsTimer.schedule(task, 0, MetricsConstants.INTERVAL_DURATION);
  }

  public DatabricksMetrics(IDatabricksConnectionContext context) throws DatabricksSQLException {
    if (context != null && context.enableTelemetry()) {
      enableTelemetry = true;
      String resourceId = context.getComputeResource().getWorkspaceId();
      setWorkspaceId(resourceId);
      this.telemetryClient = DatabricksHttpClient.getInstance(context);
      scheduleExportMetrics();
    }
  }

  private void sendRequest(Map<String, Double> map, MetricsType metricsType) {
    // Check if the telemetry client is set
    if (telemetryClient == null) {
      LoggingUtil.log(
          LogLevel.DEBUG,
          "Telemetry client is not set for resource Id: "
              + workspaceId
              + ". Initialize the Driver first.");
    } else {
      if (map.isEmpty()) {
        return;
      }
      try {
        // Convert the map to JSON string
        String jsonInputString = objectMapper.writeValueAsString(map);

        // Create the request and adding parameters & headers
        URIBuilder uriBuilder = new URIBuilder(MetricsConstants.METRICS_URL);
        HttpPost request = new HttpPost(uriBuilder.build());
        request.setHeader(MetricsConstants.METRICS_MAP_STRING, jsonInputString);
        request.setHeader(
            MetricsConstants.METRICS_TYPE, metricsType.name().equals("GAUGE") ? "1" : "0");

        // TODO (Bhuvan): Add authentication headers
        // TODO (Bhuvan): execute request using Certificates
        CloseableHttpResponse response = telemetryClient.executeWithoutCertVerification(request);

        // Error handling
        if (response == null) {
          LoggingUtil.log(LogLevel.DEBUG, "Response is null for metrics export.");
        } else if (response.getStatusLine().getStatusCode() != 200) {
          LoggingUtil.log(
              LogLevel.DEBUG,
              "Response code for metrics export: "
                  + response.getStatusLine().getStatusCode()
                  + " Response: "
                  + response.getEntity().toString());
        } else {
          // Clearing map after successful response
          map.clear();

          // Get the response string
          LoggingUtil.log(LogLevel.DEBUG, EntityUtils.toString(response.getEntity()));
          response.close();
        }
      } catch (Exception e) {
        LoggingUtil.log(LogLevel.DEBUG, "Failed to export metrics. Error: " + e.getMessage());
      }
    }
  }

  private void setGaugeMetrics(String name, double value) {
    // TODO: Handling metrics export when multiple users are accessing from the same workspace_id.
    if (!gaugeMetrics.containsKey(name)) {
      gaugeMetrics.put(name, 0.0);
    }
    gaugeMetrics.put(name, value);
  }

  private void incCounterMetrics(String name, double value) {
    if (!counterMetrics.containsKey(name)) {
      counterMetrics.put(name, 0.0);
    }
    counterMetrics.put(name, value);
  }

  private void initialExport(Map<String, Double> map, MetricsType metricsType) {
    hasInitialExportOccurred = true;
    CompletableFuture.runAsync(
        () -> {
          try {
            sendRequest(map, metricsType);
          } catch (Exception e) {
            // Commenting out the exception for now - failing silently
            // System.out.println(e.getMessage());
          }
        });
  }

  // record() appends the metric to be exported in the gauge metric map
  public void record(String name, double value) {
    if (enableTelemetry) {
      setGaugeMetrics(name + "_" + workspaceId, value);
      if (!hasInitialExportOccurred) initialExport(gaugeMetrics, MetricsType.GAUGE);
    }
  }

  // increment() appends the metric to be exported in the counter metric map
  public void increment(String name, double value) {
    if (enableTelemetry) {
      incCounterMetrics(name + "_" + workspaceId, value);
      if (!hasInitialExportOccurred) initialExport(counterMetrics, MetricsType.COUNTER);
    }
  }

  @Override
  public void close() {
    // Flush out metrics when connection is closed
    if (telemetryClient != null) {
      try {
        sendRequest(gaugeMetrics, DatabricksMetrics.MetricsType.GAUGE);
        sendRequest(counterMetrics, DatabricksMetrics.MetricsType.COUNTER);
      } catch (Exception e) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            "Failed to export metrics when connection is closed. Error: " + e.getMessage());
      }
    }
  }
}
