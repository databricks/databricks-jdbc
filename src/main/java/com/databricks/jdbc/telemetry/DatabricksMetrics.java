package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(DatabricksMetrics.class);
  private static final Map<String, Double> gaugeMetrics = new HashMap<>();
  private static final Map<String, Double> counterMetrics = new HashMap<>();
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private Boolean hasInitialExportOccurred = false;
  private String workspaceId = null;
  private DatabricksHttpClient telemetryClient;
  private boolean enableTelemetry = false;

  private enum MetricsType {
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
              exportMetrics(gaugeMetrics, MetricsType.GAUGE);
              exportMetrics(counterMetrics, MetricsType.COUNTER);
            } catch (Exception e) {
              LOGGER.error(
                  "Error while exporting metrics with scheduleExportMetrics: " + e.getMessage());
            }
          }
        };

    // Schedule the task to run after the specified interval infinitely
    metricsTimer.schedule(task, 0, MetricsConstants.INTERVAL_DURATION);
  }

  public DatabricksMetrics(IDatabricksConnectionContext context) {
    enableTelemetry = (context != null && context.enableTelemetry());
    if (enableTelemetry) {
      workspaceId = context.getComputeResource().getWorkspaceId();
      telemetryClient = DatabricksHttpClient.getInstance(context);
      scheduleExportMetrics();
    }
  }

  private void exportMetrics(Map<String, Double> map, MetricsType metricsType) throws Exception {
    if (!enableTelemetry) {
      return;
    }
    HttpPost request = getMetricsExportRequest(map, metricsType);
    handleResponseMetrics(request, map);
  }

  private void exportErrorLog(String sqlQueryId, String connectionConfig, int errorCode)
      throws Exception {
    if (!enableTelemetry) {
      return;
    }
    HttpPost request = getErrorLoggingRequest(sqlQueryId, connectionConfig, errorCode);
    responseHandling(request, "error logging");
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
            exportMetrics(map, metricsType);
          } catch (Exception e) {
            // Commenting out the exception for now - failing silently
            LOGGER.error("Initial export failed. Error: " + e.getMessage());
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

  public void exportError(String errorName, String sqlQueryId, int errorCode) {
    if (!enableTelemetry) {
      return;
    }
    increment(errorName + errorCode, 1);
    try {
      exportErrorLog(
          sqlQueryId,
          "ConnectionConfig", // This gets redacted to null anyway in logfood because of the
          // sensitive data label for connection_config
          errorCode);
      close();
    } catch (Exception e) {
      LOGGER.error("Failed to export log. Error: " + e.getMessage());
    }
  }

  public void exportUsageMetrics(
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding) {
    if (!enableTelemetry) {
      return;
    }
    try {
      HttpPost request =
          getUsageMetricsRequest(
              jvmName,
              jvmSpecVersion,
              jvmImplVersion,
              jvmVendor,
              osName,
              osVersion,
              osArch,
              localeName,
              charsetEncoding);
      responseHandling(request, "usage metrics export");
    } catch (Exception e) {
      LOGGER.error("Failed to export usage metrics. Error: " + e.getMessage());
    }
  }

  private boolean responseHandling(HttpPost request, String methodType) {
    // TODO (Bhuvan): Add authentication headers
    // TODO (Bhuvan): execute request using Certificates
    try (CloseableHttpResponse response = telemetryClient.executeWithoutCertVerification(request)) {
      if (response == null) {
        LOGGER.error("Response is null for " + methodType);
      } else if (response.getStatusLine().getStatusCode() != 200) {
        LOGGER.error(
            "Response code for "
                + methodType
                + response.getStatusLine().getStatusCode()
                + " Response: "
                + response.getEntity().toString());
      } else {
        LOGGER.error(EntityUtils.toString(response.getEntity()));
        return true;
      }
    } catch (Exception e) {
      LOGGER.error(e.getMessage());
    }
    return false;
  }

  private void handleResponseMetrics(HttpPost request, Map<String, Double> map) {
    if (map.isEmpty()) {
      return;
    }
    if (responseHandling(request, "metrics export")) {
      map.clear();
    }
  }

  private HttpPost getErrorLoggingRequest(String sqlQueryId, String connectionConfig, int errorCode)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(MetricsConstants.ERROR_LOGGING_URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(MetricsConstants.WORKSPACE_ID, workspaceId);
    request.setHeader(MetricsConstants.SQL_QUERY_ID, sqlQueryId);
    request.setHeader(
        MetricsConstants.TIMESTAMP,
        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
    request.setHeader(MetricsConstants.DRIVER_VERSION, DriverUtil.getVersion());
    request.setHeader(MetricsConstants.CONNECTION_CONFIG, connectionConfig);
    request.setHeader(MetricsConstants.ERROR_CODE, String.valueOf(errorCode));
    return request;
  }

  private HttpPost getMetricsExportRequest(Map<String, Double> map, MetricsType metricsType)
      throws Exception {
    String jsonInputString = objectMapper.writeValueAsString(map);
    URIBuilder uriBuilder = new URIBuilder(MetricsConstants.METRICS_URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(MetricsConstants.METRICS_MAP_STRING, jsonInputString);
    request.setHeader(
        MetricsConstants.METRICS_TYPE, metricsType.name().equals("GAUGE") ? "1" : "0");
    return request;
  }

  private HttpPost getUsageMetricsRequest(
      String jvmName,
      String jvmSpecVersion,
      String jvmImplVersion,
      String jvmVendor,
      String osName,
      String osVersion,
      String osArch,
      String localeName,
      String charsetEncoding)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(MetricsConstants.USAGE_METRICS_URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(MetricsConstants.WORKSPACE_ID, workspaceId);
    request.setHeader(MetricsConstants.JVM_NAME, jvmName);
    request.setHeader(MetricsConstants.JVM_SPEC_VERSION, jvmSpecVersion);
    request.setHeader(MetricsConstants.JVM_IMPL_VERSION, jvmImplVersion);
    request.setHeader(MetricsConstants.JVM_VENDOR, jvmVendor);
    request.setHeader(MetricsConstants.OS_NAME, osName);
    request.setHeader(MetricsConstants.OS_VERSION, osVersion);
    request.setHeader(MetricsConstants.OS_ARCH, osArch);
    request.setHeader(MetricsConstants.LOCALE_NAME, localeName);
    request.setHeader(MetricsConstants.CHARSET_ENCODING, charsetEncoding);
    return request;
  }

  @VisibleForTesting
  void setHttpClient(DatabricksHttpClient httpClient) {
    this.telemetryClient = httpClient;
  }

  @Override
  public void close() {
    // Flush out metrics when connection is closed
    if (enableTelemetry && telemetryClient != null) {
      try {
        exportMetrics(gaugeMetrics, DatabricksMetrics.MetricsType.GAUGE);
        exportMetrics(counterMetrics, DatabricksMetrics.MetricsType.COUNTER);
      } catch (Exception e) {
        LOGGER.error(
            "Failed to export metrics when connection is closed. Error: " + e.getMessage());
      }
    }
  }
}
