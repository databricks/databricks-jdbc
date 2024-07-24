package com.databricks.jdbc.telemetry;

import com.databricks.client.jdbc.Driver;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.MetricsConstants;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

public class DatabricksErrorLogging {
  private static DatabricksHttpClient telemetryClient;

  private static HttpPost getRequest(
      String workspaceId,
      String sqlQueryId,
      String timestamp,
      String driverVersion,
      String connectionConfig,
      int errorCode)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(MetricsConstants.ERROR_URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(MetricsConstants.WORKSPACE_ID, workspaceId);
    request.setHeader(MetricsConstants.SQL_QUERY_ID, sqlQueryId);
    request.setHeader(MetricsConstants.TIMESTAMP, timestamp);
    request.setHeader(MetricsConstants.DRIVER_VERSION, driverVersion);
    request.setHeader(MetricsConstants.CONNECTION_CONFIG, connectionConfig);
    request.setHeader(MetricsConstants.ERROR_CODE, String.valueOf(errorCode));
    return request;
  }

  private static void exportErrorLogToLogfood(
      IDatabricksConnectionContext context,
      String workspaceId,
      String sqlQueryId,
      String timestamp,
      String driverVersion,
      String connectionConfig,
      int errorCode) {
    telemetryClient = DatabricksHttpClient.getInstance(context);
    try {
      HttpPost request =
          getRequest(
              workspaceId, sqlQueryId, timestamp, driverVersion, connectionConfig, errorCode);
      CloseableHttpResponse response = telemetryClient.executeWithoutCertVerification(request);
      if (response == null) {
        LoggingUtil.log(LogLevel.DEBUG, "Response is null for error log export.");
      } else if (response.getStatusLine().getStatusCode() != 200) {
        LoggingUtil.log(
            LogLevel.DEBUG,
            "Response code for error log export: "
                + response.getStatusLine().getStatusCode()
                + " Response: "
                + response.getEntity().toString());
      } else {
        LoggingUtil.log(LogLevel.DEBUG, EntityUtils.toString(response.getEntity()));
        response.close();
      }
    } catch (Exception e) {
      LoggingUtil.log(LogLevel.DEBUG, e.getMessage());
    }
  }

  public static void exportError(
      IDatabricksConnectionContext connectionContext,
      String errorName,
      String sqlQueryId,
      int errorCode) {
    if (!connectionContext.enableTelemetry()) {
      return;
    }
    connectionContext.getMetricsExporter().increment(errorName + errorCode, 1);
    DatabricksErrorLogging.exportErrorLogToLogfood(
        connectionContext,
        connectionContext.getComputeResource().getWorkspaceId(),
        sqlQueryId,
        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        Driver.getVersion(),
        "None",
        errorCode);
    connectionContext.getMetricsExporter().close();
  }
}
