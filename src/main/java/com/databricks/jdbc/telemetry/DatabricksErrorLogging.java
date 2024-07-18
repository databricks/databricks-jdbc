package com.databricks.jdbc.telemetry;

import com.databricks.client.jdbc.Driver;
import com.databricks.jdbc.client.http.DatabricksHttpClient;
import com.databricks.jdbc.commons.LogLevel;
import com.databricks.jdbc.commons.util.LoggingUtil;
import com.databricks.jdbc.core.ArrowResultChunk;
import com.databricks.jdbc.core.DatabricksSQLException;
import com.databricks.jdbc.core.IDatabricksSession;
import com.databricks.jdbc.driver.IDatabricksConnectionContext;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class DatabricksErrorLogging {
  private static final String URL =
      "https://aa87314c1e33d4c1f91a919f8cf9c4ba-387609431.us-west-2.elb.amazonaws.com:443/api/2.0/oss-sql-driver-telemetry/logs";

  private static final String WORKSPACE_ID = "workspace_id";
  private static final String SQL_QUERY_ID = "sql_query_id";
  private static final String TIMESTAMP = "timestamp";
  private static final String DRIVER_VERSION = "driver_version";
  private static final String CONNECTION_CONFIG = "connection_config";
  private static final String ERROR_CODE = "error_code";
  private static DatabricksHttpClient telemetryClient;
  private static boolean enableTelemetry = false;

  private static HttpPost getRequest(
      String workspaceId,
      String sqlQueryId,
      String timestamp,
      String driverVersion,
      String connectionConfig,
      int errorCode)
      throws Exception {
    URIBuilder uriBuilder = new URIBuilder(URL);
    HttpPost request = new HttpPost(uriBuilder.build());
    request.setHeader(WORKSPACE_ID, workspaceId);
    request.setHeader(SQL_QUERY_ID, sqlQueryId);
    request.setHeader(TIMESTAMP, timestamp);
    request.setHeader(DRIVER_VERSION, driverVersion);
    request.setHeader(CONNECTION_CONFIG, connectionConfig);
    request.setHeader(ERROR_CODE, String.valueOf(errorCode));
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
    enableTelemetry = context.enableTelemetry();
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

  public static void exportChunkDownloadErrorLogAndErrorMetric(IDatabricksSession session, String sqlQueryId, String chunkStatus, int chunkError) throws DatabricksSQLException {
    if(enableTelemetry){
      IDatabricksConnectionContext connectionContext = session.getConnectionContext();
      connectionContext.getMetricsExporter().increment("CHUNK_" + chunkStatus, 1);
      DatabricksErrorLogging.exportErrorLogToLogfood(
              connectionContext,
              connectionContext.getComputeResource().getWorkspaceId(),
              sqlQueryId,
              LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
              Driver.getVersion(),
              "connection_config0",
              chunkError);
      connectionContext.getMetricsExporter().close();
    }
  }
}
