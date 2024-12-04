package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

class TelemetryPushTask implements Runnable {

  private static final JdbcLogger logger = JdbcLoggerFactory.getLogger(TelemetryPushTask.class);
  private List<TelemetryFrontendLog> queueToBePushed;
  private boolean isAuthenticated;
  private IDatabricksConnectionContext connectionContext;
  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Map<String, String> headers =

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
    logger.debug("Pushing ");
    TelemetryRequest request = new TelemetryRequest();
    try {
      request
              .setUploadTime(System.currentTimeMillis())
              .setProtoLogs(
                      queueToBePushed.isEmpty()
                              ? Optional.empty()
                              : Optional.of(
                              queueToBePushed.stream().map(objectMapper::writeValueAsString).collect(Collectors.toList())));
      IDatabricksHttpClient httpClient = DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      String uri = new URIBuilder(connectionContext.getHostUrl()).setPath(PathConstants.TELEMETRY_PATH_UNAUTHENTICATED).toString();
      HttpPost post = new HttpPost(uri);
      post.setEntity(new StringEntity(objectMapper.writeValueAsString(request), StandardCharsets.UTF_8));
      DatabricksJdbcConstants.JSON_HTTP_HEADERS.forEach(post::addHeader);

        try (CloseableHttpResponse response = httpClient.execute(post)) {
          // TODO: check response and add retry for partial failures
          logger.debug("");
        }

    } catch (JsonProcessingException e) {
      logger.error(e, "Failed to serialize Telemetry logs with error: {}", e.getMessage());
      return;
    } catch (DatabricksParsingException e) {
      logger.error(e, "Failed to get Host Url from connection with error: {}", e.getMessage());
      return;
    } catch (DatabricksHttpException e) {
      // Retry is already handled in HTTP client, we can return from here
      logger.error(e, "Failed to push Telemetry logs: {}", e.getMessage());
      return;
    }

    // Add handling for authenticated and unauthenticated push request
  }
}
