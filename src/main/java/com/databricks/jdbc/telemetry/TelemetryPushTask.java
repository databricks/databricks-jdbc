package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.exception.DatabricksParsingException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import com.databricks.jdbc.model.telemetry.TelemetryResponse;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

class TelemetryPushTask implements Runnable {

  private static final JdbcLogger logger = JdbcLoggerFactory.getLogger(TelemetryPushTask.class);
  private final List<TelemetryFrontendLog> queueToBePushed;
  private final boolean isAuthenticated;
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final ObjectMapper objectMapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  TelemetryPushTask(
      List<TelemetryFrontendLog> eventsQueue,
      boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext,
      DatabricksConfig databricksConfig) {
    this.queueToBePushed = eventsQueue;
    this.isAuthenticated = isAuthenticated;
    this.connectionContext = connectionContext;
    this.databricksConfig = databricksConfig;
  }

  @Override
  public void run() {
    logger.debug("Pushing Telemetry logs of size " + queueToBePushed.size());
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
                          return objectMapper.writeValueAsString(event);
                        } catch (JsonProcessingException e) {
                          logger.error(
                              "Failed to serialize Telemetry event {} with error: {}", event, e);
                          return null; // Return null for failed serialization
                        }
                      })
                  .filter(Objects::nonNull) // Remove nulls from failed serialization
                  .collect(Collectors.toList()));
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      String path =
          isAuthenticated
              ? PathConstants.TELEMETRY_PATH
              : PathConstants.TELEMETRY_PATH_UNAUTHENTICATED;
      String uri = new URIBuilder(connectionContext.getHostUrl()).setPath(path).toString();
      HttpPost post = new HttpPost(uri);
      post.setEntity(
          new StringEntity(objectMapper.writeValueAsString(request), StandardCharsets.UTF_8));
      DatabricksJdbcConstants.JSON_HTTP_HEADERS.forEach(post::addHeader);
      Map<String, String> authHeaders =
          isAuthenticated ? databricksConfig.authenticate() : Collections.emptyMap();
      authHeaders.forEach(post::addHeader);

      try (CloseableHttpResponse response = httpClient.execute(post)) {
        // TODO: check response and add retry for partial failures
        if (!HttpUtil.isSuccessfulHttpResponse(response)) {
          logger.trace(
              "Failed to push telemetry logs with error response: [%s]", response.getStatusLine());
          return;
        }
        TelemetryResponse telResponse =
            objectMapper.readValue(
                EntityUtils.toString(response.getEntity()), TelemetryResponse.class);
        if (queueToBePushed.size() != telResponse.getNumProtoSuccess()) {
          logger.error(
              "Partial failure while pushing telemetry logs with error response: [%s], request count: [%d], upload count: [%d]",
              telResponse.getErrors(), queueToBePushed.size(), telResponse.getNumProtoSuccess());
          return;
        }
      }

    } catch (URISyntaxException | DatabricksParsingException e) {
      logger.error(e, "Failed to get Host Url from connection with error: {}", e.getMessage());
      return;
    } catch (DatabricksHttpException | IOException e) {
      // Retry is already handled in HTTP client, we can return from here
      logger.error(e, "Failed to push Telemetry logs: {}", e.getMessage());
      return;
    }

    // Add handling for authenticated and unauthenticated push request
  }
}
