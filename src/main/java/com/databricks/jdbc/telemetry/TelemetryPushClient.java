package com.databricks.jdbc.telemetry;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksJdbcConstants;
import com.databricks.jdbc.common.util.HttpUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.dbclient.impl.sqlexec.PathConstants;
import com.databricks.jdbc.exception.DatabricksTelemetryException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.jdbc.model.telemetry.TelemetryRequest;
import com.databricks.jdbc.model.telemetry.TelemetryResponse;
import com.databricks.sdk.core.DatabricksConfig;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

public class TelemetryPushClient implements ITelemetryPushClient {

  private static final JdbcLogger LOGGER = JdbcLoggerFactory.getLogger(TelemetryPushClient.class);

  private static final String REQUEST_ID_HEADER = "x-request-id";
  private final boolean isAuthenticated;
  private final IDatabricksConnectionContext connectionContext;
  private final DatabricksConfig databricksConfig;
  private final ObjectMapper objectMapper =
      new ObjectMapper().setSerializationInclusion(JsonInclude.Include.NON_NULL);

  public TelemetryPushClient(
      boolean isAuthenticated,
      IDatabricksConnectionContext connectionContext,
      DatabricksConfig databricksConfig) {
    this.isAuthenticated = isAuthenticated;
    this.connectionContext = connectionContext;
    this.databricksConfig = databricksConfig;
  }

  @Override
  public void pushEvent(TelemetryRequest request) throws Exception {
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
        LOGGER.trace(
            "Failed to push telemetry logs with error response: {}", response.getStatusLine());
        if (connectionContext.isTelemetryCircuitBreakerEnabled()) {
          throw new DatabricksTelemetryException(
              "Telemetry push failed with response: " + response.getStatusLine());
        } else {
          return;
        }
      }
      TelemetryResponse telResponse =
          objectMapper.readValue(
              EntityUtils.toString(response.getEntity()), TelemetryResponse.class);
      LOGGER.trace(
          "Pushed Telemetry logs with request-Id {} with events {} with error count {}",
          response.getFirstHeader(REQUEST_ID_HEADER),
          telResponse.getNumProtoSuccess(),
          telResponse.getErrors().size());
      if (!telResponse.getErrors().isEmpty()) {
        LOGGER.trace("Failed to push telemetry logs with error: {}", telResponse.getErrors());
      }
      if (request.getProtoLogs().size() != telResponse.getNumProtoSuccess()) {
        LOGGER.debug(
            "Partial failure while pushing telemetry logs with error response: {}, request count: {}, upload count: {}",
            telResponse.getErrors(),
            request.getProtoLogs().size(),
            telResponse.getNumProtoSuccess());
      }
    } catch (Exception e) {
      LOGGER.debug(
          "Failed to push telemetry logs with error: {}, request: {}",
          e.getMessage(),
          objectMapper.writeValueAsString(request));
      if (connectionContext.isTelemetryCircuitBreakerEnabled()) {
        throw new DatabricksTelemetryException("Exception while pushing telemetry logs", e);
      }
    }
  }
}
