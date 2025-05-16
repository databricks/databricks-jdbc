package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/** This has context on dynamic feature flags that control the behavior of the driver */
public class DatabricksDriverFeatureFlagsContext {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksDriverFeatureFlagsContext.class);
  private static final String FEATURE_FLAGS_ENDPOINT =
      String.format("/2.0/connector-service/feature-flags/JDBC/%s", DriverUtil.getDriverVersion());

  private final IDatabricksConnectionContext connectionContext;
  private final Map<String, String> featureFlags;
  private final Map<String, String> authHeaders;

  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> authHeaders) {
    this.connectionContext = connectionContext;
    this.featureFlags = new HashMap<>();
    this.authHeaders = authHeaders;
    fetchFeatureFlags();
  }

  private void fetchFeatureFlags() {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      for (Map.Entry<String, String> entry : authHeaders.entrySet()) {
        request.addHeader(entry.getKey(), entry.getValue());
      }
      try (var response = httpClient.execute(request)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          String responseBody = EntityUtils.toString(response.getEntity());
          JsonNode root = JsonUtil.getMapper().readTree(responseBody);
          JsonNode flags = root.get("flags");

          if (flags != null && flags.isArray()) {
            for (JsonNode flag : flags) {
              String name = flag.get("name").asText();
              String value = flag.get("value").asText();
              featureFlags.put(name, value);
            }
          }
        } else {
          LOGGER.warn(
              "Failed to fetch feature flags. Status code: {}",
              response.getStatusLine().getStatusCode());
        }
      }
    } catch (DatabricksHttpException | IOException e) {
      LOGGER.warn("Error fetching feature flags: {}", e.getMessage());
    }
  }

  public boolean isFeatureEnabled(String name) {
    String value = featureFlags.get(name);
    return value != null && value.equalsIgnoreCase("true");
  }
}
