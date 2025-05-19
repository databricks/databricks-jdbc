package com.databricks.jdbc.common;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.auth.DatabricksAuthClientFactory;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import java.util.HashMap;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
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

  DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.featureFlags = new HashMap<>();
    fetchFeatureFlags();
  }

  // This constructor is only for testing
  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    this.connectionContext = connectionContext;
    this.featureFlags = featureFlags;
  }

  private void fetchFeatureFlags() {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      DatabricksAuthClientFactory.getInstance()
          .getConfigurator(connectionContext)
          .getDatabricksConfig()
          .authenticate()
          .forEach(request::addHeader);

      try (CloseableHttpResponse response = httpClient.execute(request)) {
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
              "Failed to fetch feature flags for connectionContext: {}. Status code: {}",
              connectionContext,
              response.getStatusLine().getStatusCode());
        }
      }
    } catch (Exception e) {
      LOGGER.warn(
          "Error fetching feature flags for connectionContext: {}. Error: {}",
          connectionContext,
          e.getMessage());
    }
  }

  public boolean isFeatureEnabled(String name) {
    return Boolean.parseBoolean(featureFlags.get(name));
  }
}
