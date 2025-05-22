package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
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
  private static final int DEFAULT_TTL_SECONDS = 900; // 15 minutes default TTL

  private final IDatabricksConnectionContext connectionContext;
  private Map<String, String> featureFlags;
  private long expiryTimestamp;

  DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.featureFlags = new HashMap<>();
    this.expiryTimestamp = System.currentTimeMillis() + (DEFAULT_TTL_SECONDS * 1000L);
    fetchFeatureFlags();
  }

  // This constructor is only for testing
  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    this.connectionContext = connectionContext;
    this.featureFlags = featureFlags;
    // For testing, set expiry to a far future time
    this.expiryTimestamp = Long.MAX_VALUE;
  }

  private void fetchFeatureFlags() {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      HttpGet request = new HttpGet(FEATURE_FLAGS_ENDPOINT);
      DatabricksClientConfiguratorManager.getInstance()
          .getConfigurator(connectionContext)
          .getDatabricksConfig()
          .authenticate()
          .forEach(request::addHeader);

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        if (response.getStatusLine().getStatusCode() == 200) {
          String responseBody = EntityUtils.toString(response.getEntity());
          FeatureFlagsResponse featureFlagsResponse =
              JsonUtil.getMapper().readValue(responseBody, FeatureFlagsResponse.class);

          if (featureFlagsResponse.getFlags() != null) {
            for (FeatureFlagsResponse.FeatureFlagEntry flag : featureFlagsResponse.getFlags()) {
              featureFlags.put(flag.getName(), flag.getValue());
            }
          }

          // Update expiry timestamp based on TTL from response
          Integer ttlSeconds = featureFlagsResponse.getTtlSeconds();
          if (ttlSeconds != null) {
            this.expiryTimestamp = System.currentTimeMillis() + (ttlSeconds * 1000L);
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
    if (System.currentTimeMillis() > expiryTimestamp) {
      fetchFeatureFlags(); // Refresh flags if expired
    }
    return Boolean.parseBoolean(featureFlags.get(name));
  }
}
