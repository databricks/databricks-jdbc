package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/** This has context on dynamic feature flags that control the behavior of the driver */
public class DatabricksDriverFeatureFlagsContext {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksDriverFeatureFlagsContext.class);
  static final String FEATURE_FLAGS_ENDPOINT =
      String.format("/2.0/connector-service/feature-flags/JDBC/%s", DriverUtil.getDriverVersion());
  private static final int DEFAULT_TTL_SECONDS = 900; // 15 minutes default TTL

  private final IDatabricksConnectionContext connectionContext;
  private Cache<String, String> featureFlags;

  DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.featureFlags =
        CacheBuilder.newBuilder().expireAfterWrite(DEFAULT_TTL_SECONDS, TimeUnit.SECONDS).build();
    fetchFeatureFlags();
  }

  // This constructor is only for testing
  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> featureFlags) {
    this.connectionContext = connectionContext;
    this.featureFlags =
        CacheBuilder.newBuilder()
            .expireAfterWrite(Long.MAX_VALUE, TimeUnit.SECONDS) // Never expire for testing
            .build();
    featureFlags.forEach(this.featureFlags::put);
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
      fetchAndSetFlagsFromServer(httpClient, request);
    } catch (Exception e) {
      LOGGER.warn(
          "Error fetching feature flags for connectionContext: {}. Error: {}",
          connectionContext,
          e.getMessage());
    }
  }

  @VisibleForTesting
  void fetchAndSetFlagsFromServer(IDatabricksHttpClient httpClient, HttpGet request)
      throws DatabricksHttpException, IOException {
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String responseBody = EntityUtils.toString(response.getEntity());
        FeatureFlagsResponse featureFlagsResponse =
            JsonUtil.getMapper().readValue(responseBody, FeatureFlagsResponse.class);

        if (featureFlagsResponse.getFlags() != null) {
          // Clear existing flags
          featureFlags.invalidateAll();

          // Add new flags
          for (FeatureFlagsResponse.FeatureFlagEntry flag : featureFlagsResponse.getFlags()) {
            featureFlags.put(flag.getName(), flag.getValue());
          }
        }

        // Update TTL if provided in response
        Integer ttlSeconds = featureFlagsResponse.getTtlSeconds();
        if (ttlSeconds != null) {
          Cache<String, String> newCache =
              CacheBuilder.newBuilder().expireAfterWrite(ttlSeconds, TimeUnit.SECONDS).build();
          featureFlags.asMap().forEach(newCache::put);
          featureFlags = newCache; // Replace the entire cache instance
        }

      } else {
        LOGGER.warn(
            "Failed to fetch feature flags for connectionContext: {}. Status code: {}",
            connectionContext,
            response.getStatusLine().getStatusCode());
      }
    }
  }

  public boolean isFeatureEnabled(String name) {
    String value = featureFlags.getIfPresent(name);
    if (value == null) {
      fetchFeatureFlags(); // Refresh flags if not present
      value = featureFlags.getIfPresent(name);
    }
    return Boolean.parseBoolean(value);
  }
}
