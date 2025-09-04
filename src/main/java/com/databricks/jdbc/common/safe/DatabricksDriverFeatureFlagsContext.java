package com.databricks.jdbc.common.safe;

import static java.lang.Math.max;

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
import com.google.common.cache.*;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;

/** Context for dynamic feature flags that control the behavior of the driver. */
public class DatabricksDriverFeatureFlagsContext {
  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DatabricksDriverFeatureFlagsContext.class);
  private static final String FEATURE_FLAGS_ENDPOINT_SUFFIX =
      String.format(
          "/api/2.0/connector-service/feature-flags/OSS_JDBC/%s",
          DriverUtil.getDriverVersionWithoutOSSSuffix());
  private static final int DEFAULT_TTL_SECONDS = 900; // 15 minutes
  private static final int REFRESH_BEFORE_EXPIRY_SECONDS = 10; // refresh 10s before expiry
  private final String featureFlagEndpoint;
  private final IDatabricksConnectionContext connectionContext;
  private LoadingCache<String, String> featureFlags;
  private final ExecutorService asyncExecutor = Executors.newCachedThreadPool();

  public DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.featureFlags = createFeatureFlagsCache(DEFAULT_TTL_SECONDS);
    this.featureFlagEndpoint =
        String.format(
            "https://%s%s", connectionContext.getHostForOAuth(), FEATURE_FLAGS_ENDPOINT_SUFFIX);
  }

  // Constructor for testing
  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> initialFlags) {
    this.connectionContext = connectionContext;
    this.featureFlags = createFeatureFlagsCache(DEFAULT_TTL_SECONDS);
    this.featureFlagEndpoint =
        String.format(
            "https://%s%s", connectionContext.getHostForOAuth(), FEATURE_FLAGS_ENDPOINT_SUFFIX);
    initialFlags.forEach(this.featureFlags::put);
  }

  private LoadingCache<String, String> createFeatureFlagsCache(int ttlSeconds) {
    return CacheBuilder.newBuilder()
        .expireAfterWrite(ttlSeconds, TimeUnit.SECONDS)
        .refreshAfterWrite(
            max(
                300,
                ttlSeconds
                    - REFRESH_BEFORE_EXPIRY_SECONDS), // refresh time should be minimum 5 minutes
            TimeUnit.SECONDS)
        .build(
            new CacheLoader<String, String>() {
              @Override
              public String load(String key) {
                refreshAllFeatureFlags();
                return featureFlags.getIfPresent(key) != null
                    ? featureFlags.getIfPresent(key)
                    : "false";
              }

              @Override
              public ListenableFuture<String> reload(String key, String oldValue) {
                asyncExecutor.submit(() -> refreshAllFeatureFlags());
                return Futures.immediateFuture(oldValue); // keep old value until refresh is done
              }
            });
  }

  private void refreshAllFeatureFlags() {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      HttpGet request = new HttpGet(featureFlagEndpoint);
      DatabricksClientConfiguratorManager.getInstance()
          .getConfigurator(connectionContext)
          .getDatabricksConfig()
          .authenticate()
          .forEach(request::addHeader);
      fetchAndSetFlagsFromServer(httpClient, request);
    } catch (Exception e) {
      LOGGER.trace(
          "Error fetching feature flags for context: {}. Error: {}",
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
        featureFlags.invalidateAll();
        if (featureFlagsResponse.getFlags() != null) {
          for (FeatureFlagsResponse.FeatureFlagEntry flag : featureFlagsResponse.getFlags()) {
            featureFlags.put(flag.getName(), flag.getValue());
          }
        }

        Integer ttlSeconds = featureFlagsResponse.getTtlSeconds();
        if (ttlSeconds != null) {
          featureFlags = createFeatureFlagsCache(ttlSeconds);
        }
      } else {
        LOGGER.trace(
            "Failed to fetch feature flags. Context: {}, Status code: {}",
            connectionContext,
            response.getStatusLine().getStatusCode());
      }
    }
  }

  public boolean isFeatureEnabled(String name) {
    try {
      return Boolean.parseBoolean(featureFlags.get(name));
    } catch (Exception e) {
      LOGGER.trace("Error fetching feature flag {}: {}", name, e.getMessage());
      return false;
    }
  }
}
