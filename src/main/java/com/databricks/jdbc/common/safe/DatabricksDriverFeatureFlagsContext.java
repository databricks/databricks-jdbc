package com.databricks.jdbc.common.safe;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.util.DriverUtil;
import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.common.util.UserAgentManager;
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
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
  private final String featureFlagEndpoint;
  private final IDatabricksConnectionContext connectionContext;
  private final Cache<String, String> featureFlags;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "databricks-jdbc-feature-flags-refresh");
            t.setDaemon(true);
            return t;
          });
  private ScheduledFuture<?> scheduledRefreshTask;
  private volatile int refreshIntervalSeconds = DEFAULT_TTL_SECONDS;

  public DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this.connectionContext = connectionContext;
    this.featureFlags = CacheBuilder.newBuilder().build();
    this.featureFlagEndpoint =
        String.format(
            "https://%s%s", connectionContext.getHostForOAuth(), FEATURE_FLAGS_ENDPOINT_SUFFIX);
    // Make an initial blocking call to fetch featureFlags
    refreshAllFeatureFlags();
    // Async fetch eventually
    scheduleOrRescheduleRefresh(DEFAULT_TTL_SECONDS);
  }

  // Constructor for testing
  DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, Map<String, String> initialFlags) {
    this.connectionContext = connectionContext;
    this.featureFlags = CacheBuilder.newBuilder().build();
    this.featureFlagEndpoint =
        String.format(
            "https://%s%s", connectionContext.getHostForOAuth(), FEATURE_FLAGS_ENDPOINT_SUFFIX);
    initialFlags.forEach(this.featureFlags::put);
    scheduleOrRescheduleRefresh(DEFAULT_TTL_SECONDS);
  }

  private void scheduleOrRescheduleRefresh(int ttlSeconds) {
    this.refreshIntervalSeconds = ttlSeconds > 0 ? ttlSeconds : DEFAULT_TTL_SECONDS;
    if (scheduler.isShutdown()) {
      return;
    }
    if (scheduledRefreshTask != null && !scheduledRefreshTask.isCancelled()) {
      scheduledRefreshTask.cancel(false);
    }
    // Schedule refresh at a fixed rate.
    scheduledRefreshTask =
        scheduler.scheduleAtFixedRate(
            this::refreshAllFeatureFlags,
            this.refreshIntervalSeconds,
            this.refreshIntervalSeconds,
            TimeUnit.SECONDS);
  }

  private void refreshAllFeatureFlags() {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(connectionContext);
      HttpGet request = new HttpGet(featureFlagEndpoint);

      // Set custom User-Agent for connector service (includes custom user agent without client
      // type)
      String userAgent = UserAgentManager.buildUserAgentForConnectorService(connectionContext);
      request.setHeader("User-Agent", userAgent);

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
          scheduleOrRescheduleRefresh(ttlSeconds);
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
    String value = featureFlags.getIfPresent(name);
    return Boolean.parseBoolean(value);
  }

  public void shutdown() {
    ScheduledFuture<?> task = scheduledRefreshTask;
    if (task != null) {
      task.cancel(false);
    }
    scheduler.shutdown();
    try {
      if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
        scheduler.shutdownNow();
      }
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      scheduler.shutdownNow();
    }
  }
}
