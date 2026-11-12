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
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.hash.Hashing;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
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
  private static final int MAX_SNAPSHOTS = 128;

  /**
   * Last successful response per request shape, so a context created after the previous one for the
   * same host shut down can skip the initial fetch while the response is recent.
   */
  private static final Cache<String, FlagsSnapshot> SNAPSHOTS =
      CacheBuilder.newBuilder().maximumSize(MAX_SNAPSHOTS).build();

  private final String featureFlagEndpoint;
  private volatile IDatabricksConnectionContext connectionContext;
  private final Cache<String, String> featureFlags;
  private final ScheduledExecutorService scheduler =
      Executors.newSingleThreadScheduledExecutor(
          r -> {
            Thread t = new Thread(r, "databricks-jdbc-feature-flags-refresh");
            t.setDaemon(true);
            return t;
          });
  private volatile ScheduledFuture<?> scheduledRefreshTask;
  private final Object initLock = new Object();
  private volatile boolean initialized;
  private volatile Thread initializingThread;

  public DatabricksDriverFeatureFlagsContext(IDatabricksConnectionContext connectionContext) {
    this(connectionContext, /* fetchNow= */ true);
  }

  private DatabricksDriverFeatureFlagsContext(
      IDatabricksConnectionContext connectionContext, boolean fetchNow) {
    this.connectionContext = connectionContext;
    this.featureFlags = CacheBuilder.newBuilder().build();
    this.featureFlagEndpoint =
        String.format(
            "https://%s%s", connectionContext.getHostForOAuth(), FEATURE_FLAGS_ENDPOINT_SUFFIX);
    if (fetchNow) {
      ensureInitialized(connectionContext);
    }
  }

  /** Creates a context whose initial fetch is deferred until the first caller initializes it. */
  static DatabricksDriverFeatureFlagsContext createUninitialized(
      IDatabricksConnectionContext connectionContext) {
    return new DatabricksDriverFeatureFlagsContext(connectionContext, /* fetchNow= */ false);
  }

  /**
   * Loads flags once, from an unexpired snapshot of the same request or else with a blocking fetch,
   * and schedules periodic refresh. Concurrent callers wait for the first attempt, including any
   * required authentication.
   *
   * <p>Re-entrant telemetry lookups during configurator creation or flag fetching see defaults
   * without completing initialization.
   */
  void ensureInitialized(IDatabricksConnectionContext callerContext) {
    if (initialized || initializingThread == Thread.currentThread()) {
      return;
    }
    if (DatabricksClientConfiguratorManager.getInstance()
        .isCreatingConfiguratorOnCurrentThread(callerContext)) {
      return;
    }
    synchronized (initLock) {
      if (initialized) {
        return;
      }
      initializingThread = Thread.currentThread();
      try {
        if (!seedFromSnapshot(callerContext)) {
          // The stored context may belong to another thread holding its configurator lock.
          refreshAllFeatureFlags(callerContext);
          scheduleOrRescheduleRefresh(DEFAULT_TTL_SECONDS);
        }
        initialized = true;
      } finally {
        initializingThread = null;
      }
    }
  }

  /**
   * Loads the caller's last fetched flags if they are unexpired and schedules the first refresh for
   * when they expire, then refreshes as a newly fetched context does. Returns false, so the caller
   * fetches, if there is no usable snapshot or the lookup fails.
   */
  private boolean seedFromSnapshot(IDatabricksConnectionContext callerContext) {
    try {
      String key =
          snapshotKey(
              featureFlagEndpoint,
              UserAgentManager.buildUserAgentForConnectorService(callerContext),
              callerContext.getCustomHeaders());
      FlagsSnapshot snapshot = SNAPSHOTS.getIfPresent(key);
      if (snapshot == null) {
        return false;
      }
      long remainingNanos = snapshot.expiresAtNanos - System.nanoTime();
      if (remainingNanos <= 0) {
        SNAPSHOTS.asMap().remove(key, snapshot);
        return false;
      }
      snapshot.flags.forEach(featureFlags::put);
      long remainingSeconds = TimeUnit.NANOSECONDS.toSeconds(remainingNanos);
      scheduleRefresh(remainingSeconds, DEFAULT_TTL_SECONDS);
      LOGGER.debug(
          "Reusing feature flags fetched for {} (next refresh in {}s)",
          featureFlagEndpoint,
          remainingSeconds);
      return true;
    } catch (RuntimeException e) {
      LOGGER.trace("Feature flag snapshot lookup failed: {}", e.getMessage());
      return false;
    }
  }

  /**
   * Identifies everything the request sends except credentials. The server evaluates flags per
   * workspace (the host, or the org-id header on shared hosts), driver version, and User-Agent.
   * Hashed so custom header values are not retained after their connections close.
   */
  @VisibleForTesting
  static String snapshotKey(String endpoint, String userAgent, Map<String, String> customHeaders) {
    StringBuilder raw = new StringBuilder(endpoint).append('\0').append(userAgent);
    if (customHeaders != null) {
      new TreeMap<>(customHeaders)
          .forEach((name, value) -> raw.append('\0').append(name).append('\0').append(value));
    }
    return Hashing.sha256().hashString(raw, StandardCharsets.UTF_8).toString();
  }

  @VisibleForTesting
  static void clearSnapshots() {
    SNAPSHOTS.invalidateAll();
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
    this.initialized = true;
  }

  private void scheduleOrRescheduleRefresh(int ttlSeconds) {
    int interval = ttlSeconds > 0 ? ttlSeconds : DEFAULT_TTL_SECONDS;
    scheduleRefresh(interval, interval);
  }

  private void scheduleRefresh(long initialDelaySeconds, int intervalSeconds) {
    if (scheduler.isShutdown()) {
      return;
    }
    if (scheduledRefreshTask != null && !scheduledRefreshTask.isCancelled()) {
      scheduledRefreshTask.cancel(false);
    }
    // Schedule refresh at a fixed rate.
    try {
      scheduledRefreshTask =
          scheduler.scheduleAtFixedRate(
              this::refreshAllFeatureFlags, initialDelaySeconds, intervalSeconds, TimeUnit.SECONDS);
    } catch (RejectedExecutionException e) {
      if (!scheduler.isShutdown()) {
        throw e;
      }
    }
  }

  private void refreshAllFeatureFlags() {
    refreshAllFeatureFlags(connectionContext);
  }

  private void refreshAllFeatureFlags(IDatabricksConnectionContext context) {
    try {
      IDatabricksHttpClient httpClient =
          DatabricksHttpClientFactory.getInstance().getClient(context);
      HttpGet request = new HttpGet(featureFlagEndpoint);

      // Set custom User-Agent for connector service (includes custom user agent without client
      // type)
      String userAgent = UserAgentManager.buildUserAgentForConnectorService(context);
      request.setHeader("User-Agent", userAgent);

      DatabricksClientConfiguratorManager.getInstance()
          .getConfigurator(context)
          .getDatabricksConfig()
          .authenticate()
          .forEach(request::addHeader);
      context.getCustomHeaders().forEach(request::addHeader);
      FlagsSnapshot fetched = fetchAndSetFlagsFromServer(httpClient, request, context);
      if (fetched != null) {
        SNAPSHOTS.put(
            snapshotKey(featureFlagEndpoint, userAgent, context.getCustomHeaders()), fetched);
      }
    } catch (Exception e) {
      LOGGER.trace(
          "Error fetching feature flags for context: {}. Error: {}", context, e.getMessage());
    }
  }

  /** Returns the applied flags on a successful response, otherwise null. */
  @VisibleForTesting
  FlagsSnapshot fetchAndSetFlagsFromServer(
      IDatabricksHttpClient httpClient, HttpGet request, IDatabricksConnectionContext context)
      throws DatabricksHttpException, IOException {
    long requestStartNanos = System.nanoTime();
    try (CloseableHttpResponse response = httpClient.execute(request)) {
      if (response.getStatusLine().getStatusCode() == 200) {
        String responseBody = EntityUtils.toString(response.getEntity());
        FeatureFlagsResponse featureFlagsResponse =
            JsonUtil.getMapper().readValue(responseBody, FeatureFlagsResponse.class);
        featureFlags.invalidateAll();
        Map<String, String> applied = new HashMap<>();
        if (featureFlagsResponse.getFlags() != null) {
          for (FeatureFlagsResponse.FeatureFlagEntry flag : featureFlagsResponse.getFlags()) {
            featureFlags.put(flag.getName(), flag.getValue());
            applied.put(flag.getName(), flag.getValue());
          }
        }
        LOGGER.debug(
            "Feature flags from connector-service: endpoint={}, {}",
            request.getURI(),
            featureFlagsResponse);

        Integer ttlSeconds = featureFlagsResponse.getTtlSeconds();
        if (ttlSeconds != null) {
          scheduleOrRescheduleRefresh(ttlSeconds);
        }
        return new FlagsSnapshot(applied, ttlSeconds, requestStartNanos);
      }
      LOGGER.trace(
          "Failed to fetch feature flags. Context: {}, Status code: {}",
          context,
          response.getStatusLine().getStatusCode());
      return null;
    }
  }

  IDatabricksConnectionContext getConnectionContext() {
    return connectionContext;
  }

  void updateConnectionContext(IDatabricksConnectionContext newContext) {
    this.connectionContext = newContext;
  }

  /** Returns true only for a case-insensitive "true" value; missing values default to false. */
  public boolean getBoolean(String name) {
    String value = featureFlags.getIfPresent(name);
    return Boolean.parseBoolean(value);
  }

  public OptionalInt getInt32(String name) {
    JsonNode value = parse(name);
    return value != null && value.isIntegralNumber() && value.canConvertToInt()
        ? OptionalInt.of(value.intValue())
        : OptionalInt.empty();
  }

  public OptionalLong getInt64(String name) {
    JsonNode value = parse(name);
    return value != null && value.isIntegralNumber() && value.canConvertToLong()
        ? OptionalLong.of(value.longValue())
        : OptionalLong.empty();
  }

  public OptionalDouble getDouble(String name) {
    JsonNode value = parse(name);
    if (value == null || !value.isNumber()) {
      return OptionalDouble.empty();
    }
    double number = value.doubleValue();
    return Double.isFinite(number) ? OptionalDouble.of(number) : OptionalDouble.empty();
  }

  public Optional<String> getString(String name) {
    JsonNode value = parse(name);
    return value != null && value.isTextual() ? Optional.of(value.textValue()) : Optional.empty();
  }

  public Optional<List<String>> getStringList(String name) {
    JsonNode value = parse(name);
    if (value == null || !value.isArray()) {
      return Optional.empty();
    }
    List<String> result = new ArrayList<>(value.size());
    for (JsonNode item : value) {
      if (!item.isTextual()) {
        return Optional.empty();
      }
      result.add(item.textValue());
    }
    return Optional.of(Collections.unmodifiableList(result));
  }

  private JsonNode parse(String name) {
    if (name == null || name.isEmpty()) {
      return null;
    }
    String value = featureFlags.getIfPresent(name);
    if (value == null) {
      return null;
    }
    try {
      return JsonUtil.getMapper().readTree(value);
    } catch (IOException | RuntimeException e) {
      LOGGER.debug("Feature flag {} has malformed JSON; using the consumer default", name);
      return null;
    }
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

  static final class FlagsSnapshot {
    final Map<String, String> flags;
    final long expiresAtNanos;

    /**
     * Expires when a newly fetched context would first refresh (the default interval, or the
     * server's TTL if shorter), counted from when the request was sent.
     */
    FlagsSnapshot(Map<String, String> flags, Integer ttlSeconds, long requestStartNanos) {
      this.flags = Collections.unmodifiableMap(flags);
      int lifetimeSeconds =
          ttlSeconds != null && ttlSeconds > 0
              ? Math.min(ttlSeconds, DEFAULT_TTL_SECONDS)
              : DEFAULT_TTL_SECONDS;
      this.expiresAtNanos = requestStartNanos + TimeUnit.SECONDS.toNanos(lifetimeSeconds);
    }
  }
}
