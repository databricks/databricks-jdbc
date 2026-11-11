package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;

/**
 * Caches the SDK's unauthenticated discovery GETs (host metadata and OIDC discovery) per URL.
 *
 * <p>Each connection builds its own {@code DatabricksConfig}, whose {@code resolve()} and default
 * credentials chain fetch these documents again on every connect. They do not change for a
 * workspace, so refetching only adds round trips and a TLS handshake to each connection.
 *
 * <p>Entries are also keyed by the client's TLS trust settings, so a document fetched under one
 * trust configuration is never served to a client configured differently. Truststores are
 * identified by path, so replacing a truststore file in place reaches cached documents within the
 * TTL.
 */
final class DiscoveryResponseCache {
  private static final String HOST_METADATA_SUFFIX = "/.well-known/databricks-config";
  private static final String OIDC_DISCOVERY_SUFFIX = "/.well-known/oauth-authorization-server";
  @VisibleForTesting static final long TTL_MILLIS = TimeUnit.HOURS.toMillis(1);
  // Short, so a transient 404 (e.g. from a gateway during a rollout) doesn't stick for an hour.
  @VisibleForTesting static final long NOT_FOUND_TTL_MILLIS = TimeUnit.MINUTES.toMillis(5);

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DiscoveryResponseCache.class);
  private static final Map<List<Object>, CachedResponse> CACHE = new ConcurrentHashMap<>();

  private DiscoveryResponseCache() {}

  static HttpClient wrap(HttpClient delegate, List<Object> trustSettingsKey) {
    // Monotonic, so wall-clock jumps can't stretch or cut short an entry's TTL.
    return wrap(delegate, trustSettingsKey, () -> TimeUnit.NANOSECONDS.toMillis(System.nanoTime()));
  }

  @VisibleForTesting
  static HttpClient wrap(
      HttpClient delegate, List<Object> trustSettingsKey, LongSupplier clockMillis) {
    return request -> {
      String url = discoveryUrl(request);
      if (url == null) {
        return delegate.execute(request);
      }
      List<Object> key = List.of(trustSettingsKey, url);
      CachedResponse cached = CACHE.get(key);
      long lookupMillis = clockMillis.getAsLong();
      if (cached != null && !cached.isExpired(lookupMillis)) {
        LOGGER.debug(
            "Using cached discovery response (HTTP {}, {} s old) for {}",
            cached.statusCode,
            (lookupMillis - cached.fetchedAtMillis) / 1000,
            url);
        return cached.toResponse(request);
      }
      Response response = delegate.execute(request);
      if (!isCacheable(url, response)) {
        return response;
      }
      long now = clockMillis.getAsLong();
      CachedResponse fresh = new CachedResponse(response, now);
      // Inserts are rare (about once per key per TTL), so this scan is cheap. It limits the
      // cache to hosts used recently.
      CACHE.values().removeIf(entry -> entry.isExpired(now));
      CACHE.put(key, fresh);
      LOGGER.debug("Cached discovery response (HTTP {}) for {}", fresh.statusCode, url);
      return fresh.toResponse(request);
    };
  }

  @VisibleForTesting
  static void clear() {
    CACHE.clear();
  }

  @VisibleForTesting
  static int size() {
    return CACHE.size();
  }

  private static String discoveryUrl(Request request) {
    if (!"GET".equalsIgnoreCase(request.getMethod())
        || !request.getQuery().isEmpty()
        || hasCredentials(request)) {
      return null;
    }
    String url = request.getUrl();
    if (url == null) {
      return null;
    }
    return url.endsWith(HOST_METADATA_SUFFIX) || url.endsWith(OIDC_DISCOVERY_SUFFIX) ? url : null;
  }

  // Responses are shared across users, so only cache requests that carry no credentials.
  private static boolean hasCredentials(Request request) {
    return request.getHeaders().keySet().stream()
        .anyMatch(
            name -> "Authorization".equalsIgnoreCase(name) || "Cookie".equalsIgnoreCase(name));
  }

  private static boolean isCacheable(String requestedUrl, Response response) {
    if (response.getUrl() == null
        || !URI.create(requestedUrl).getRawPath().equals(response.getUrl().getPath())) {
      return false;
    }
    // A 404 from the host-metadata endpoint means the host does not support it.
    if (response.getStatusCode() == 404) {
      return requestedUrl.endsWith(HOST_METADATA_SUFFIX);
    }
    if (response.getStatusCode() != 200 || response.getAllHeaders() == null) {
      return false;
    }
    String contentType = response.getFirstHeader("Content-Type");
    return contentType != null
        && "application/json".equalsIgnoreCase(contentType.split(";", 2)[0].trim());
  }

  private static final class CachedResponse {
    final URL url;
    final int statusCode;
    final String status;
    final Map<String, List<String>> headers;
    final String body;
    final long fetchedAtMillis;

    private CachedResponse(Response response, long fetchedAtMillis) throws IOException {
      try (InputStream in = response.getBody()) {
        this.body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      this.url = response.getUrl();
      this.statusCode = response.getStatusCode();
      this.status = response.getStatus();
      this.headers = readOnlyCopy(response.getAllHeaders());
      this.fetchedAtMillis = fetchedAtMillis;
    }

    // Every cache hit shares this map, so no caller may change it.
    private static Map<String, List<String>> readOnlyCopy(Map<String, List<String>> headers) {
      Map<String, List<String>> copy = new HashMap<>();
      if (headers != null) {
        headers.forEach(
            (name, values) ->
                copy.put(
                    name,
                    values == null ? null : Collections.unmodifiableList(new ArrayList<>(values))));
      }
      return Collections.unmodifiableMap(copy);
    }

    boolean isExpired(long nowMillis) {
      long ttlMillis = statusCode == 200 ? TTL_MILLIS : NOT_FOUND_TTL_MILLIS;
      return nowMillis - fetchedAtMillis >= ttlMillis;
    }

    Response toResponse(Request request) {
      return new Response(request, url, statusCode, status, headers, body);
    }
  }
}
