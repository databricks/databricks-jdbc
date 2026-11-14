package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.jdbc.common.util.JsonUtil;
import com.databricks.jdbc.log.JdbcLogger;
import com.databricks.jdbc.log.JdbcLoggerFactory;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.oauth.HostMetadata;
import com.databricks.sdk.core.oauth.OpenIDConnectEndpoints;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Ticker;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
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
import java.util.concurrent.TimeUnit;

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
  @VisibleForTesting static final int MAX_ENTRIES = 1024;

  private static final JdbcLogger LOGGER =
      JdbcLoggerFactory.getLogger(DiscoveryResponseCache.class);
  private static final DiscoveryResponseCache INSTANCE =
      new DiscoveryResponseCache(Ticker.systemTicker());
  private final Cache<List<Object>, CachedResponse> cache;

  @VisibleForTesting
  DiscoveryResponseCache(Ticker ticker) {
    cache =
        CacheBuilder.newBuilder()
            .expireAfterWrite(TTL_MILLIS, TimeUnit.MILLISECONDS)
            .maximumSize(MAX_ENTRIES)
            .ticker(ticker)
            .build();
  }

  static HttpClient wrap(HttpClient delegate, List<Object> trustSettingsKey) {
    return INSTANCE.wrapClient(delegate, trustSettingsKey);
  }

  @VisibleForTesting
  HttpClient wrapClient(HttpClient delegate, List<Object> trustSettingsKey) {
    return request -> {
      String url = discoveryUrl(request);
      if (url == null) {
        return delegate.execute(request);
      }
      List<Object> key = List.of(trustSettingsKey, url);
      CachedResponse cached = cache.getIfPresent(key);
      if (cached != null) {
        LOGGER.debug("Using cached discovery response (HTTP {}) for {}", cached.statusCode, url);
        return cached.toResponse(request);
      }
      Response response = delegate.execute(request);
      if (!isCacheable(url, response)) {
        return response;
      }
      CachedResponse fresh = new CachedResponse(response);
      if (canParseDiscovery(url, fresh.body)) {
        cache.put(key, fresh);
        LOGGER.debug("Cached discovery response (HTTP {}) for {}", fresh.statusCode, url);
      }
      return fresh.toResponse(request);
    };
  }

  @VisibleForTesting
  static void clear() {
    INSTANCE.cache.invalidateAll();
  }

  @VisibleForTesting
  long size() {
    cache.cleanUp();
    return cache.size();
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
    URI requestedUri = URI.create(requestedUrl);
    URL finalUrl = response.getUrl();
    if (finalUrl == null
        || !finalUrl.getProtocol().equalsIgnoreCase(requestedUri.getScheme())
        || !finalUrl.getHost().equalsIgnoreCase(requestedUri.getHost())
        || !finalUrl.getPath().equals(requestedUri.getRawPath())) {
      return false;
    }
    int requestedPort =
        requestedUri.getPort() == -1 ? finalUrl.getDefaultPort() : requestedUri.getPort();
    int finalPort = finalUrl.getPort() == -1 ? finalUrl.getDefaultPort() : finalUrl.getPort();
    if (requestedPort != finalPort
        || response.getStatusCode() != 200
        || response.getAllHeaders() == null) {
      return false;
    }
    String contentType = response.getFirstHeader("Content-Type");
    return contentType != null
        && "application/json".equalsIgnoreCase(contentType.split(";", 2)[0].trim());
  }

  private static boolean canParseDiscovery(String url, String body) {
    Class<?> responseType =
        url.endsWith(HOST_METADATA_SUFFIX) ? HostMetadata.class : OpenIDConnectEndpoints.class;
    try {
      return JsonUtil.getMapper().readValue(body, responseType) != null;
    } catch (IOException e) {
      // Let the SDK handle the parsing failure without retaining the invalid document.
      return false;
    }
  }

  private static final class CachedResponse {
    final URL url;
    final int statusCode;
    final String status;
    final Map<String, List<String>> headers;
    final String body;

    private CachedResponse(Response response) throws IOException {
      try (InputStream in = response.getBody()) {
        this.body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      this.url = response.getUrl();
      this.statusCode = response.getStatusCode();
      this.status = response.getStatus();
      this.headers = readOnlyCopy(response.getAllHeaders());
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

    Response toResponse(Request request) {
      return new Response(request, url, statusCode, status, headers, body);
    }
  }
}
