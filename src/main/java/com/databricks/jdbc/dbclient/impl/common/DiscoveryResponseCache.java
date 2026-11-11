package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
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
 * <p>Entries are also keyed by the client's TLS trust settings, so a document fetched by a client
 * that trusts more servers is never served to one that trusts fewer.
 */
final class DiscoveryResponseCache {
  private static final String HOST_METADATA_SUFFIX = "/.well-known/databricks-config";
  private static final String OIDC_DISCOVERY_SUFFIX = "/.well-known/oauth-authorization-server";
  @VisibleForTesting static final long TTL_MILLIS = TimeUnit.HOURS.toMillis(1);

  private static final Map<List<String>, CachedResponse> CACHE = new ConcurrentHashMap<>();

  private DiscoveryResponseCache() {}

  static HttpClient wrap(HttpClient delegate, String trustSettingsKey) {
    return wrap(delegate, trustSettingsKey, System::currentTimeMillis);
  }

  @VisibleForTesting
  static HttpClient wrap(HttpClient delegate, String trustSettingsKey, LongSupplier clockMillis) {
    return request -> {
      String url = discoveryUrl(request);
      if (url == null) {
        return delegate.execute(request);
      }
      List<String> key = List.of(trustSettingsKey, url);
      CachedResponse cached = CACHE.get(key);
      if (cached != null && !cached.isExpired(clockMillis.getAsLong())) {
        return cached.toResponse(request);
      }
      Response response = delegate.execute(request);
      if (!isCacheable(url, response.getStatusCode())) {
        return response;
      }
      long now = clockMillis.getAsLong();
      CachedResponse fresh = CachedResponse.from(response, now);
      // Inserts are rare (about once per key per TTL), so this scan is cheap. It limits the
      // cache to hosts used within the last TTL.
      CACHE.values().removeIf(entry -> entry.isExpired(now));
      CACHE.put(key, fresh);
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
    if (!"GET".equalsIgnoreCase(request.getMethod()) || !request.getQuery().isEmpty()) {
      return null;
    }
    String url = request.getUrl();
    if (url == null) {
      return null;
    }
    return url.endsWith(HOST_METADATA_SUFFIX) || url.endsWith(OIDC_DISCOVERY_SUFFIX) ? url : null;
  }

  private static boolean isCacheable(String url, int statusCode) {
    // A 404 from the host-metadata endpoint means the host does not support it.
    return statusCode == 200 || (statusCode == 404 && url.endsWith(HOST_METADATA_SUFFIX));
  }

  private static final class CachedResponse {
    final URL url;
    final int statusCode;
    final String status;
    final Map<String, List<String>> headers;
    final String body;
    final long fetchedAtMillis;

    private CachedResponse(
        URL url,
        int statusCode,
        String status,
        Map<String, List<String>> headers,
        String body,
        long fetchedAtMillis) {
      this.url = url;
      this.statusCode = statusCode;
      this.status = status;
      this.headers = headers;
      this.body = body;
      this.fetchedAtMillis = fetchedAtMillis;
    }

    static CachedResponse from(Response response, long fetchedAtMillis) throws IOException {
      String body;
      try (InputStream in = response.getBody()) {
        body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      return new CachedResponse(
          response.getUrl(),
          response.getStatusCode(),
          response.getStatus(),
          readOnlyCopy(response.getAllHeaders()),
          body,
          fetchedAtMillis);
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
      return nowMillis - fetchedAtMillis >= TTL_MILLIS;
    }

    Response toResponse(Request request) {
      return new Response(request, url, statusCode, status, headers, body);
    }
  }
}
