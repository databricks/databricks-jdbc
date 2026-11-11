package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Caches the SDK's unauthenticated discovery GETs (host metadata and OIDC discovery) per URL.
 *
 * <p>Each connection builds its own {@code DatabricksConfig}, whose {@code resolve()} and default
 * credentials chain fetch these documents again on every connect. They do not change for a
 * workspace, so refetching only adds round trips and a TLS handshake to each connection.
 */
final class DiscoveryResponseCache {
  private static final String HOST_METADATA_SUFFIX = "/.well-known/databricks-config";
  private static final String OIDC_DISCOVERY_SUFFIX = "/.well-known/oauth-authorization-server";
  @VisibleForTesting static final long TTL_MILLIS = TimeUnit.HOURS.toMillis(1);

  private static final Map<String, CachedResponse> CACHE = new ConcurrentHashMap<>();

  private DiscoveryResponseCache() {}

  static HttpClient wrap(HttpClient delegate) {
    return request -> {
      String key = cacheKey(request);
      if (key == null) {
        return delegate.execute(request);
      }
      CachedResponse cached = CACHE.get(key);
      if (cached != null && System.currentTimeMillis() - cached.fetchedAtMillis < TTL_MILLIS) {
        return cached.toResponse(request);
      }
      Response response = delegate.execute(request);
      if (!isCacheable(key, response.getStatusCode())) {
        return response;
      }
      CachedResponse fresh = CachedResponse.from(response);
      CACHE.put(key, fresh);
      return fresh.toResponse(request);
    };
  }

  @VisibleForTesting
  static void clear() {
    CACHE.clear();
  }

  private static String cacheKey(Request request) {
    if (!"GET".equalsIgnoreCase(request.getMethod()) || !request.getQuery().isEmpty()) {
      return null;
    }
    String url = request.getUrl();
    if (url == null) {
      return null;
    }
    return url.endsWith(HOST_METADATA_SUFFIX) || url.endsWith(OIDC_DISCOVERY_SUFFIX) ? url : null;
  }

  private static boolean isCacheable(String key, int statusCode) {
    // A 404 from the host-metadata endpoint means the host does not support it.
    return statusCode == 200 || (statusCode == 404 && key.endsWith(HOST_METADATA_SUFFIX));
  }

  private static final class CachedResponse {
    final URL url;
    final int statusCode;
    final String status;
    final Map<String, List<String>> headers;
    final String body;
    final long fetchedAtMillis;

    private CachedResponse(
        URL url, int statusCode, String status, Map<String, List<String>> headers, String body) {
      this.url = url;
      this.statusCode = statusCode;
      this.status = status;
      this.headers = headers;
      this.body = body;
      this.fetchedAtMillis = System.currentTimeMillis();
    }

    static CachedResponse from(Response response) throws IOException {
      String body;
      try (InputStream in = response.getBody()) {
        body = in == null ? "" : new String(in.readAllBytes(), StandardCharsets.UTF_8);
      }
      Map<String, List<String>> headers =
          response.getAllHeaders() == null
              ? new HashMap<>()
              : new HashMap<>(response.getAllHeaders());
      return new CachedResponse(
          response.getUrl(), response.getStatusCode(), response.getStatus(), headers, body);
    }

    Response toResponse(Request request) {
      return new Response(request, url, statusCode, status, headers, body);
    }
  }
}
