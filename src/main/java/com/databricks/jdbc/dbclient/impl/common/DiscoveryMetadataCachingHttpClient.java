package com.databricks.jdbc.dbclient.impl.common;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/** Shares public SDK discovery responses across connections to the same host. */
final class DiscoveryMetadataCachingHttpClient implements HttpClient {
  private static final int MAX_BODY_LENGTH = 64 * 1024;
  private static final Cache<String, CachedResponse> CACHE =
      CacheBuilder.newBuilder().maximumSize(256).expireAfterWrite(5, TimeUnit.MINUTES).build();

  private final HttpClient delegate;

  DiscoveryMetadataCachingHttpClient(HttpClient delegate) {
    this.delegate = delegate;
  }

  @Override
  public Response execute(Request request) throws IOException {
    if (!isDiscoveryRequest(request)) {
      return delegate.execute(request);
    }

    try {
      return CACHE.get(request.getUri().toString(), () -> load(request)).toResponse(request);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof UncacheableResponse) {
        return ((UncacheableResponse) cause).response.toResponse(request);
      }
      if (cause instanceof IOException) {
        throw (IOException) cause;
      }
      if (cause instanceof RuntimeException) {
        throw (RuntimeException) cause;
      }
      throw new IOException(cause);
    }
  }

  private CachedResponse load(Request request) throws IOException, UncacheableResponse {
    CachedResponse response = new CachedResponse(delegate.execute(request));
    if (response.statusCode != 200
        || response.body == null
        || response.body.length() > MAX_BODY_LENGTH
        || response.headers.keySet().stream()
            .anyMatch(
                header ->
                    header.equalsIgnoreCase("Cache-Control")
                        || header.equalsIgnoreCase("Pragma")
                        || header.equalsIgnoreCase("Expires"))) {
      throw new UncacheableResponse(response);
    }
    return response;
  }

  private static boolean isDiscoveryRequest(Request request) {
    if (!Request.GET.equals(request.getMethod())
        || request.getBodyString() != null
        || request.getBodyStream() != null
        || request.getHeaders().keySet().stream()
            .anyMatch(header -> !header.equalsIgnoreCase("User-Agent"))) {
      return false;
    }
    URI uri = request.getUri();
    if (!"https".equalsIgnoreCase(uri.getScheme())
        || uri.getHost() == null
        || uri.getUserInfo() != null
        || uri.getRawQuery() != null
        || uri.getFragment() != null) {
      return false;
    }
    String path = uri.getPath();
    return "/.well-known/databricks-config".equals(path)
        || path.endsWith("/.well-known/oauth-authorization-server");
  }

  private static final class CachedResponse {
    private final URL url;
    private final int statusCode;
    private final String status;
    private final Map<String, List<String>> headers;
    private final String body;

    private CachedResponse(Response response) throws IOException {
      this.url = response.getUrl();
      this.statusCode = response.getStatusCode();
      this.status = response.getStatus();
      this.headers =
          response.getAllHeaders() == null
              ? Collections.emptyMap()
              : response.getAllHeaders().entrySet().stream()
                  .collect(
                      Collectors.toMap(Map.Entry::getKey, entry -> List.copyOf(entry.getValue())));
      String debugBody = response.getDebugBody();
      if (debugBody != null && !"\"<InputStream>\"".equals(debugBody)) {
        this.body = debugBody;
      } else if (response.getBody() == null) {
        this.body = null;
      } else {
        try (InputStream stream = response.getBody()) {
          this.body = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
      }
    }

    private Response toResponse(Request request) {
      return new Response(request, url, statusCode, status, headers, body);
    }
  }

  private static final class UncacheableResponse extends Exception {
    private final CachedResponse response;

    private UncacheableResponse(CachedResponse response) {
      this.response = response;
    }
  }
}
