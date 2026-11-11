package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.google.common.base.Ticker;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

class DiscoveryResponseCacheTest {
  private static final String HOST_METADATA =
      "https://host-a.example.com/.well-known/databricks-config";
  private static final String OIDC_DISCOVERY =
      "https://host-a.example.com/oidc/.well-known/oauth-authorization-server";
  private static final List<Object> TRUST = List.of("default-trust");

  private final AtomicInteger calls = new AtomicInteger();
  private final AtomicLong nowMillis = new AtomicLong(1_000L);
  private final DiscoveryResponseCache cache =
      new DiscoveryResponseCache(
          new Ticker() {
            @Override
            public long read() {
              return TimeUnit.MILLISECONDS.toNanos(nowMillis.get());
            }
          });
  private int nextStatus = 200;
  private URL nextUrl;
  private String nextContentType = "application/json";

  private final HttpClient delegate =
      request -> {
        calls.incrementAndGet();
        return new Response(
            request,
            nextUrl == null ? request.getUri().toURL() : nextUrl,
            nextStatus,
            "status",
            nextContentType == null ? Map.of() : Map.of("Content-Type", List.of(nextContentType)),
            "body-" + calls.get());
      };

  @ParameterizedTest
  @ValueSource(
      strings = {
        "application/json",
        "application/json; charset=utf-8",
        "Application/JSON ; charset=UTF-8"
      })
  void cachesSuccessfulHostMetadataAndOidcDiscovery(String contentType) throws IOException {
    nextContentType = contentType;
    HttpClient client = cache.wrapClient(delegate, TRUST);

    Response first = client.execute(new Request("GET", HOST_METADATA));
    Response second = client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", OIDC_DISCOVERY));
    Response oidcAgain = client.execute(new Request("GET", OIDC_DISCOVERY));

    assertEquals(2, calls.get());
    assertEquals(200, second.getStatusCode());
    assertEquals(contentType, second.getFirstHeader("Content-Type"));
    assertEquals("body-1", read(first));
    assertEquals("body-1", read(second));
    assertEquals("body-2", read(oidcAgain));
  }

  @ParameterizedTest
  @ValueSource(strings = {HOST_METADATA, OIDC_DISCOVERY})
  void redirectedResponsesAreNotCached(String url) throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    nextUrl = new URL("https://host-a.example.com/login.html?error=private-link-validation-error");

    Response redirected = client.execute(new Request("GET", url));
    assertSame(nextUrl, redirected.getUrl());
    assertEquals(200, redirected.getStatusCode());
    assertEquals("body-1", read(redirected));
    assertEquals(0, cache.size());

    nextUrl = null;
    assertEquals("body-2", read(client.execute(new Request("GET", url))));
    assertEquals("body-2", read(client.execute(new Request("GET", url))));
    assertEquals(2, calls.get());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://host-a.example.com",
        "https://host-b.example.com",
        "https://host-a.example.com:8443"
      })
  void doesNotCacheResponsesFromDifferentOrigins(String responseOrigin) throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    nextUrl = new URL(responseOrigin + "/oidc/.well-known/oauth-authorization-server");

    assertEquals("body-1", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-2", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals(2, calls.get());
    assertEquals(0, cache.size());
  }

  @ParameterizedTest
  @CsvSource({
    "https://host-a.example.com, https://HOST-A.example.com:443",
    "https://host-a.example.com:443, https://host-a.example.com",
    "http://host-a.example.com, http://host-a.example.com:80",
    "http://host-a.example.com:80, http://host-a.example.com",
    "HTTPS://HOST-A.example.com, https://host-a.example.com",
    "https://host-a.example.com:8443, https://host-a.example.com:8443"
  })
  void matchingOriginsAreCached(String requestOrigin, String responseOrigin) throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    String url = requestOrigin + "/oidc/.well-known/oauth-authorization-server";
    nextUrl = new URL(responseOrigin + "/oidc/.well-known/oauth-authorization-server");

    assertEquals("body-1", read(client.execute(new Request("GET", url))));
    assertEquals("body-1", read(client.execute(new Request("GET", url))));
    assertEquals(1, calls.get());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"text/html", "application/jsonp"})
  void nonJsonSuccessIsNotCached(String contentType) throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    nextContentType = contentType;

    Response blocked = client.execute(new Request("GET", OIDC_DISCOVERY));
    assertEquals(contentType, blocked.getFirstHeader("Content-Type"));
    assertEquals("body-1", read(blocked));
    assertEquals(0, cache.size());

    nextContentType = "application/json";
    assertEquals("body-2", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-2", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals(2, calls.get());
  }

  @Test
  void clientsWithDifferentTrustSettingsDoNotShareEntries() throws IOException {
    HttpClient strict = cache.wrapClient(delegate, TRUST);
    HttpClient lax = cache.wrapClient(delegate, List.of("allow-self-signed"));

    lax.execute(new Request("GET", OIDC_DISCOVERY));
    assertEquals("body-2", read(strict.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-2", read(strict.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-1", read(lax.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals(2, calls.get());
  }

  @Test
  void cachedHeadersAreReadOnly() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    client.execute(new Request("GET", HOST_METADATA));
    Response hit = client.execute(new Request("GET", HOST_METADATA));

    assertThrows(
        UnsupportedOperationException.class, () -> hit.getAllHeaders().put("X", List.of("y")));
    assertThrows(
        UnsupportedOperationException.class, () -> hit.getHeaders("Content-Type").add("text/html"));
  }

  @Test
  void expiredEntryIsRefetchedAndReplaced() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);

    client.execute(new Request("GET", HOST_METADATA));
    nowMillis.addAndGet(DiscoveryResponseCache.TTL_MILLIS - 1);
    assertEquals("body-1", read(client.execute(new Request("GET", HOST_METADATA))));

    nowMillis.addAndGet(1);
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals(2, calls.get());
  }

  @Test
  void cacheSizeIsBounded() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    int requests = 2 * DiscoveryResponseCache.MAX_ENTRIES;
    for (int i = 0; i < requests; i++) {
      client.execute(
          new Request("GET", "https://host-" + i + ".example.com/.well-known/databricks-config"));
    }

    assertEquals(requests, calls.get());
    assertTrue(cache.size() <= DiscoveryResponseCache.MAX_ENTRIES);
  }

  @Test
  void cachedResponseCarriesCurrentRequest() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    client.execute(new Request("GET", HOST_METADATA));

    Request again = new Request("GET", HOST_METADATA);
    assertSame(again, client.execute(again).getRequest());
  }

  @Test
  void doesNotCacheOtherRequests() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);

    client.execute(new Request("GET", "https://host-a.example.com/api/2.0/sql/statements"));
    client.execute(new Request("GET", "https://host-a.example.com/api/2.0/sql/statements"));
    client.execute(new Request("POST", HOST_METADATA, "{}"));
    client.execute(new Request("POST", HOST_METADATA, "{}"));
    client.execute(new Request("GET", HOST_METADATA).withQueryParam("x", "1"));
    client.execute(new Request("GET", HOST_METADATA).withQueryParam("x", "1"));

    assertEquals(6, calls.get());
  }

  @Test
  void doesNotCacheRequestsWithCredentials() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);

    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("Authorization", "Bearer a"));
    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("authorization", "Bearer b"));
    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("Cookie", "session=c"));

    assertEquals(3, calls.get());
    assertEquals(0, cache.size());
  }

  @Test
  void doesNotCacheNotFound() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    nextStatus = 404;

    client.execute(new Request("GET", HOST_METADATA));
    assertEquals(404, client.execute(new Request("GET", HOST_METADATA)).getStatusCode());
    client.execute(new Request("GET", OIDC_DISCOVERY));
    client.execute(new Request("GET", OIDC_DISCOVERY));

    assertEquals(4, calls.get());
    assertEquals(0, cache.size());
  }

  @Test
  void doesNotCacheServerErrors() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);
    nextStatus = 503;

    client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", HOST_METADATA));

    assertEquals(2, calls.get());
  }

  @Test
  void hostsAreCachedSeparately() throws IOException {
    HttpClient client = cache.wrapClient(delegate, TRUST);

    client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", "https://host-b.example.com/.well-known/databricks-config"));

    assertEquals(2, calls.get());
  }

  private static String read(Response response) throws IOException {
    try (InputStream in = response.getBody()) {
      return new String(in.readAllBytes(), StandardCharsets.UTF_8);
    }
  }
}
