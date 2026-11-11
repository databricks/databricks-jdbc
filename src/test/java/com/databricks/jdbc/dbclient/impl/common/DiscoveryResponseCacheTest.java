package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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

  @BeforeEach
  @AfterEach
  void clearCache() {
    DiscoveryResponseCache.clear();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "application/json",
        "application/json; charset=utf-8",
        "Application/JSON ; charset=UTF-8"
      })
  void cachesSuccessfulHostMetadataAndOidcDiscovery(String contentType) throws IOException {
    nextContentType = contentType;
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);

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
  @CsvSource({HOST_METADATA + ",200", HOST_METADATA + ",404", OIDC_DISCOVERY + ",200"})
  void redirectedResponsesAreNotCached(String url, int statusCode) throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    nextStatus = statusCode;
    nextUrl = new URL("https://host-a.example.com/login.html?error=private-link-validation-error");

    Response redirected = client.execute(new Request("GET", url));
    assertSame(nextUrl, redirected.getUrl());
    assertEquals(statusCode, redirected.getStatusCode());
    assertEquals("body-1", read(redirected));
    assertEquals(0, DiscoveryResponseCache.size());

    nextUrl = null;
    nextStatus = 200;
    assertEquals("body-2", read(client.execute(new Request("GET", url))));
    assertEquals("body-2", read(client.execute(new Request("GET", url))));
    assertEquals(2, calls.get());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(strings = {"text/html", "application/jsonp"})
  void nonJsonSuccessIsNotCached(String contentType) throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    nextContentType = contentType;

    Response blocked = client.execute(new Request("GET", OIDC_DISCOVERY));
    assertEquals(contentType, blocked.getFirstHeader("Content-Type"));
    assertEquals("body-1", read(blocked));
    assertEquals(0, DiscoveryResponseCache.size());

    nextContentType = "application/json";
    assertEquals("body-2", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-2", read(client.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals(2, calls.get());
  }

  @Test
  void clientsWithDifferentTrustSettingsDoNotShareEntries() throws IOException {
    HttpClient strict = DiscoveryResponseCache.wrap(delegate, TRUST);
    HttpClient lax = DiscoveryResponseCache.wrap(delegate, List.of("allow-self-signed"));

    lax.execute(new Request("GET", OIDC_DISCOVERY));
    assertEquals("body-2", read(strict.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-2", read(strict.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals("body-1", read(lax.execute(new Request("GET", OIDC_DISCOVERY))));
    assertEquals(2, calls.get());
  }

  @Test
  void cachedHeadersAreReadOnly() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    client.execute(new Request("GET", HOST_METADATA));
    Response hit = client.execute(new Request("GET", HOST_METADATA));

    assertThrows(
        UnsupportedOperationException.class, () -> hit.getAllHeaders().put("X", List.of("y")));
    assertThrows(
        UnsupportedOperationException.class, () -> hit.getHeaders("Content-Type").add("text/html"));
  }

  @Test
  void expiredEntryIsRefetchedAndReplaced() throws IOException {
    AtomicLong now = new AtomicLong(1_000L);
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST, now::get);

    client.execute(new Request("GET", HOST_METADATA));
    now.addAndGet(DiscoveryResponseCache.TTL_MILLIS - 1);
    assertEquals("body-1", read(client.execute(new Request("GET", HOST_METADATA))));

    now.addAndGet(1);
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals(2, calls.get());
  }

  @Test
  void storingAnEntryDropsExpiredOnes() throws IOException {
    AtomicLong now = new AtomicLong(1_000L);
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST, now::get);
    client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", OIDC_DISCOVERY));
    assertEquals(2, DiscoveryResponseCache.size());

    now.addAndGet(DiscoveryResponseCache.TTL_MILLIS);
    client.execute(new Request("GET", "https://host-b.example.com/.well-known/databricks-config"));

    assertEquals(1, DiscoveryResponseCache.size());
  }

  @Test
  void cachedResponseCarriesCurrentRequest() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    client.execute(new Request("GET", HOST_METADATA));

    Request again = new Request("GET", HOST_METADATA);
    assertSame(again, client.execute(again).getRequest());
  }

  @Test
  void doesNotCacheOtherRequests() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);

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
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);

    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("Authorization", "Bearer a"));
    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("authorization", "Bearer b"));
    client.execute(new Request("GET", OIDC_DISCOVERY).withHeader("Cookie", "session=c"));

    assertEquals(3, calls.get());
    assertEquals(0, DiscoveryResponseCache.size());
  }

  @Test
  void cachesNotFoundOnlyForHostMetadata() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    nextStatus = 404;

    client.execute(new Request("GET", HOST_METADATA));
    assertEquals(404, client.execute(new Request("GET", HOST_METADATA)).getStatusCode());
    client.execute(new Request("GET", OIDC_DISCOVERY));
    client.execute(new Request("GET", OIDC_DISCOVERY));

    assertEquals(3, calls.get());
  }

  @Test
  void notFoundExpiresSoonerThanSuccess() throws IOException {
    AtomicLong now = new AtomicLong(1_000L);
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST, now::get);
    nextStatus = 404;
    client.execute(new Request("GET", HOST_METADATA));

    now.addAndGet(DiscoveryResponseCache.NOT_FOUND_TTL_MILLIS - 1);
    client.execute(new Request("GET", HOST_METADATA));
    assertEquals(1, calls.get());

    now.addAndGet(1);
    nextStatus = 200;
    assertEquals(200, client.execute(new Request("GET", HOST_METADATA)).getStatusCode());
    assertEquals(2, calls.get());
  }

  @Test
  void doesNotCacheServerErrors() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);
    nextStatus = 503;

    client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", HOST_METADATA));

    assertEquals(2, calls.get());
  }

  @Test
  void hostsAreCachedSeparately() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate, TRUST);

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
