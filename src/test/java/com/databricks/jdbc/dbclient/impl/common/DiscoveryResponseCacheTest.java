package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DiscoveryResponseCacheTest {
  private static final String HOST_METADATA =
      "https://host-a.example.com/.well-known/databricks-config";
  private static final String OIDC_DISCOVERY =
      "https://host-a.example.com/oidc/.well-known/oauth-authorization-server";

  private final AtomicInteger calls = new AtomicInteger();
  private int nextStatus = 200;

  private final HttpClient delegate =
      request -> {
        calls.incrementAndGet();
        return new Response(
            request, nextStatus, "status", Collections.emptyMap(), "body-" + calls.get());
      };

  @BeforeEach
  @AfterEach
  void clearCache() {
    DiscoveryResponseCache.clear();
  }

  @Test
  void cachesSuccessfulHostMetadataAndOidcDiscovery() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);

    Response first = client.execute(new Request("GET", HOST_METADATA));
    Response second = client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", OIDC_DISCOVERY));
    Response oidcAgain = client.execute(new Request("GET", OIDC_DISCOVERY));

    assertEquals(2, calls.get());
    assertEquals(200, second.getStatusCode());
    assertEquals("body-1", read(first));
    assertEquals("body-1", read(second));
    assertEquals("body-2", read(oidcAgain));
  }

  @Test
  void expiredEntryIsRefetchedAndReplaced() throws IOException {
    AtomicLong now = new AtomicLong(1_000L);
    HttpClient client = DiscoveryResponseCache.wrap(delegate, now::get);

    client.execute(new Request("GET", HOST_METADATA));
    now.addAndGet(DiscoveryResponseCache.TTL_MILLIS - 1);
    assertEquals("body-1", read(client.execute(new Request("GET", HOST_METADATA))));

    now.addAndGet(1);
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals("body-2", read(client.execute(new Request("GET", HOST_METADATA))));
    assertEquals(2, calls.get());
  }

  @Test
  void cachedResponseCarriesCurrentRequest() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);
    client.execute(new Request("GET", HOST_METADATA));

    Request again = new Request("GET", HOST_METADATA);
    assertSame(again, client.execute(again).getRequest());
  }

  @Test
  void doesNotCacheOtherRequests() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);

    client.execute(new Request("GET", "https://host-a.example.com/api/2.0/sql/statements"));
    client.execute(new Request("GET", "https://host-a.example.com/api/2.0/sql/statements"));
    client.execute(new Request("POST", HOST_METADATA, "{}"));
    client.execute(new Request("POST", HOST_METADATA, "{}"));
    client.execute(new Request("GET", HOST_METADATA).withQueryParam("x", "1"));
    client.execute(new Request("GET", HOST_METADATA).withQueryParam("x", "1"));

    assertEquals(6, calls.get());
  }

  @Test
  void cachesNotFoundOnlyForHostMetadata() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);
    nextStatus = 404;

    client.execute(new Request("GET", HOST_METADATA));
    assertEquals(404, client.execute(new Request("GET", HOST_METADATA)).getStatusCode());
    client.execute(new Request("GET", OIDC_DISCOVERY));
    client.execute(new Request("GET", OIDC_DISCOVERY));

    assertEquals(3, calls.get());
  }

  @Test
  void doesNotCacheServerErrors() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);
    nextStatus = 503;

    client.execute(new Request("GET", HOST_METADATA));
    client.execute(new Request("GET", HOST_METADATA));

    assertEquals(2, calls.get());
  }

  @Test
  void hostsAreCachedSeparately() throws IOException {
    HttpClient client = DiscoveryResponseCache.wrap(delegate);

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
