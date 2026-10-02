package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class DiscoveryMetadataCachingHttpClientTest {
  @Test
  void sharesConcurrentDiscoveryAcrossClients() throws Exception {
    String hostPrefix = UUID.randomUUID().toString();
    String url = "https://" + hostPrefix + "Aa.example.com/.well-known/databricks-config";
    String otherUrl = "https://" + hostPrefix + "BB.example.com/.well-known/databricks-config";
    assertEquals(url.hashCode(), otherUrl.hashCode());
    AtomicInteger requests = new AtomicInteger();
    CountDownLatch started = new CountDownLatch(1);
    CountDownLatch release = new CountDownLatch(1);
    HttpClient delegate =
        request -> {
          requests.incrementAndGet();
          if (request.getUrl().equals(otherUrl)) {
            return new Response(request, 200, "OK", Map.of(), "{\"workspace_id\":\"other\"}");
          }
          started.countDown();
          try {
            if (!release.await(5, TimeUnit.SECONDS)) {
              throw new AssertionError("Timed out waiting to release discovery request");
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
          }
          return new Response(request, 200, "OK", Map.of(), "{\"workspace_id\":\"123\"}");
        };
    HttpClient firstClient = new DiscoveryMetadataCachingHttpClient(delegate);
    HttpClient secondClient = new DiscoveryMetadataCachingHttpClient(delegate);
    ExecutorService executor = Executors.newFixedThreadPool(3);
    try {
      Future<Response> first = executor.submit(() -> firstClient.execute(new Request("GET", url)));
      assertTrue(started.await(5, TimeUnit.SECONDS));
      Future<Response> second =
          executor.submit(() -> secondClient.execute(new Request("GET", url)));
      assertThrows(TimeoutException.class, () -> second.get(100, TimeUnit.MILLISECONDS));
      Future<Response> other =
          executor.submit(() -> secondClient.execute(new Request("GET", otherUrl)));
      assertEquals("{\"workspace_id\":\"other\"}", body(other.get(5, TimeUnit.SECONDS)));

      release.countDown();
      assertEquals("{\"workspace_id\":\"123\"}", body(first.get(5, TimeUnit.SECONDS)));
      assertEquals("{\"workspace_id\":\"123\"}", body(second.get(5, TimeUnit.SECONDS)));
      assertEquals(2, requests.get());
    } finally {
      release.countDown();
      executor.shutdownNow();
      executor.awaitTermination(5, TimeUnit.SECONDS);
    }
  }

  @Test
  void keysDiscoveryByFullUrlAndDoesNotCacheAuthenticatedRequests() throws Exception {
    String host = UUID.randomUUID() + ".example.com";
    AtomicInteger requests = new AtomicInteger();
    HttpClient client =
        new DiscoveryMetadataCachingHttpClient(
            request ->
                new Response(
                    request, 200, "OK", Map.of(), "{\"call\":" + requests.incrementAndGet() + "}"));
    String configUrl = "https://" + host + "/.well-known/databricks-config";
    String oidcUrl = "https://" + host + "/oidc/.well-known/oauth-authorization-server";

    assertEquals("{\"call\":1}", body(client.execute(new Request("GET", configUrl))));
    assertEquals("{\"call\":1}", body(client.execute(new Request("GET", configUrl))));
    assertEquals("{\"call\":2}", body(client.execute(new Request("GET", oidcUrl))));
    assertEquals(
        "{\"call\":3}",
        body(
            client.execute(
                new Request("GET", configUrl).withHeader("Authorization", "Bearer token"))));
    assertEquals(3, requests.get());
  }

  @Test
  void doesNotCacheErrorsOrResponsesWithCacheDirectives() throws Exception {
    String url = "https://" + UUID.randomUUID() + ".example.com/.well-known/databricks-config";
    AtomicInteger requests = new AtomicInteger();
    HttpClient client =
        new DiscoveryMetadataCachingHttpClient(
            request -> {
              int call = requests.incrementAndGet();
              return new Response(
                  request,
                  call == 1 ? 503 : 200,
                  call == 1 ? "Unavailable" : "OK",
                  Map.of("Cache-Control", List.of("no-store")),
                  "{\"call\":" + call + "}");
            });

    assertEquals(503, client.execute(new Request("GET", url)).getStatusCode());
    assertEquals("{\"call\":2}", body(client.execute(new Request("GET", url))));
    assertEquals("{\"call\":3}", body(client.execute(new Request("GET", url))));
    assertEquals(3, requests.get());
  }

  private static String body(Response response) throws Exception {
    return new String(response.getBody().readAllBytes(), StandardCharsets.UTF_8);
  }
}
