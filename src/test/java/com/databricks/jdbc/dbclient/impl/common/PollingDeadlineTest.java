package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksHttpTTransport;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.commons.CommonsHttpClient;
import com.databricks.sdk.core.http.Request;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class PollingDeadlineTest {
  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void blockedSocketIsReleasedAndPoolCanBeReused(boolean sdk) throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger requests = new AtomicInteger();
    ExecutorService serverThreads = Executors.newCachedThreadPool();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(serverThreads);
    server.createContext(
        "/",
        exchange -> {
          if (requests.incrementAndGet() == 1) {
            try {
              release.await(12, TimeUnit.SECONDS);
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
            }
          }
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          try {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, body.length);
            exchange.getResponseBody().write(body);
          } finally {
            exchange.close();
          }
        });
    server.start();
    String url = "http://127.0.0.1:" + server.getAddress().getPort() + "/";
    PollingConnectionManager pool = new PollingConnectionManager();
    pool.setMaxTotal(1);
    pool.setDefaultMaxPerRoute(1);
    try (CloseableHttpClient apache =
        HttpClients.custom()
            .setConnectionManager(pool)
            .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(30000).build())
            .build()) {
      TimeoutHandler handler = handler(2, null);
      IDatabricksHttpClient transportClient = mock(IDatabricksHttpClient.class);
      lenient()
          .when(transportClient.execute(any()))
          .thenAnswer(call -> apache.execute(call.getArgument(0)));
      DatabricksConfig config = mock(DatabricksConfig.class);
      lenient().when(config.authenticate()).thenReturn(Collections.emptyMap());
      DatabricksHttpTTransport transport =
          new DatabricksHttpTTransport(
              transportClient, url, config, mock(IDatabricksConnectionContext.class));
      CommonsHttpClient sdkClient =
          new CommonsHttpClient.Builder()
              .withConnectionManager(pool)
              .withTimeoutSeconds(30)
              .build();
      TimeoutHandler.Poll<Void, Exception> poll =
          () -> {
            if (sdk) {
              sdkClient.execute(new Request(Request.GET, url));
            } else {
              transport.write(new byte[] {1}, 0, 1);
              transport.flush();
            }
            return null;
          };
      long started = System.nanoTime();
      DatabricksTimeoutException failure =
          assertThrows(DatabricksTimeoutException.class, () -> handler.executeWithTimeout(poll));
      assertTrue((System.nanoTime() - started) / 1e9 < 3.5);
      assertNotNull(failure.getCause());
      assertEquals(0, pool.getTotalStats().getLeased());
      assertFalse(Thread.currentThread().isInterrupted());
      handler(0, null).executeWithTimeout(poll);
      assertEquals(2, requests.get());
      assertEquals(0, pool.getTotalStats().getLeased());
    } finally {
      release.countDown();
      server.stop(0);
      serverThreads.shutdownNow();
      pool.close();
    }
  }

  @Test
  void waitingForPoolLeaseHonorsDeadline() throws Exception {
    try (PollingConnectionManager pool = new PollingConnectionManager()) {
      pool.setMaxTotal(1);
      pool.setDefaultMaxPerRoute(1);
      HttpRoute route = new HttpRoute(new HttpHost("127.0.0.1", 80));
      HttpClientConnection occupied = pool.requestConnection(route, null).get(1, TimeUnit.SECONDS);
      try {
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                handler(1, null)
                    .executeWithTimeout(
                        () -> pool.requestConnection(route, null).get(30, TimeUnit.SECONDS)));
        assertEquals(1, pool.getTotalStats().getLeased());
        assertEquals(0, pool.getTotalStats().getPending());
        assertFalse(Thread.currentThread().isInterrupted());
      } finally {
        pool.releaseConnection(occupied, null, 0, TimeUnit.MILLISECONDS);
      }
      HttpClientConnection reused = pool.requestConnection(route, null).get(1, TimeUnit.SECONDS);
      pool.releaseConnection(reused, null, 0, TimeUnit.MILLISECONDS);
      assertEquals(0, pool.getTotalStats().getLeased());
    }
  }

  @Test
  void slowCancellationDoesNotDelayTimeout() {
    CountDownLatch release = new CountDownLatch(1);
    Runnable cancel =
        () -> {
          try {
            release.await(12, TimeUnit.SECONDS);
          } catch (InterruptedException failure) {
            Thread.currentThread().interrupt();
          }
        };
    long started = System.nanoTime();
    try {
      assertThrows(DatabricksTimeoutException.class, () -> handler(1, cancel).sleep(12000));
      assertTrue((System.nanoTime() - started) / 1e9 < 2.5);
    } finally {
      release.countDown();
    }
  }

  @Test
  void completedPollDoesNotInterruptLaterWork() throws Exception {
    TimeoutHandler handler = handler(1, null);
    assertEquals("ok", handler.executeWithTimeout(() -> "ok"));
    Thread.sleep(1200);
    assertFalse(Thread.currentThread().isInterrupted());
  }

  @Test
  void pollFailureBeforeDeadlineIsUnchanged() {
    IllegalStateException original = new IllegalStateException("poll failed");
    Exception failure =
        assertThrows(
            IllegalStateException.class,
            () ->
                handler(5, null)
                    .executeWithTimeout(
                        () -> {
                          throw original;
                        }));
    assertSame(original, failure);
  }

  private TimeoutHandler handler(int seconds, Runnable cancel) {
    return new TimeoutHandler(
        seconds, "test", cancel, DatabricksDriverErrorCode.STATEMENT_EXECUTION_TIMEOUT);
  }
}
