package com.databricks.jdbc.api.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.HttpClientType;
import com.databricks.jdbc.common.TelemetryLogLevel;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.common.safe.DatabricksDriverFeatureFlagsContextFactory;
import com.databricks.jdbc.dbclient.IDatabricksClient;
import com.databricks.jdbc.dbclient.IDatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClientFactory;
import com.databricks.jdbc.model.telemetry.TelemetryFrontendLog;
import com.databricks.jdbc.telemetry.ITelemetryClient;
import com.databricks.jdbc.telemetry.NoopTelemetryClient;
import com.databricks.jdbc.telemetry.TelemetryClientFactory;
import com.databricks.jdbc.telemetry.latency.TelemetryCollectorManager;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

/**
 * Close-path tests against the real client factories, with telemetry forced on and served by a
 * local HTTP server that records every telemetry push.
 */
public class DatabricksConnectionCloseTest {
  private static final String HOST = "127.0.0.1";

  private final Queue<String> telemetryBodies = new ConcurrentLinkedQueue<>();
  private int sharedClientIterations;
  private HttpServer server;
  private String url;
  private IDatabricksClient client;

  @BeforeEach
  void setUp() throws Exception {
    TelemetryClientFactory.getInstance().reset();
    server = HttpServer.create(new InetSocketAddress(HOST, 0), 0);
    server.createContext("/", this::handle);
    server.setExecutor(Executors.newCachedThreadPool());
    server.start();
    url =
        String.format(
            "jdbc:databricks://%s:%d/default;transportMode=http;ssl=0;AuthMech=3;UID=token;"
                + "PWD=test-token;httpPath=/sql/1.0/warehouses/99999999;"
                + "ForceEnableTelemetry=1;TelemetrySocketTimeout=5;logLevel=FATAL",
            HOST, server.getAddress().getPort());
    client = mock(IDatabricksClient.class);
    when(client.createSession(any(Warehouse.class), any(), any(), any()))
        .thenReturn(
            ImmutableSessionInfo.builder()
                .computeResource(new Warehouse("99999999"))
                .sessionId("session")
                .build());
  }

  @AfterEach
  void tearDown() {
    TelemetryClientFactory.getInstance().reset();
    server.stop(0);
  }

  @Test
  void finalTelemetryPushRunsBeforeItsHttpClientCloses() throws Exception {
    IDatabricksConnectionContext ctx = DatabricksConnectionContext.parse(url, new Properties());
    DatabricksConnection connection = openWithTelemetry(ctx, "only");
    DatabricksHttpClient telemetryHttp =
        spy(
            (DatabricksHttpClient)
                DatabricksHttpClientFactory.getInstance().getClient(ctx, HttpClientType.TELEMETRY));
    httpClients()
        .put(new SimpleEntry<>(ctx.getConnectionUuid(), HttpClientType.TELEMETRY), telemetryHttp);

    connection.close();
    awaitNoBackgroundCloseThreads();

    InOrder order = inOrder(telemetryHttp);
    order.verify(telemetryHttp).execute(any(HttpUriRequest.class));
    order.verify(telemetryHttp).close();
    assertTrue(received("only"), "The final telemetry batch was not delivered");
  }

  @Test
  void lastCloseRacingReopenToSameHostLeavesNoStaleOrLeakedState() throws Exception {
    ExecutorService pool = Executors.newFixedThreadPool(2);
    try {
      for (int iteration = 0; iteration < 50; iteration++) {
        int i = iteration;
        String it = "iteration " + i;
        IDatabricksConnectionContext closingCtx =
            DatabricksConnectionContext.parse(url, new Properties());
        IDatabricksConnectionContext openingCtx =
            DatabricksConnectionContext.parse(url, new Properties());
        DatabricksConnection closing = openWithTelemetry(closingCtx, "closing-" + i);
        ITelemetryClient closingTelemetry =
            TelemetryClientFactory.getInstance().getTelemetryClient(closingCtx);
        CyclicBarrier start = new CyclicBarrier(2);
        // Odd iterations let the new connection join the closing one's telemetry client first.
        boolean joinFirst = i % 2 == 1;
        CountDownLatch joined = new CountDownLatch(1);

        Future<?> close =
            pool.submit(
                () -> {
                  start.await();
                  if (joinFirst) {
                    assertTrue(joined.await(30, TimeUnit.SECONDS));
                  }
                  closing.close();
                  return null;
                });
        Future<DatabricksConnection> open =
            pool.submit(
                () -> {
                  start.await();
                  return openWithTelemetry(openingCtx, "opening-" + i, joined::countDown);
                });
        close.get(30, TimeUnit.SECONDS);
        DatabricksConnection opened = open.get(30, TimeUnit.SECONDS);
        awaitNoBackgroundCloseThreads();

        // The new connection keeps its own live telemetry client, flags and HTTP clients.
        ITelemetryClient openedTelemetry =
            TelemetryClientFactory.getInstance().getTelemetryClient(openingCtx);
        assertFalse(openedTelemetry instanceof NoopTelemetryClient, it);
        // Either a fresh client, or the shared one, now bound to the connection still open.
        assertEquals(openingCtx.getConnectionUuid(), boundUuid(openedTelemetry), it);
        if (openedTelemetry == closingTelemetry) {
          sharedClientIterations++;
        }
        assertTrue(registeredTelemetryConnections().contains(openingCtx.getConnectionUuid()), it);
        assertTrue(featureFlagConnections().contains(openingCtx.getConnectionUuid()), it);
        assertFalse(featureFlagSchedulerShutDown(), it);
        assertNotNull(
            DatabricksClientConfiguratorManager.getInstance()
                .getConfiguratorOnlyIfExists(openingCtx),
            it);
        for (HttpClientType type : HttpClientType.values()) {
          IDatabricksHttpClient http =
              httpClients().get(new SimpleEntry<>(openingCtx.getConnectionUuid(), type));
          if (http != null) {
            assertFalse(
                isShutDown(assertInstanceOf(DatabricksHttpClient.class, http, it)),
                it + " " + type);
          }
        }

        opened.close();
        awaitNoBackgroundCloseThreads();
        assertNoStateLeft(closingCtx, it);
        assertNoStateLeft(openingCtx, it);
        assertTrue(received("closing-" + i), it + ": closing connection's telemetry lost");
        assertTrue(received("opening-" + i), it + ": new connection's telemetry lost");
      }
      assertTrue(sharedClientIterations > 0, "No iteration shared the closing telemetry client");
    } finally {
      pool.shutdownNow();
    }
  }

  private static String boundUuid(ITelemetryClient telemetryClient) throws Exception {
    Object context = field(telemetryClient.getClass(), "context").get(telemetryClient);
    return ((IDatabricksConnectionContext) context).getConnectionUuid();
  }

  private DatabricksConnection openWithTelemetry(IDatabricksConnectionContext ctx, String eventId)
      throws Exception {
    return openWithTelemetry(ctx, eventId, () -> {});
  }

  private DatabricksConnection openWithTelemetry(
      IDatabricksConnectionContext ctx, String eventId, Runnable afterJoiningTelemetry)
      throws Exception {
    DatabricksConnection connection = new DatabricksConnection(ctx, client);
    connection.open();
    DatabricksClientConfiguratorManager.getInstance().getConfigurator(ctx);
    ITelemetryClient telemetry = TelemetryClientFactory.getInstance().getTelemetryClient(ctx);
    afterJoiningTelemetry.run();
    telemetry.exportEvent(
        new TelemetryFrontendLog(TelemetryLogLevel.INFO).setFrontendLogEventId(eventId));
    return connection;
  }

  private void handle(HttpExchange exchange) throws IOException {
    String path = exchange.getRequestURI().getPath();
    String body = new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
    String response;
    int status = 200;
    if (path.startsWith("/telemetry")) {
      telemetryBodies.add(body);
      response = "{\"errors\":[],\"numSuccess\":0,\"numProtoSuccess\":0}";
    } else if (path.startsWith("/oidc/.well-known")) {
      String base = "http://" + HOST + ":" + server.getAddress().getPort();
      response =
          String.format(
              "{\"authorization_endpoint\":\"%s/oidc/v1/authorize\","
                  + "\"token_endpoint\":\"%s/oidc/v1/token\"}",
              base, base);
    } else {
      status = 404;
      response = "{}";
    }
    byte[] bytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(bytes);
    }
  }

  private boolean received(String eventId) {
    String marker = "\\\"frontend_log_event_id\\\":\\\"" + eventId + "\\\"";
    return telemetryBodies.stream().anyMatch(b -> b.contains(marker));
  }

  private static void assertNoStateLeft(IDatabricksConnectionContext ctx, String message)
      throws Exception {
    String uuid = ctx.getConnectionUuid();
    assertTrue(registeredTelemetryConnections().isEmpty(), message);
    assertFalse(featureFlagContexts().containsKey(HOST), message);
    assertNull(
        DatabricksClientConfiguratorManager.getInstance().getConfiguratorOnlyIfExists(ctx),
        message);
    for (HttpClientType type : HttpClientType.values()) {
      IDatabricksHttpClient http = httpClients().get(new SimpleEntry<>(uuid, type));
      assertFalse(http instanceof DatabricksHttpClient, message + " live " + type + " client");
    }
    assertFalse(collectors().containsKey(uuid), message + " leaked collector");
  }

  private static void awaitNoBackgroundCloseThreads() throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
    while (DatabricksConnection.backgroundCloseThreadCount() > 0 && System.nanoTime() < deadline) {
      Thread.sleep(5);
    }
    assertEquals(0, DatabricksConnection.backgroundCloseThreadCount());
  }

  @SuppressWarnings("unchecked")
  private static Map<Object, IDatabricksHttpClient> httpClients() throws Exception {
    return (Map<Object, IDatabricksHttpClient>)
        field(DatabricksHttpClientFactory.class, "instances")
            .get(DatabricksHttpClientFactory.getInstance());
  }

  private static boolean isShutDown(DatabricksHttpClient http) throws Exception {
    Object manager = field(DatabricksHttpClient.class, "connectionManager").get(http);
    return ((AtomicBoolean)
            field(PoolingHttpClientConnectionManager.class, "isShutDown").get(manager))
        .get();
  }

  @SuppressWarnings("unchecked")
  private static Set<String> registeredTelemetryConnections() throws Exception {
    Set<String> uuids = new HashSet<>();
    for (String name : new String[] {"telemetryClientHolders", "noauthTelemetryClientHolders"}) {
      Map<String, Object> holders =
          (Map<String, Object>)
              field(TelemetryClientFactory.class, name).get(TelemetryClientFactory.getInstance());
      for (Object holder : holders.values()) {
        uuids.addAll(
            ((Map<String, Object>) field(holder.getClass(), "connections").get(holder)).keySet());
      }
    }
    return uuids;
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> featureFlagContexts() throws Exception {
    return (Map<String, Object>)
        field(DatabricksDriverFeatureFlagsContextFactory.class, "contextMap").get(null);
  }

  @SuppressWarnings("unchecked")
  private static Set<String> featureFlagConnections() throws Exception {
    Object holder = featureFlagContexts().get(HOST);
    if (holder == null) {
      return Collections.emptySet();
    }
    return ((Map<String, Object>) field(holder.getClass(), "activeContexts").get(holder)).keySet();
  }

  private static boolean featureFlagSchedulerShutDown() throws Exception {
    Object holder = featureFlagContexts().get(HOST);
    Object context = field(holder.getClass(), "context").get(holder);
    return ((ExecutorService) field(context.getClass(), "scheduler").get(context)).isShutdown();
  }

  @SuppressWarnings("unchecked")
  private static Map<String, Object> collectors() throws Exception {
    return (Map<String, Object>)
        field(TelemetryCollectorManager.class, "collectors")
            .get(TelemetryCollectorManager.getInstance());
  }

  private static Field field(Class<?> type, String name) throws NoSuchFieldException {
    for (Class<?> k = type; k != null; k = k.getSuperclass()) {
      try {
        Field f = k.getDeclaredField(name);
        f.setAccessible(true);
        return f;
      } catch (NoSuchFieldException ignored) {
        // Look in the superclass.
      }
    }
    throw new NoSuchFieldException(name);
  }
}
