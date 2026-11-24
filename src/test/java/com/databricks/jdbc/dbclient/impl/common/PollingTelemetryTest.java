package com.databricks.jdbc.dbclient.impl.common;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpClient;
import com.databricks.jdbc.dbclient.impl.http.DatabricksHttpRetryHandler;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksHttpTTransport;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.jdbc.log.LogCapture;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.jdbc.telemetry.TelemetryHelper;
import com.databricks.sdk.core.DatabricksConfig;
import com.sun.net.httpserver.HttpServer;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.MockedStatic;

class PollingTelemetryTest {
  @ParameterizedTest
  @CsvSource({
    "false,STATEMENT_EXECUTION_TIMEOUT,1007",
    "false,OPERATION_TIMEOUT_ERROR,1036",
    "true,STATEMENT_EXECUTION_TIMEOUT,1007"
  })
  void deadlineAbortExportsOnlyTimeoutAndPoolRemainsReusable(
      boolean retryAfter, DatabricksDriverErrorCode errorCode, int numericCode) throws Exception {
    CountDownLatch release = new CountDownLatch(1);
    AtomicInteger requests = new AtomicInteger();
    AtomicReference<String> emittedName = new AtomicReference<>();
    ExecutorService serverThreads = Executors.newCachedThreadPool();
    HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.setExecutor(serverThreads);
    server.createContext(
        "/",
        exchange -> {
          boolean first = requests.incrementAndGet() == 1;
          if (first && !retryAfter) {
            try {
              release.await(15, TimeUnit.SECONDS);
            } catch (InterruptedException failure) {
              Thread.currentThread().interrupt();
            }
          }
          int status = first && retryAfter ? 503 : 200;
          if (status == 503) {
            exchange.getResponseHeaders().add("Retry-After", "10");
          }
          byte[] body = "ok".getBytes(StandardCharsets.UTF_8);
          try {
            exchange.sendResponseHeaders(status, body.length);
            exchange.getResponseBody().write(body);
          } finally {
            exchange.close();
          }
        });
    server.start();
    IDatabricksConnectionContext context = mock(IDatabricksConnectionContext.class);
    when(context.getTemporarilyUnavailableRetryTimeout()).thenReturn(60);
    when(context.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
    DatabricksHttpRetryHandler retry = new DatabricksHttpRetryHandler(context);
    PollingConnectionManager pool = new PollingConnectionManager();
    pool.setMaxTotal(1);
    pool.setDefaultMaxPerRoute(1);
    try (CloseableHttpClient apache =
            HttpClients.custom()
                .setConnectionManager(pool)
                .setDefaultRequestConfig(RequestConfig.custom().setSocketTimeout(30000).build())
                .setRetryHandler(retry)
                .addInterceptorFirst(retry)
                .build();
        MockedStatic<TelemetryHelper> telemetry = mockStatic(TelemetryHelper.class);
        LogCapture logs = new LogCapture()) {
      telemetry
          .when(() -> TelemetryHelper.exportFailureLog(any(), anyString(), anyString(), any()))
          .thenAnswer(
              call -> {
                emittedName.set(call.getArgument(1));
                return null;
              });
      Constructor<DatabricksHttpClient> constructor =
          DatabricksHttpClient.class.getDeclaredConstructor(
              CloseableHttpClient.class, PoolingHttpClientConnectionManager.class);
      constructor.setAccessible(true);
      DatabricksHttpClient client = constructor.newInstance(apache, pool);
      DatabricksConfig config = mock(DatabricksConfig.class);
      when(config.authenticate()).thenReturn(Collections.emptyMap());
      DatabricksHttpTTransport transport =
          new DatabricksHttpTTransport(
              client, "http://127.0.0.1:" + server.getAddress().getPort() + "/", config, context);
      long started = System.nanoTime();
      TimeoutHandler handler =
          new TimeoutHandler(1, "Statement ID: telemetry-test", null, errorCode);
      DatabricksTimeoutException failure =
          assertThrows(
              DatabricksTimeoutException.class,
              () ->
                  handler.executeWithTimeout(
                      () -> {
                        transport.write(new byte[] {1}, 0, 1);
                        transport.flush();
                        return null;
                      }));
      assertTrue((System.nanoTime() - started) / 1e9 < 10);
      assertEquals(1, requests.get(), "Request must be in flight; retry delay must not re-send");
      assertNotNull(failure.getCause());
      assertEquals(errorCode.name(), failure.getSQLState());
      telemetry.verify(
          () -> TelemetryHelper.exportFailureLog(any(), eq(errorCode.name()), anyString(), any()),
          times(1));
      telemetry.verifyNoMoreInteractions();
      assertEquals(numericCode, DatabricksDriverErrorCode.valueOf(emittedName.get()).getCode());
      assertFalse(logs.getOutput().contains("Caught error while executing http request"));
      assertFalse(logs.getOutput().contains("Failed to flush data to server"));
      assertEquals(0, pool.getTotalStats().getLeased());
      assertFalse(Thread.currentThread().isInterrupted());
      transport.write(new byte[] {1}, 0, 1);
      transport.flush();
      assertEquals(2, requests.get());
      assertEquals(0, pool.getTotalStats().getLeased());
    } finally {
      release.countDown();
      server.stop(0);
      serverThreads.shutdownNow();
      pool.close();
    }
  }
}
