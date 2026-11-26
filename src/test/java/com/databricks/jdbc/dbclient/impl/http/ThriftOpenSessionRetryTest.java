package com.databricks.jdbc.dbclient.impl.http;

import static com.databricks.jdbc.common.EnvironmentVariables.JDBC_THRIFT_VERSION;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.IDatabricksComputeResource;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.dbclient.impl.thrift.DatabricksThriftServiceClient;
import com.databricks.jdbc.exception.DatabricksHttpException;
import com.databricks.jdbc.model.client.thrift.generated.*;
import com.databricks.sdk.core.DatabricksConfig;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TMessageType;
import org.apache.thrift.transport.TMemoryBuffer;
import org.apache.thrift.transport.TMemoryInputTransport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;

class ThriftOpenSessionRetryTest {

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void retriesRealOpenSessionButNotStatementOrClose(int statusCode) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(statusCode)) {
      endpoint.failuresBeforeSuccess = 1;
      ImmutableSessionInfo sessionInfo = endpoint.openSession();
      assertNotNull(sessionInfo);
      assertEquals(2, endpoint.attempts.get());
      assertEquals(1, endpoint.sleeps.size());
      assertTrue(endpoint.sleeps.get(0) >= 800 && endpoint.sleeps.get(0) < 1200);
      assertArrayEquals(endpoint.payloads.get(0), endpoint.payloads.get(1));

      endpoint.failuresBeforeSuccess = Integer.MAX_VALUE;
      endpoint.attempts.set(0);
      IDatabricksSession session = mock(IDatabricksSession.class);
      when(session.getSessionInfo()).thenReturn(sessionInfo);
      when(session.getSessionId()).thenReturn(sessionInfo.sessionId());
      assertThrows(
          SQLException.class,
          () ->
              endpoint.service.executeStatement(
                  "SELECT 1",
                  sessionInfo.computeResource(),
                  Collections.emptyMap(),
                  StatementType.SQL,
                  session,
                  null,
                  null));
      assertEquals(1, endpoint.attempts.get());
      assertEquals("ExecuteStatement", endpoint.methods.get(2));
      endpoint.attempts.set(0);
      assertThrows(SQLException.class, () -> endpoint.service.deleteSession(sessionInfo));
      assertEquals(1, endpoint.attempts.get());
      assertEquals("CloseSession", endpoint.methods.get(3));
      assertEquals(1, endpoint.sleeps.size());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void persistentFailureExhaustsDefaultBudgetWithHttpDiagnostics(int statusCode) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(statusCode)) {
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), statusCode);
      long budgetMillis = statusCode == 429 ? 120_000 : 900_000;
      long sleepMillis = endpoint.sleeps.stream().mapToLong(Long::longValue).sum();
      assertTrue(sleepMillis <= budgetMillis);
      assertTrue(sleepMillis > budgetMillis - 38_400);
      assertEquals(endpoint.sleeps.size() + 1, endpoint.attempts.get());
      assertTrue(endpoint.attempts.get() < 45);
      assertTrue(endpoint.sleeps.stream().allMatch(delay -> delay >= 800 && delay < 38_400));
    }
  }

  @Test
  void customCodeLongSequenceKeepsLegacyBackoffWithoutOverflow() throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(503)) {
      when(endpoint.connectionContext.getApiRetriableHttpCodes())
          .thenReturn(Collections.singleton(503));
      when(endpoint.connectionContext.getApiRetryTimeout()).thenReturn(900);
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), 503);
      assertEquals(92, endpoint.attempts.get());
      assertEquals(894_000L, endpoint.sleeps.stream().mapToLong(Long::longValue).sum());
      assertEquals(Arrays.asList(2000L, 4000L, 8000L), endpoint.sleeps.subList(0, 3));
      assertTrue(
          endpoint.sleeps.subList(3, endpoint.sleeps.size()).stream()
              .allMatch(delay -> delay == 10_000));
    }
  }

  @Test
  void delayedGatewayResponsesCountAgainstWallClockBudget() throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(504)) {
      when(endpoint.connectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(4);
      endpoint.realTime = true;
      endpoint.responseDelayMillis = 2000;
      long started = System.nanoTime();
      SQLException exception = assertThrows(SQLException.class, endpoint::openSession);
      long elapsedMillis = (System.nanoTime() - started) / 1_000_000;
      assertHttpDiagnostics(exception, 504);
      assertEquals(2, endpoint.attempts.get());
      assertTrue(
          elapsedMillis <= 10000,
          "Four-second budget, one two-second attempt and four-second CI scheduling allowance: "
              + elapsedMillis);
    }
  }

  @ParameterizedTest
  @CsvSource({"503,429", "429,503", "503,502", "502,503", "429,502", "502,429"})
  void mixedCategoriesStartIndependentBudgetWindows(int firstStatus, int secondStatus)
      throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(firstStatus)) {
      endpoint.mixCategories(firstStatus, secondStatus, "5");
      assertNotNull(endpoint.openSession());
      assertEquals(7, endpoint.attempts.get());
      assertEquals(155_000, endpoint.clockMillis.get());
      assertEquals(
          Arrays.asList(30_000L, 30_000L, 30_000L, 30_000L, 30_000L, 5000L), endpoint.sleeps);
    }
  }

  @ParameterizedTest
  @CsvSource({"503,429", "429,503", "503,502", "502,503", "429,502", "502,429"})
  void fallbackBackoffStartsFreshForEachCategory(int firstStatus, int secondStatus)
      throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(firstStatus)) {
      endpoint.mixCategories(firstStatus, secondStatus, null);
      assertNotNull(endpoint.openSession());
      assertEquals(7, endpoint.attempts.get());
      long delay = endpoint.sleeps.get(5);
      if (secondStatus == 502) {
        assertEquals(2000L, delay);
      } else {
        assertTrue(delay >= 800 && delay < 1200, "First category-local backoff: " + delay);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"503,500", "500,503"})
  void customCodesShareOneOpenSessionBudget(int firstStatus, int secondStatus) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(firstStatus)) {
      when(endpoint.connectionContext.getApiRetriableHttpCodes())
          .thenReturn(new java.util.HashSet<>(Arrays.asList(500, 503)));
      when(endpoint.connectionContext.getApiRetryTimeout()).thenReturn(160);
      endpoint.responseStatuses =
          Arrays.asList(
              firstStatus, firstStatus, firstStatus, firstStatus, firstStatus, secondStatus, 200);
      endpoint.retryAfterHeaders = Arrays.asList("30", "30", "30", "30", "30", "20", null);
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), secondStatus);
      assertEquals(6, endpoint.attempts.get());
      assertEquals(150_000, endpoint.clockMillis.get());
      assertEquals(5, endpoint.sleeps.size());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void interruptedOpenSessionSleepEndsWithCheckedFailure(int statusCode) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(statusCode)) {
      endpoint.interruptSleep = true;
      try {
        SQLException failure = assertThrows(SQLException.class, endpoint::openSession);
        assertTrue(Thread.currentThread().isInterrupted());
        assertEquals(1, endpoint.attempts.get());
        assertEquals(1, endpoint.sleeps.size());
        Throwable cause = failure;
        while (cause != null && !(cause instanceof DatabricksHttpException)) {
          cause = cause.getCause();
        }
        assertInstanceOf(DatabricksHttpException.class, cause);
        assertEquals(0, ((DatabricksHttpException) cause).getErrorCode());
        assertTrue(cause.getMessage().contains("HTTP response code: " + statusCode));
      } finally {
        Thread.interrupted();
      }
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void zeroBudgetPreservesRequestIdAndServerMessage(int statusCode) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(statusCode)) {
      when(endpoint.connectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(0);
      when(endpoint.connectionContext.getRateLimitRetryTimeout()).thenReturn(0);
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), statusCode);
      assertEquals(1, endpoint.attempts.get());
      assertTrue(endpoint.sleeps.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {503, 429, 502, 504, 408})
  void disabledRetryPreservesRequestIdAndServerMessage(int statusCode) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(statusCode)) {
      when(endpoint.connectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(false);
      when(endpoint.connectionContext.shouldRetryRateLimitError()).thenReturn(false);
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), statusCode);
      assertEquals(1, endpoint.attempts.get());
      assertTrue(endpoint.sleeps.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"0", "-3", "invalid", "Tue, 24 Nov 2026 12:00:00 GMT"})
  void invalidRetryAfterUsesBoundedFallback(String retryAfter) throws Exception {
    try (RetryEndpoint endpoint = new RetryEndpoint(502)) {
      endpoint.retryAfter = retryAfter;
      assertHttpDiagnostics(assertThrows(SQLException.class, endpoint::openSession), 502);
      assertTrue(endpoint.attempts.get() > 20 && endpoint.attempts.get() < 45);
      assertTrue(endpoint.sleeps.stream().allMatch(delay -> delay > 0));
    }
  }

  private static void assertHttpDiagnostics(SQLException exception, int statusCode) {
    Throwable cause = exception;
    while (cause != null) {
      if (cause instanceof DatabricksHttpException
          && "08000".equals(((DatabricksHttpException) cause).getSQLState())) {
        DatabricksHttpException httpException = (DatabricksHttpException) cause;
        assertEquals(0, httpException.getErrorCode());
        assertTrue(httpException.getMessage().contains("code: " + statusCode));
        assertTrue(httpException.getMessage().contains("Request ID: req-abc-123"));
        assertTrue(httpException.getMessage().contains("Error message: upstream boom"));
        return;
      }
      cause = cause.getCause();
    }
    fail("Missing original HTTP exception", exception);
  }

  private static class RetryEndpoint implements AutoCloseable {
    private final IDatabricksConnectionContext connectionContext =
        mock(IDatabricksConnectionContext.class);
    private final List<Long> sleeps = new ArrayList<>();
    private final List<byte[]> payloads = Collections.synchronizedList(new ArrayList<>());
    private final List<String> methods = Collections.synchronizedList(new ArrayList<>());
    private final AtomicInteger attempts = new AtomicInteger();
    private final AtomicLong clockMillis = new AtomicLong();
    private final HttpServer server;
    private final DatabricksHttpClient httpClient;
    private final MockedStatic<DatabricksHttpClientFactory> clients;
    private final MockedStatic<DatabricksClientConfiguratorManager> configurators;
    private final DatabricksThriftServiceClient service;
    private int failuresBeforeSuccess = Integer.MAX_VALUE;
    private long responseDelayMillis;
    private String retryAfter;
    private boolean realTime;
    private boolean interruptSleep;
    private List<Integer> responseStatuses = Collections.emptyList();
    private List<String> retryAfterHeaders = Collections.emptyList();

    private RetryEndpoint(int statusCode) throws Exception {
      when(connectionContext.shouldRetryTemporarilyUnavailableError()).thenReturn(true);
      when(connectionContext.shouldRetryRateLimitError()).thenReturn(true);
      when(connectionContext.getTemporarilyUnavailableRetryTimeout()).thenReturn(900);
      when(connectionContext.getRateLimitRetryTimeout()).thenReturn(120);
      DatabricksConfig config = mock(DatabricksConfig.class);
      when(config.authenticate()).thenReturn(Collections.emptyMap());
      DatabricksHttpRetryHandler retryHandler =
          new DatabricksHttpRetryHandler(connectionContext) {
            @Override
            protected void doSleepForDelay(long delayMillis) {
              assertTrue(delayMillis > 0);
              sleeps.add(delayMillis);
              if (interruptSleep) {
                Thread.currentThread().interrupt();
              }
              if (realTime || interruptSleep) {
                super.doSleepForDelay(delayMillis);
              } else {
                clockMillis.addAndGet(delayMillis);
              }
            }

            @Override
            protected long currentTimeMillis() {
              return realTime ? super.currentTimeMillis() : clockMillis.get();
            }
          };
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
      server.createContext(
          "/thrift",
          exchange -> {
            byte[] request = exchange.getRequestBody().readAllBytes();
            payloads.add(request);
            try {
              TMessage message =
                  new TBinaryProtocol(new TMemoryInputTransport(request)).readMessageBegin();
              methods.add(message.name);
              int attempt = attempts.incrementAndGet();
              int responseStatus =
                  responseStatuses.isEmpty()
                      ? attempt <= failuresBeforeSuccess ? statusCode : 200
                      : responseStatuses.get(Math.min(attempt - 1, responseStatuses.size() - 1));
              if (responseDelayMillis > 0) {
                try {
                  Thread.sleep(responseDelayMillis);
                } catch (InterruptedException exception) {
                  Thread.currentThread().interrupt();
                  throw new IOException(exception);
                }
              }
              byte[] response = "{\"message\":\"upstream boom\"}".getBytes(StandardCharsets.UTF_8);
              if (responseStatus == 200) {
                TMemoryBuffer buffer = new TMemoryBuffer(256);
                TBinaryProtocol protocol = new TBinaryProtocol(buffer);
                protocol.writeMessageBegin(
                    new TMessage(message.name, TMessageType.REPLY, message.seqid));
                new TCLIService.OpenSession_result()
                    .setSuccess(
                        new TOpenSessionResp()
                            .setStatus(new TStatus(TStatusCode.SUCCESS_STATUS))
                            .setServerProtocolVersion(JDBC_THRIFT_VERSION)
                            .setSessionHandle(
                                new TSessionHandle(
                                    new THandleIdentifier(
                                        ByteBuffer.wrap(new byte[16]),
                                        ByteBuffer.wrap(new byte[16])))))
                    .write(protocol);
                protocol.writeMessageEnd();
                response = Arrays.copyOf(buffer.getArray(), buffer.length());
              }
              exchange.getResponseHeaders().set("x-request-id", "req-abc-123");
              String retryAfterHeader =
                  retryAfterHeaders.isEmpty()
                      ? retryAfter
                      : retryAfterHeaders.get(Math.min(attempt - 1, retryAfterHeaders.size() - 1));
              if (retryAfterHeader != null) {
                exchange.getResponseHeaders().set("Retry-After", retryAfterHeader);
              }
              exchange.sendResponseHeaders(responseStatus, response.length);
              exchange.getResponseBody().write(response);
            } catch (TException exception) {
              throw new IOException(exception);
            } finally {
              exchange.close();
            }
          });
      server.start();
      when(connectionContext.getEndpointURL())
          .thenReturn("http://127.0.0.1:" + server.getAddress().getPort() + "/thrift");
      PoolingHttpClientConnectionManager connections = new PoolingHttpClientConnectionManager();
      CloseableHttpClient apacheClient =
          HttpClients.custom()
              .setConnectionManager(connections)
              .addInterceptorFirst(retryHandler)
              .setRetryHandler(retryHandler)
              .build();
      httpClient = new DatabricksHttpClient(apacheClient, connections);
      clients = mockStatic(DatabricksHttpClientFactory.class);
      DatabricksHttpClientFactory clientFactory = mock(DatabricksHttpClientFactory.class);
      clients.when(DatabricksHttpClientFactory::getInstance).thenReturn(clientFactory);
      when(clientFactory.getClient(connectionContext)).thenReturn(httpClient);
      configurators = mockStatic(DatabricksClientConfiguratorManager.class);
      DatabricksClientConfiguratorManager configuratorManager =
          mock(DatabricksClientConfiguratorManager.class);
      configurators
          .when(DatabricksClientConfiguratorManager::getInstance)
          .thenReturn(configuratorManager);
      ClientConfigurator configurator = mock(ClientConfigurator.class);
      when(configuratorManager.getConfigurator(connectionContext)).thenReturn(configurator);
      when(configurator.getDatabricksConfig()).thenReturn(config);
      service = new DatabricksThriftServiceClient(connectionContext);
    }

    private void mixCategories(int firstStatus, int secondStatus, String secondRetryAfter) {
      when(connectionContext.getApiRetriableHttpCodes()).thenReturn(Collections.singleton(502));
      when(connectionContext.getTemporarilyUnavailableRetryTimeout())
          .thenReturn(firstStatus == 503 ? 900 : 120);
      when(connectionContext.getRateLimitRetryTimeout()).thenReturn(firstStatus == 429 ? 900 : 120);
      when(connectionContext.getApiRetryTimeout()).thenReturn(firstStatus == 502 ? 900 : 120);
      responseStatuses =
          Arrays.asList(
              firstStatus, firstStatus, firstStatus, firstStatus, firstStatus, secondStatus, 200);
      retryAfterHeaders = Arrays.asList("30", "30", "30", "30", "30", secondRetryAfter, null);
    }

    private ImmutableSessionInfo openSession() throws SQLException {
      return service.createSession(
          mock(IDatabricksComputeResource.class), null, null, Collections.emptyMap());
    }

    @Override
    public void close() throws IOException {
      try {
        httpClient.close();
      } finally {
        configurators.close();
        clients.close();
        server.stop(0);
      }
    }
  }
}
