package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.log.LogCapture;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.utils.Timer;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

class CreateSessionRetryHandlerTest {
  private final Request request =
      new Request(Request.POST, "https://workspace.example.com/api/2.0/sql/warehouses/wh/sessions");
  private final FakeTimer timer = new FakeTimer();
  private final DatabricksConfig config = mock(DatabricksConfig.class);

  @Test
  void timeoutsAreReadOnceBeforeTransportRuns() throws Exception {
    IDatabricksConnectionContext context = spy(context(5, 5, null));
    CreateSessionRetryHandler handler =
        new CreateSessionRetryHandler(context, timer, () -> 1.0, "wh");
    verify(context).getRateLimitRetryTimeout();
    verify(context).getTemporarilyUnavailableRetryTimeout();
    Queue<Response> responses =
        new ArrayDeque<>(
            Arrays.asList(response(429, "1"), response(503, "1"), response(200, null)));
    handler.execute(ignored -> responses.remove(), request, config);
    verify(context).getRateLimitRetryTimeout();
    verify(context).getTemporarilyUnavailableRetryTimeout();
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void longerBudgetsAllowMoreAttempts(int status) throws Exception {
    for (int seconds : new int[] {1, 5, 10}) {
      FakeTimer clock = new FakeTimer();
      AtomicInteger attempts = new AtomicInteger();
      CreateSessionRetryHandler handler =
          new CreateSessionRetryHandler(context(seconds, seconds, null), clock, () -> 1.0, "wh");
      handler.execute(
          ignored -> {
            attempts.incrementAndGet();
            return response(status, "1");
          },
          request,
          config);
      assertEquals(seconds + 1, attempts.get());
      assertEquals(seconds * 1000L, clock.now);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void attemptTimeCountsFromFirstRetryableResponse(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    CreateSessionRetryHandler handler = handler(4, 4, null);
    handler.execute(
        ignored -> {
          timer.now += 3000;
          attempts.incrementAndGet();
          return response(status, "1");
        },
        request,
        config);
    assertEquals(2, attempts.get());
    assertEquals(List.of(1000L), timer.sleeps);
    assertEquals(7000, timer.now);
  }

  @Test
  void categoriesHaveIndependentStartsAndKeepTheirDeadlines() throws Exception {
    Queue<Response> responses =
        new ArrayDeque<>(
            Arrays.asList(
                response(429, "1"), response(503, "1"), response(503, "1"), response(429, "1")));
    Response last = handler(2, 4, null).execute(ignored -> responses.remove(), request, config);
    assertEquals(429, last.getStatusCode());
    assertTrue(responses.isEmpty());
    assertEquals(List.of(1000L, 1000L, 1000L), timer.sleeps);
  }

  @Test
  void budgetPersistsAcrossSdkRetriesOfOtherStatuses() throws Exception {
    CreateSessionRetryHandler handler = handler(3, 3, null);
    Queue<Response> responses =
        new ArrayDeque<>(Arrays.asList(response(429, "1"), response(501, "1")));
    assertEquals(
        501, handler.execute(ignored -> responses.remove(), request, config).getStatusCode());
    timer.now += 3000;
    Response last = response(429, "1");
    assertSame(last, handler.execute(ignored -> last, request, config));
    assertEquals(List.of(1000L), timer.sleeps);
  }

  @ParameterizedTest
  @CsvSource({"429, RateLimitRetry", "503, TemporarilyUnavailableRetry"})
  void retryTogglesAreHonored(int status, String disabledOption) throws Exception {
    Response last = response(status, "1");
    assertSame(last, handler(120, 900, disabledOption).execute(ignored -> last, request, config));
    assertTrue(timer.sleeps.isEmpty());
    assertEquals(0, timer.clockReads);
  }

  @ParameterizedTest
  @NullAndEmptySource
  @ValueSource(strings = {"invalid", "0", "-1"})
  void missingOrInvalidRetryAfterUsesCappedExponentialBackoff(String retryAfter) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    Response success = response(200, null);
    assertSame(
        success,
        handler(120, 900, null)
            .execute(
                ignored -> attempts.incrementAndGet() <= 8 ? response(503, retryAfter) : success,
                request,
                config));
    assertEquals(List.of(1000L, 2000L, 4000L, 8000L, 16000L, 32000L, 32000L, 32000L), timer.sleeps);
  }

  @ParameterizedTest
  @ValueSource(doubles = {0.8, 1.0, 1.1999})
  void fallbackAppliesJitter(double jitter) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    new CreateSessionRetryHandler(context(120, 900, null), timer, () -> jitter, "wh")
        .execute(
            ignored -> response(attempts.incrementAndGet() == 1 ? 503 : 200, null),
            request,
            config);
    assertEquals(List.of((long) (1000 * jitter)), timer.sleeps);
  }

  @Test
  void positiveRetryAfterTakesPrecedenceAndMatchesHeaderCaseInsensitively() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    handler(120, 900, null)
        .execute(
            ignored -> response(attempts.incrementAndGet() == 1 ? 503 : 200, "7"), request, config);
    assertEquals(List.of(7000L), timer.sleeps);
  }

  @Test
  void delayOutsideBudgetReturnsOriginalResponseWithoutWaiting() throws Exception {
    Response last = response(429, "100");
    CreateSessionRetryHandler handler = handler(1, 900, null);
    try (LogCapture logs = new LogCapture()) {
      assertSame(last, handler.execute(ignored -> last, request, config));
      assertTrue(logs.getOutput().contains("Retry-After exceeds remaining retry budget"));
    }
    assertTrue(timer.sleeps.isEmpty());
  }

  @Test
  void maximumTimeoutUsesLongMilliseconds() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    handler(Integer.MAX_VALUE, Integer.MAX_VALUE, null)
        .execute(
            ignored -> response(attempts.incrementAndGet() == 1 ? 429 : 200, "2147483647"),
            request,
            config);
    assertEquals(2, attempts.get());
    assertEquals(List.of(2147483647000L), timer.sleeps);
  }

  @Test
  void scheduledRetryStillRunsAfterOversleep() throws Exception {
    timer.oversleep = 5000;
    AtomicInteger attempts = new AtomicInteger();
    handler(2, 2, null)
        .execute(
            ignored -> {
              attempts.incrementAndGet();
              return response(503, "1");
            },
            request,
            config);
    assertEquals(2, attempts.get());
  }

  @Test
  void interruptionRestoresFlagAndReportsInterruption() throws Exception {
    timer.interruptSleep = true;
    Response last = response(503, "1");
    try {
      DatabricksException error =
          assertThrows(
              DatabricksException.class,
              () -> handler(120, 900, null).execute(ignored -> last, request, config));
      assertEquals("Current thread was interrupted", error.getMessage());
      assertInstanceOf(InterruptedException.class, error.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
    } finally {
      Thread.interrupted();
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void retryDiagnosticsContainOnlySafeRequestDetails(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    request.withHeader("Authorization", "Bearer sensitive-token");
    CreateSessionRetryHandler handler = handler(5, 5, null);
    try (LogCapture logs = new LogCapture()) {
      handler.execute(
          ignored ->
              new Response(
                  request,
                  attempts.incrementAndGet() <= 2 ? status : 200,
                  "test",
                  Map.of("Retry-After", List.of("1"), "x-ReQuEsT-iD", List.of("retry-request")),
                  "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\",\"message\":\"sensitive-message\"}"),
          request,
          config);
      String output = logs.getOutput();
      assertTrue(output.contains("status=" + status));
      assertTrue(output.contains("error_code=TEMPORARILY_UNAVAILABLE"));
      assertTrue(output.contains("requestId=retry-request"));
      assertTrue(output.contains("delayMs=1000"));
      assertTrue(output.contains("attempt=2"));
      assertTrue(output.contains("attempt=1"));
      assertTrue(output.contains("remainingMs=5000"));
      assertTrue(output.contains("warehouseId=wh"));
      assertFalse(output.contains("workspace.example.com"));
      assertFalse(output.contains("sensitive-token"));
      assertFalse(output.contains("sensitive-message"));
    }
  }

  @Test
  void retryClosesDiscardedBodyAndRefreshesAuthentication() throws Exception {
    ByteArrayInputStream discarded = spy(new ByteArrayInputStream(new byte[0]));
    ByteArrayInputStream retained = spy(new ByteArrayInputStream(new byte[0]));
    Response failure =
        new Response(request, request.getUri().toURL(), 503, "test", Map.of(), discarded);
    Response success =
        new Response(request, request.getUri().toURL(), 200, "test", Map.of(), retained);
    when(config.authenticate()).thenReturn(Map.of("Authorization", "Bearer refreshed"));
    Queue<Response> responses = new ArrayDeque<>(Arrays.asList(failure, success));
    assertSame(
        success, handler(120, 900, null).execute(ignored -> responses.remove(), request, config));
    verify(discarded).close();
    verify(retained, never()).close();
    verify(config).authenticate();
    assertEquals("Bearer refreshed", request.getHeaders().get("Authorization"));
  }

  @Test
  void retryDiagnosticsUseStructuredRequestIdWithoutHeaders() throws Exception {
    Response failure =
        new Response(
            request,
            503,
            "test",
            Map.of(),
            "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\",\"details\":[{"
                + "\"@type\":\"type.googleapis.com/google.rpc.RequestInfo\","
                + "\"request_id\":\"body-request\",\"serving_data\":\"sensitive-data\"}]}");
    Queue<Response> responses = new ArrayDeque<>(Arrays.asList(failure, response(200, null)));
    try (LogCapture logs = new LogCapture()) {
      handler(5, 5, null).execute(ignored -> responses.remove(), request, config);
      assertTrue(logs.getOutput().contains("error_code=TEMPORARILY_UNAVAILABLE"));
      assertTrue(logs.getOutput().contains("requestId=body-request"));
      assertFalse(logs.getOutput().contains("sensitive-data"));
    }
  }

  @Test
  void retryDiagnosticsBoundBodyReads() throws Exception {
    ByteArrayInputStream body = new ByteArrayInputStream(new byte[16384]);
    Response failure = new Response(request, request.getUri().toURL(), 503, "test", Map.of(), body);
    Queue<Response> responses = new ArrayDeque<>(Arrays.asList(failure, response(200, null)));
    assertEquals(
        200,
        handler(5, 5, null)
            .execute(ignored -> responses.remove(), request, config)
            .getStatusCode());
    assertEquals(8192, body.available());
  }

  @Test
  void diagnosticReadFailureDoesNotPreventRetryOrLoseHeaderRequestId() throws Exception {
    InputStream body = mock(InputStream.class);
    when(body.read(any(byte[].class), anyInt(), anyInt())).thenThrow(new IOException("test"));
    Response failure =
        new Response(
            request,
            request.getUri().toURL(),
            429,
            "test",
            Map.of("X-Request-Id", List.of("header-request")),
            body);
    Queue<Response> responses = new ArrayDeque<>(Arrays.asList(failure, response(200, null)));
    try (LogCapture logs = new LogCapture()) {
      assertEquals(
          200,
          handler(5, 5, null)
              .execute(ignored -> responses.remove(), request, config)
              .getStatusCode());
      assertTrue(logs.getOutput().contains("requestId=header-request"));
    }
    verify(body).close();
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 307, 400, 401, 403, 404, 408, 500, 501, 502, 504})
  void otherStatusesDoNotTouchClockOrRetry(int status) throws Exception {
    Response last = response(status, "1");
    assertSame(last, handler(120, 900, null).execute(ignored -> last, request, config));
    assertEquals(0, timer.clockReads);
    assertTrue(timer.sleeps.isEmpty());
    verifyNoInteractions(config);
  }

  @Test
  void transportFailuresAreLeftToSdk() throws Exception {
    IOException error = new IOException("test failure");
    HttpClient transport =
        ignored -> {
          throw error;
        };
    assertSame(
        error,
        assertThrows(
            IOException.class, () -> handler(120, 900, null).execute(transport, request, config)));
    assertTrue(timer.sleeps.isEmpty());
  }

  private CreateSessionRetryHandler handler(int rateLimit, int unavailable, String disabledOption)
      throws Exception {
    return new CreateSessionRetryHandler(
        context(rateLimit, unavailable, disabledOption), timer, () -> 1.0, "wh");
  }

  private IDatabricksConnectionContext context(
      int rateLimit, int unavailable, String disabledOption) throws Exception {
    Properties properties = new Properties();
    properties.setProperty("RateLimitRetryTimeout", Integer.toString(rateLimit));
    properties.setProperty("TemporarilyUnavailableRetryTimeout", Integer.toString(unavailable));
    if (disabledOption != null) {
      properties.setProperty(disabledOption, "0");
    }
    return DatabricksConnectionContext.parse(
        "jdbc:databricks://workspace.example.com/default;AuthMech=3;httpPath=/sql/1.0/warehouses/wh",
        properties);
  }

  private Response response(int status, String retryAfter) {
    return new Response(
        request,
        status,
        "test",
        retryAfter == null ? Map.of() : Map.of("rEtRy-AfTeR", List.of(retryAfter)),
        "{}");
  }

  private static class FakeTimer implements Timer {
    long now;
    long oversleep;
    int clockReads;
    boolean interruptSleep;
    final List<Long> sleeps = new ArrayList<>();

    @Override
    public void sleep(long milliseconds) throws InterruptedException {
      if (interruptSleep) {
        throw new InterruptedException("test interruption");
      }
      sleeps.add(milliseconds);
      now += milliseconds + oversleep;
    }

    @Override
    public long getCurrentTime() {
      clockReads++;
      return now;
    }
  }
}
