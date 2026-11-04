package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.core.retry.RetryStrategy;
import com.databricks.sdk.core.utils.SystemTimer;
import java.io.IOException;
import java.net.BindException;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLPeerUnverifiedException;
import org.apache.http.HttpHost;
import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.conn.ConnectionPoolTimeoutException;
import org.apache.http.conn.HttpHostConnectException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;

class DatabricksRetryStrategyPickerTest {
  private final DatabricksRetryStrategyPicker picker = new DatabricksRetryStrategyPicker(900);

  @ParameterizedTest
  @CsvSource({
    "POST, /api/2.0/sql/statements/sessions/, true",
    "POST, /api/2.0/sql/sessions/, true",
    "POST, /api/2.0/sql/warehouses/warehouse/sessions, true",
    "DELETE, /api/2.0/sql/sessions/session, true",
    "DELETE, /api/2.0/sql/statements/sessions/session, true",
    "DELETE, /api/2.0/sql/warehouses/warehouse/sessions/session, true",
    "GET, /api/2.0/sql/statements/statement, true",
    "GET, /api/2.0/sql/statements/statement?include_manifest=true, true",
    "GET, /api/2.0/sql/statements/statement/status, true",
    "GET, /api/2.0/sql/statements/statement/result/chunks/0, true",
    "DELETE, /api/2.0/sql/statements/statement, true",
    "POST, /api/2.0/sql/statements/statement/cancel, true",
    "GET, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement, true",
    "GET, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement/status, true",
    "GET, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement/result/chunks/0, true",
    "DELETE, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement, true",
    "POST, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement/cancel, true",
    "POST, /api/2.0/sql/statements/, false",
    "POST, /api/2.0/sql/warehouses/warehouse/sessions/session/statements, false",
    "POST, /api/2.0/sql/sessions/unrecognized-operation, false",
    "POST, /api/2.0/jobs/run-now, false",
    "GET, /unrecognized-path, false"
  })
  void selectsPolicyForRelativeAndAbsoluteUrls(String method, String path, boolean idempotent) {
    DatabricksError serverFailure = new DatabricksError("INTERNAL_ERROR", "Server error", 500);
    for (String prefix : List.of("", "https://example.com")) {
      RetryStrategy strategy = picker.getRetryStrategy(new Request(method, prefix + path));
      assertEquals(idempotent, strategy.isRetriable(serverFailure), method + " " + prefix + path);
    }
  }

  @ParameterizedTest
  @MethodSource("connectionFailures")
  void retriesConnectionFailuresForStatementSubmission(IOException failure) {
    RetryStrategy strategy =
        picker.getRetryStrategy(new Request("POST", "/api/2.0/sql/statements/"));
    assertTrue(strategy.isRetriable(new DatabricksError("IO_ERROR", 523, failure)));
  }

  static Stream<IOException> connectionFailures() {
    return Stream.of(
        new ConnectException("Connection refused"),
        new HttpHostConnectException(
            new ConnectException("Connection refused"), new HttpHost("example.com")),
        new ConnectTimeoutException("Connect timed out"),
        new ConnectionPoolTimeoutException("Pool timed out"),
        new NoRouteToHostException("No route to host"),
        new PortUnreachableException("Port unreachable"),
        new BindException("Cannot assign requested address"),
        new UnknownHostException("Temporary failure in name resolution"),
        new SocketException("Network is unreachable"),
        new SSLHandshakeException("Remote host terminated the handshake"),
        new IOException("Wrapped connection failure", new ConnectException("Connection refused")));
  }

  @ParameterizedTest
  @MethodSource({"connectionFailures", "ambiguousNetworkFailures"})
  void initialAttemptCountsTowardNetworkRetryTimeLimit(IOException failure) {
    AtomicLong nanoTime = new AtomicLong();
    DatabricksRetryStrategyPicker timedPicker =
        new DatabricksRetryStrategyPicker(900, nanoTime::get);
    for (String method : List.of("GET", "POST")) {
      RetryStrategy strategy =
          timedPicker.getRetryStrategy(new Request(method, "/api/2.0/sql/statements/statement"));
      nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(900));
      assertFalse(strategy.isRetriable(new DatabricksError("IO_ERROR", 523, failure)));
    }
  }

  @Test
  void networkRetryTimeLimitIsIndependentForEachRequest() {
    AtomicLong nanoTime = new AtomicLong();
    DatabricksRetryStrategyPicker timedPicker = new DatabricksRetryStrategyPicker(3, nanoTime::get);
    Request request = new Request("POST", "/api/2.0/sql/statements/");
    RetryStrategy firstRequest = timedPicker.getRetryStrategy(request);
    DatabricksError failure = new DatabricksError("IO_ERROR", 523, new ConnectTimeoutException());
    nanoTime.set(TimeUnit.SECONDS.toNanos(3));
    RetryStrategy nextRequest = timedPicker.getRetryStrategy(request);

    assertFalse(firstRequest.isRetriable(failure));
    assertTrue(nextRequest.isRetriable(failure));
  }

  @Test
  void networkRetryTimeLimitDoesNotChangeHttpStatusPolicy() {
    AtomicLong nanoTime = new AtomicLong();
    DatabricksRetryStrategyPicker timedPicker = new DatabricksRetryStrategyPicker(3, nanoTime::get);
    RetryStrategy strategy =
        timedPicker.getRetryStrategy(new Request("POST", "/api/2.0/sql/statements/"));
    nanoTime.set(TimeUnit.SECONDS.toNanos(3));

    assertTrue(strategy.isRetriable(new DatabricksError("HTTP_ERROR", "Unavailable", 503)));
  }

  @ParameterizedTest
  @MethodSource("ambiguousNetworkFailures")
  void retriesAmbiguousFailuresOnlyForIdempotentRequests(IOException failure) {
    DatabricksError error = new DatabricksError("IO_ERROR", 523, failure);
    assertFalse(
        picker
            .getRetryStrategy(new Request("POST", "/api/2.0/sql/statements/"))
            .isRetriable(error));
    assertTrue(
        picker
            .getRetryStrategy(new Request("GET", "/api/2.0/sql/statements/statement"))
            .isRetriable(error));
  }

  static Stream<IOException> ambiguousNetworkFailures() {
    return Stream.of(
        new SocketTimeoutException("Read timed out"),
        new SocketException("Connection reset"),
        new NoHttpResponseException("Server failed to respond"),
        new IOException("Unknown I/O failure"));
  }

  @ParameterizedTest
  @MethodSource("certificateFailures")
  void doesNotRetryCertificateFailures(IOException failure) {
    DatabricksError error = new DatabricksError("IO_ERROR", 523, failure);
    for (String method : List.of("GET", "POST")) {
      assertFalse(
          picker
              .getRetryStrategy(new Request(method, "/api/2.0/sql/statements/statement"))
              .isRetriable(error));
    }
  }

  static Stream<IOException> certificateFailures() {
    return Stream.of(
        new SSLHandshakeException("PKIX path building failed"),
        new SSLPeerUnverifiedException("Hostname mismatch"),
        new IOException("Wrapped SSL failure", new SSLHandshakeException("Invalid certificate")));
  }

  @ParameterizedTest
  @CsvSource({
    "307, false",
    "400, false",
    "401, false",
    "403, false",
    "429, true",
    "501, true",
    "503, true"
  })
  void preservesSdkHttpPolicy(int status, boolean retryable) {
    DatabricksError error = new DatabricksError("HTTP_ERROR", "Server error", status);
    for (String method : List.of("GET", "POST")) {
      assertEquals(
          retryable,
          picker
              .getRetryStrategy(new Request(method, "/api/2.0/sql/statements/statement"))
              .isRetriable(error));
    }
  }

  @ParameterizedTest
  @MethodSource("retryingRequests")
  void driverRetriesThroughSdkWithCorrectPolicy(String method, String path, IOException failure)
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    HttpClient transport =
        request -> {
          if (attempts.incrementAndGet() == 1) {
            if (failure != null) {
              throw failure;
            }
            return new Response(request, 500, "Internal Server Error", Map.of(), "{}");
          }
          return new Response(request, 200, "OK", Map.of(), "{}");
        };
    DatabricksConfig config = config(transport, false);
    try (MockedConstruction<SystemTimer> timers = mockConstruction(SystemTimer.class)) {
      ApiClient client = DatabricksSdkClient.buildApiClient(config, null, true, 900);
      client.execute(new Request(method, path, "{}"), Void.class);
      assertEquals(2, attempts.get());
      verify(timers.constructed().get(0)).sleep(anyLong());
    }
  }

  static Stream<Arguments> retryingRequests() {
    return Stream.of(
        Arguments.of("POST", "/api/2.0/sql/sessions/", null),
        Arguments.of("GET", "/api/2.0/sql/statements/statement", null),
        Arguments.of("DELETE", "/api/2.0/sql/statements/statement", null),
        Arguments.of("POST", "/api/2.0/sql/warehouses/warehouse/sessions", null),
        Arguments.of("POST", "/api/2.0/sql/statements/", new ConnectTimeoutException()),
        Arguments.of("POST", "/api/2.0/sql/statements/", new ConnectionPoolTimeoutException()));
  }

  @Test
  void limitsSdkNetworkRetries() throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    ConnectTimeoutException failure = new ConnectTimeoutException("Connect timed out");
    DatabricksConfig config =
        config(
            request -> {
              attempts.incrementAndGet();
              throw failure;
            },
            false);
    try (MockedConstruction<SystemTimer> timers = mockConstruction(SystemTimer.class)) {
      ApiClient client = DatabricksSdkClient.buildApiClient(config, null, true, 900);
      DatabricksException error =
          assertThrows(
              DatabricksException.class,
              () ->
                  client.execute(
                      new Request("POST", "/api/2.0/sql/statements/", "{}"), Void.class));
      assertEquals(4, attempts.get());
      assertSame(failure, error.getCause().getCause());
      verify(timers.constructed().get(0), times(3)).sleep(anyLong());
    }
  }

  @ParameterizedTest
  @CsvSource({"429", "503"})
  void sdkUsesBackoffWhenRetryAfterIsAbsent(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config =
        config(
            request ->
                new Response(
                    request,
                    attempts.incrementAndGet() == 1 ? status : 200,
                    "Injected response",
                    Map.of(),
                    "{}"),
            false);
    try (MockedConstruction<SystemTimer> timers = mockConstruction(SystemTimer.class)) {
      ApiClient client = DatabricksSdkClient.buildApiClient(config, null, true, 900);
      client.execute(new Request("POST", "/api/2.0/sql/statements/", "{}"), Void.class);
      assertEquals(2, attempts.get());
      ArgumentCaptor<Long> delay = ArgumentCaptor.forClass(Long.class);
      verify(timers.constructed().get(0)).sleep(delay.capture());
      assertTrue(delay.getValue() >= 1050 && delay.getValue() <= 1750);
    }
  }

  @Test
  void honorsDisabledSdkRetries() {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config =
        config(
            request -> {
              attempts.incrementAndGet();
              throw new ConnectTimeoutException("Connect timed out");
            },
            true);
    ApiClient client = DatabricksSdkClient.buildApiClient(config, null, true, 900);
    assertThrows(
        DatabricksException.class,
        () -> client.execute(new Request("POST", "/api/2.0/sql/statements/", "{}"), Void.class));
    assertEquals(1, attempts.get());
  }

  private DatabricksConfig config(HttpClient transport, boolean disableRetries) {
    DatabricksConfig config = mock(DatabricksConfig.class);
    when(config.getHttpClient()).thenReturn(transport);
    when(config.getHost()).thenReturn("https://example.com");
    when(config.authenticate()).thenReturn(Map.of());
    when(config.getAuthType()).thenReturn("pat");
    when(config.getDisableRetries()).thenReturn(disableRetries);
    return config;
  }
}
