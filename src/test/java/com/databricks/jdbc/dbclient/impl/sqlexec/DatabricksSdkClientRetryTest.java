package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksRateLimitException;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

class DatabricksSdkClientRetryTest {
  private static final String JDBC_URL =
      "jdbc:databricks://workspace.example.com/default;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse;UseThriftClient=0";

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void exhaustedNonzeroBudgetUsesJdbcErrorSurface(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, Integer.MAX_VALUE);
    Properties properties = new Properties();
    properties.setProperty("RateLimitRetryTimeout", "2");
    properties.setProperty("TemporarilyUnavailableRetryTimeout", "2");
    IDatabricksConnectionContext context = DatabricksConnectionContext.parse(JDBC_URL, properties);
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> client.createSession(new Warehouse("warehouse"), null, null, null));
      assertEquals(
          status == 429 ? DatabricksRateLimitException.class : DatabricksSQLException.class,
          error.getClass());
      assertEquals(status == 429 ? "RATE_LIMIT_EXCEEDED" : "CONNECTION_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      assertTrue(error.getMessage().contains("test request_id=retry-request"));
      assertTrue(attempts.get() >= 2);
    } finally {
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void interruptedBackoffKeepsSdkInterruptionError(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, Integer.MAX_VALUE);
    HttpClient transport = config.getHttpClient();
    config.setHttpClient(
        request -> {
          Response response = transport.execute(request);
          if (response.getStatusCode() == status) {
            Thread.currentThread().interrupt();
          }
          return response;
        });
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      DatabricksException error =
          assertThrows(
              DatabricksException.class,
              () -> client.createSession(new Warehouse("warehouse"), null, null, null));
      assertEquals(DatabricksException.class, error.getClass());
      assertEquals("Current thread was interrupted", error.getMessage());
      assertInstanceOf(InterruptedException.class, error.getCause());
      assertTrue(Thread.currentThread().isInterrupted());
      assertEquals(1, attempts.get());
    } finally {
      Thread.interrupted();
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"501", "ConnectException", "IOExceptionAfter503"})
  void remainingCreateSessionPoliciesMatchSdk(String scenario) throws Exception {
    List<Exception> errors = new ArrayList<>();
    List<Integer> counts = new ArrayList<>();
    for (boolean baseline : new boolean[] {true, false}) {
      AtomicInteger attempts = new AtomicInteger();
      DatabricksConfig config = config(501, attempts, Integer.MAX_VALUE, "0");
      HttpClient transport = config.getHttpClient();
      config.setHttpClient(
          request -> {
            if (request.getUrl().contains("/.well-known/") || "501".equals(scenario)) {
              return transport.execute(request);
            }
            int attempt = attempts.incrementAndGet();
            if ("ConnectException".equals(scenario)) {
              throw new ConnectException("test connection failure");
            }
            if (attempt == 1) {
              return new Response(
                  request,
                  503,
                  "Unavailable",
                  Map.of("Retry-After", List.of("0")),
                  "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\",\"message\":\"unavailable\"}");
            }
            throw new IOException("test transport failure");
          });
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(JDBC_URL, new Properties());
      try (MockedConstruction<ClientConfigurator> ignored =
          mockConstruction(
              ClientConfigurator.class,
              (configurator, construction) ->
                  when(configurator.getDatabricksConfig()).thenReturn(config))) {
        DatabricksSdkClient client =
            baseline
                ? new DatabricksSdkClient(
                    context, null, new ApiClient.Builder().withDatabricksConfig(config).build())
                : new DatabricksSdkClient(context);
        errors.add(
            assertThrows(
                Exception.class,
                () -> client.createSession(new Warehouse("warehouse"), null, null, null)));
        counts.add(attempts.get());
      } finally {
        DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
      }
    }
    assertEquals("IOExceptionAfter503".equals(scenario) ? 2 : 4, counts.get(0));
    assertEquals(counts.get(0), counts.get(1));
    assertEquals(errors.get(0).getClass(), errors.get(1).getClass());
    assertEquals(errors.get(0).getMessage(), errors.get(1).getMessage());
    if (errors.get(0) instanceof DatabricksSQLException) {
      DatabricksSQLException baseline = (DatabricksSQLException) errors.get(0);
      DatabricksSQLException actual = (DatabricksSQLException) errors.get(1);
      assertEquals(baseline.getSQLState(), actual.getSQLState());
      assertEquals(baseline.getErrorCode(), actual.getErrorCode());
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void zeroBudgetDoesNotRetryCreateSession(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, Integer.MAX_VALUE);
    Properties properties = new Properties();
    properties.setProperty("RateLimitRetryTimeout", "0");
    properties.setProperty("TemporarilyUnavailableRetryTimeout", "0");
    IDatabricksConnectionContext context = DatabricksConnectionContext.parse(JDBC_URL, properties);
    AtomicInteger warnings = new AtomicInteger();
    Logger logger = Logger.getLogger("com.databricks.jdbc.dbclient.impl.sqlexec");
    Handler capture =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            if (record.getLevel() == Level.WARNING) {
              warnings.incrementAndGet();
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    logger.addHandler(capture);
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      DatabricksSQLException error =
          assertThrows(
              DatabricksSQLException.class,
              () -> client.createSession(new Warehouse("warehouse"), null, null, null));
      assertEquals(status == 429 ? "RATE_LIMIT_EXCEEDED" : "CONNECTION_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      DatabricksError cause = assertInstanceOf(DatabricksError.class, error.getCause());
      assertEquals(status, cause.getStatusCode());
      assertEquals("TEMPORARILY_UNAVAILABLE", cause.getErrorCode());
      assertTrue(cause.getMessage().contains("test request_id=retry-request"));
      assertEquals(1, attempts.get());
      assertEquals(1, warnings.get());
    } finally {
      logger.removeHandler(capture);
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void budgetAllowsMoreThanFourCreateSessionAttempts(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, 4);
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      assertEquals(
          "session",
          client.createSession(new Warehouse("warehouse"), null, null, null).sessionId());
      assertEquals(5, attempts.get());
    } finally {
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "429, true, RateLimitRetry",
    "503, true, TemporarilyUnavailableRetry",
    "429, false, RateLimitRetry",
    "503, false, TemporarilyUnavailableRetry"
  })
  void disabledRetriesArePreserved(int status, boolean sdkDisabled, String option)
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config =
        config(status, attempts, Integer.MAX_VALUE).setDisableRetries(sdkDisabled);
    Properties properties = new Properties();
    if (!sdkDisabled) {
      properties.setProperty(option, "0");
    }
    IDatabricksConnectionContext context = DatabricksConnectionContext.parse(JDBC_URL, properties);
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      assertThrows(
          DatabricksSQLException.class,
          () -> client.createSession(new Warehouse("warehouse"), null, null, null));
      assertEquals(1, attempts.get());
    } finally {
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {307, 400, 401, 403, 404, 408, 502, 504})
  void otherCreateSessionStatusesKeepSdkBehavior(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, Integer.MAX_VALUE);
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(JDBC_URL, new Properties());
    try (MockedConstruction<ClientConfigurator> ignored =
        mockConstruction(
            ClientConfigurator.class,
            (configurator, construction) ->
                when(configurator.getDatabricksConfig()).thenReturn(config))) {
      DatabricksSdkClient client = new DatabricksSdkClient(context);
      assertThrows(
          DatabricksSQLException.class,
          () -> client.createSession(new Warehouse("warehouse"), null, null, null));
      assertEquals(1, attempts.get());
    } finally {
      DatabricksClientConfiguratorManager.getInstance().removeInstance(context);
    }
  }

  @ParameterizedTest
  @CsvSource({
    "POST, /api/2.0/sql/warehouses/warehouse/sessions/session/statements",
    "POST, /api/2.0/sql/statements/",
    "POST, /api/2.0/sql/statements/statement/cancel",
    "POST, /api/2.0/other",
    "GET, /api/2.0/sql/statements/statement/status",
    "DELETE, /api/2.0/sql/sessions/session",
    "GET, /api/2.0/sql/warehouses/warehouse/sessions/session/statements/statement"
  })
  void everyOtherRequestKeepsSdkStrategy(String method, String path) {
    for (int status : new int[] {307, 400, 408, 429, 501, 502, 503, 504}) {
      AtomicInteger baselineAttempts = new AtomicInteger();
      AtomicInteger actualAttempts = new AtomicInteger();
      ApiClient baseline =
          new ApiClient.Builder()
              .withDatabricksConfig(config(status, baselineAttempts, Integer.MAX_VALUE, "0"))
              .build();
      ApiClient actual =
          DatabricksSdkClient.buildApiClient(
              config(status, actualAttempts, Integer.MAX_VALUE, "0"), null, true);
      DatabricksException baselineError =
          assertThrows(
              DatabricksException.class,
              () -> baseline.execute(new Request(method, path), Void.class));
      DatabricksException actualError =
          assertThrows(
              DatabricksException.class,
              () -> actual.execute(new Request(method, path), Void.class));
      assertEquals(baselineAttempts.get(), actualAttempts.get(), method + path + " HTTP " + status);
      assertEquals(baselineError.getClass(), actualError.getClass());
      assertEquals(baselineError.getMessage(), actualError.getMessage());
    }
  }

  private DatabricksConfig config(int status, AtomicInteger attempts, int failures) {
    return config(status, attempts, failures, "1");
  }

  private DatabricksConfig config(
      int status, AtomicInteger attempts, int failures, String retryAfter) {
    return new DatabricksConfig()
        .setHost("https://workspace.example.com")
        .setAuthType("pat")
        .setToken("test-token")
        .setHttpClient(
            request -> {
              if (request.getUrl().contains("/.well-known/")) {
                return new Response(request, 200, "OK", Map.of(), "{}");
              }
              int responseStatus = attempts.incrementAndGet() <= failures ? status : 200;
              return new Response(
                  request,
                  responseStatus,
                  "test response",
                  Map.of("Retry-After", List.of(retryAfter)),
                  responseStatus == 200
                      ? "{\"session_id\":\"session\"}"
                      : "{\"error_code\":\"TEMPORARILY_UNAVAILABLE\",\"message\":\"test request_id=retry-request\"}");
            });
  }
}
