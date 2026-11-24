package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientConfiguratorManager;
import com.databricks.jdbc.common.Warehouse;
import com.databricks.jdbc.dbclient.impl.common.ClientConfigurator;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;

class DatabricksSdkClientRetryTest {
  private static final String JDBC_URL =
      "jdbc:databricks://workspace.example.com/default;AuthMech=3;httpPath=/sql/1.0/warehouses/warehouse;UseThriftClient=0";

  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void zeroBudgetDoesNotRetryCreateSession(int status) throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    DatabricksConfig config = config(status, attempts, Integer.MAX_VALUE);
    Properties properties = new Properties();
    properties.setProperty("RateLimitRetryTimeout", "0");
    properties.setProperty("TemporarilyUnavailableRetryTimeout", "0");
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
      assertEquals(status == 429 ? "RATE_LIMIT_EXCEEDED" : "CONNECTION_ERROR", error.getSQLState());
      assertEquals(0, error.getErrorCode());
      DatabricksError cause = assertInstanceOf(DatabricksError.class, error.getCause());
      assertEquals(status, cause.getStatusCode());
      assertEquals("TEMPORARILY_UNAVAILABLE", cause.getErrorCode());
      assertTrue(cause.getMessage().contains("test request_id=retry-request"));
      assertEquals(1, attempts.get());
    } finally {
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
