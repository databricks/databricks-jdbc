package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class DatabricksSdkClientRetryTest {
  private static final String SESSIONS = "/api/2.0/sql/warehouses/warehouse/sessions";
  private static final String SESSION = SESSIONS + "/session";

  @ParameterizedTest
  @CsvSource({
    "DELETE, ''",
    "GET, /statements/statement",
    "GET, /statements/statement/status",
    "DELETE, /statements/statement",
    "GET, /statements/statement/result/chunks/0"
  })
  void realtimeOperationsRetryHeaderlessGatewayErrors(String method, String suffix)
      throws Exception {
    for (int status : new int[] {408, 502, 503, 504}) {
      AtomicInteger attempts = new AtomicInteger();
      ApiClient client = newClient(status, false, attempts);
      client.execute(new Request(method, SESSION + suffix), Void.class);
      assertEquals(2, attempts.get(), method + suffix + " HTTP " + status);
    }
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 502, 503, 504})
  void disabledRetriesArePreserved(int status) {
    AtomicInteger attempts = new AtomicInteger();
    ApiClient client = newClient(status, true, attempts);
    DatabricksError error =
        assertThrows(
            DatabricksError.class,
            () -> client.execute(new Request(Request.POST, SESSIONS), Void.class));
    assertEquals(status, error.getStatusCode());
    assertEquals(1, attempts.get());
  }

  @ParameterizedTest
  @ValueSource(ints = {408, 502, 504})
  void executeStatementDoesNotRetryAmbiguousFailures(int status) {
    AtomicInteger attempts = new AtomicInteger();
    ApiClient client = newClient(status, false, attempts);
    DatabricksError error =
        assertThrows(
            DatabricksError.class,
            () ->
                client.execute(
                    new Request(Request.POST, SESSION + "/statements", "{}"), Void.class));
    assertEquals(status, error.getStatusCode());
    assertEquals(1, attempts.get());
  }

  private ApiClient newClient(int status, boolean disableRetries, AtomicInteger attempts) {
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://workspace.example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setDisableRetries(disableRetries)
            .setHttpClient(
                request -> {
                  if (request.getUrl().contains("/.well-known/")) {
                    return new Response(request, 200, "OK", Map.of(), "{}");
                  }
                  int responseStatus = attempts.incrementAndGet() == 1 ? status : 200;
                  return new Response(request, responseStatus, "test response", Map.of(), "{}");
                });
    return DatabricksSdkClient.buildApiClient(config, null, true);
  }
}
