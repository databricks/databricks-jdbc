package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.DatabricksError;
import com.databricks.sdk.core.DatabricksException;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.NoRouteToHostException;
import java.net.PortUnreachableException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class RedactingRequestTest {
  @ParameterizedTest
  @ValueSource(ints = {429, 503})
  void sdkRetryThrowableCannotLogEchoedApcIdentifiers(int status) throws Exception {
    String secret = "private-org-private-cluster-binding-SQL";
    AtomicInteger attempts = new AtomicInteger();
    ApiClient client =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(
                    request -> {
                      if (request.getUri().getPath().startsWith("/oidc/")) {
                        return new Response(request, 200, "OK", Map.of(), "{}");
                      }
                      attempts.incrementAndGet();
                      return new Response(
                          request,
                          status,
                          "server echoed " + secret,
                          Map.of("retry-after", List.of("0")),
                          "{\"error_code\":\"RATE_LIMITED\",\"message\":\""
                              + secret
                              + "\",\"details\":{\"sql\":\""
                              + secret
                              + "\"}}");
                    }),
            null,
            true);
    Logger logger = Logger.getLogger(ApiClient.class.getName());
    Level previousLevel = logger.getLevel();
    StringWriter logged = new StringWriter();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logged.append(record.getMessage()).append('\n');
            if (record.getThrown() != null) {
              record.getThrown().printStackTrace(new PrintWriter(logged));
            }
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.FINE);
    logger.addHandler(handler);
    try {
      DatabricksException failure =
          assertThrows(
              DatabricksException.class,
              () ->
                  client.execute(
                      new RedactingRequest(
                          Request.POST,
                          "/api/2.0/sql/statements/",
                          "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
                              + secret
                              + "\"}},\"statement\":\"SELECT '"
                              + secret
                              + "'\"}"),
                      Void.class));
      assertTrue(attempts.get() > 1);
      assertTrue(failure.getCause() instanceof DatabricksError);
      assertEquals("RATE_LIMITED", ((DatabricksError) failure.getCause()).getErrorCode());
      assertFalse(failure.getMessage().contains(secret));
      assertFalse(failure.getCause().getMessage().contains(secret));
      assertFalse(logged.toString().contains(secret));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
  }

  private static final String TRANSPORT_SECRET = "private-org-private-cluster-host";

  static Stream<IOException> connectPhaseFailures() {
    return Stream.of(
        new ConnectException(TRANSPORT_SECRET),
        new UnknownHostException(TRANSPORT_SECRET),
        new NoRouteToHostException(TRANSPORT_SECRET),
        new PortUnreachableException(TRANSPORT_SECRET),
        new IOException("wrapped " + TRANSPORT_SECRET, new ConnectException(TRANSPORT_SECRET)));
  }

  private static ApiClient apcApiClientFailingFirstStatementPost(
      IOException failure, AtomicInteger attempts) {
    return DatabricksSdkClient.buildApiClient(
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(
                request -> {
                  if (request.getUri().getPath().startsWith("/oidc/")) {
                    return new Response(request, 200, "OK", Map.of(), "{}");
                  }
                  if (attempts.incrementAndGet() == 1) {
                    throw failure;
                  }
                  return new Response(request, 200, "OK", Map.of(), "{}");
                }),
        null,
        true);
  }

  private static RedactingRequest apcExecuteStatementRequest() {
    return new RedactingRequest(
        Request.POST,
        "/api/2.0/sql/statements/",
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\"c\"}},"
            + "\"statement\":\"SELECT 1\"}");
  }

  // ExecuteStatement is a non-idempotent POST: the SDK retries it only when the IOException (or a
  // nested cause) is a connect-phase class, so sanitizing must not collapse them to IOException.
  @ParameterizedTest
  @MethodSource("connectPhaseFailures")
  void sanitizedConnectPhaseFailureStillRetriesApcExecuteStatement(IOException failure)
      throws Exception {
    AtomicInteger attempts = new AtomicInteger();
    ApiClient client = apcApiClientFailingFirstStatementPost(failure, attempts);

    client.execute(apcExecuteStatementRequest(), Void.class);

    assertEquals(2, attempts.get());
  }

  @Test
  void sanitizedNonConnectFailureIsNotRetriedForApcExecuteStatement() {
    AtomicInteger attempts = new AtomicInteger();
    ApiClient client =
        apcApiClientFailingFirstStatementPost(
            new SocketTimeoutException(TRANSPORT_SECRET), attempts);

    DatabricksError failure =
        assertThrows(
            DatabricksError.class, () -> client.execute(apcExecuteStatementRequest(), Void.class));

    assertEquals(1, attempts.get());
    assertFalse(String.valueOf(failure.getMessage()).contains(TRANSPORT_SECRET));
    assertTrue(failure.getCause() instanceof SocketTimeoutException);
  }

  @ParameterizedTest
  @MethodSource("connectPhaseFailures")
  void sanitizedTransportFailureKeepsRetryClassButDropsMessageAndCause(IOException original) {
    IOException sanitized = DatabricksSdkClient.sanitizeApcTransportFailure(original);

    assertFalse(sanitized.getMessage().contains(TRANSPORT_SECRET));
    assertNull(sanitized.getCause());
    Class<?> expected =
        original.getCause() != null ? original.getCause().getClass() : original.getClass();
    assertEquals(expected, sanitized.getClass());
  }

  @Test
  void sdkDebugLogHidesBoundValueEchoedByHttpError() throws Exception {
    String secret = "response-secret-918273";
    String body =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\"cluster\"}},"
            + "\"statement\":\"SELECT ?\",\"parameters\":[{\"value\":\""
            + secret
            + "\"}]}";
    List<String> wireBodies = new ArrayList<>();
    ApiClient client =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(
                    request -> {
                      if (!request.getUri().getPath().equals("/api/2.0/sql/statements/")) {
                        return new Response(request, 200, "OK", Map.of(), "{}");
                      }
                      wireBodies.add(request.getBodyString());
                      return new Response(
                          request,
                          400,
                          "Bad Request",
                          Map.of(),
                          "{\"error_code\":\"BAD_REQUEST\",\"message\":\"" + secret + "\"}");
                    }),
            null,
            true);
    Logger logger = Logger.getLogger(ApiClient.class.getName());
    Level previousLevel = logger.getLevel();
    StringBuilder logged = new StringBuilder();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logged.append(record.getMessage()).append('\n');
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.FINE);
    logger.addHandler(handler);
    try {
      assertThrows(
          DatabricksError.class,
          () ->
              client.execute(
                  new RedactingRequest(Request.POST, "/api/2.0/sql/statements/", body),
                  Void.class));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }

    assertTrue(wireBodies.contains(body));
    assertTrue(logged.toString().contains("BAD_REQUEST"));
    assertFalse(logged.toString().contains(secret));
  }

  @Test
  void sdkDebugLogHidesApcTargetWhileTransportReceivesIt() throws Exception {
    String cluster = "secret-cluster-918273";
    String statementId = "secret-statement-918273";
    String sessionId = "secret-session-918273";
    String body =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}},\"statement\":\"SELECT ?\",\"parameters\":[{\"value\":\"native-secret\"}],"
            + "\"parameter_sets\":[{\"parameters\":[{\"value\":\"batch-secret\"}]}]}";
    String echoed = "response-secret-918273";
    String createBody =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}},\"catalog\":\"secret-catalog\",\"schema\":\"secret-schema\","
            + "\"session_configs\":{\"tag\":\"secret-config\"}}";
    String interpolatedBody =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}},\"statement\":\"SELECT 'interpolated-secret'\"}";
    String cancelBody =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}},\"statementId\":\""
            + statementId
            + "\"}";
    String deleteBody =
        "{\"backend_compute\":{\"all_purpose_cluster_compute\":{\"cluster_id\":\""
            + cluster
            + "\"}},\"session_id\":\""
            + sessionId
            + "\"}";
    List<String> wireBodies = new ArrayList<>();
    List<String> wireUrls = new ArrayList<>();
    ApiClient client =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(
                    request -> {
                      if (request.getUri().getPath().startsWith("/api/2.0/sql/")) {
                        wireBodies.add(request.getBodyString());
                        wireUrls.add(request.getUri().toString());
                      }
                      if (request.getUri().getPath().endsWith("/echo-error")) {
                        return new Response(
                            request,
                            400,
                            "Bad Request",
                            Map.of(),
                            "{\"message\":\"" + echoed + "\"}");
                      }
                      if (request.getUri().getPath().startsWith("/api/2.0/sql/sessions/")
                          && Request.DELETE.equals(request.getMethod())) {
                        return new Response(
                            request,
                            403,
                            "Forbidden",
                            Map.of(),
                            "{\"error_code\":\"PERMISSION_DENIED\",\"message\":\""
                                + sessionId
                                + " "
                                + cluster
                                + "\"}");
                      }
                      return new Response(request, 204, "No Content", Map.of());
                    }),
            null,
            true);
    Logger logger = Logger.getLogger(ApiClient.class.getName());
    Level previousLevel = logger.getLevel();
    StringBuilder logged = new StringBuilder();
    Handler handler =
        new Handler() {
          @Override
          public void publish(LogRecord record) {
            logged.append(record.getMessage()).append('\n');
          }

          @Override
          public void flush() {}

          @Override
          public void close() {}
        };
    handler.setLevel(Level.ALL);
    logger.setLevel(Level.FINE);
    logger.addHandler(handler);
    try {
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/sessions/", createBody), Void.class);
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/statements/", body), Void.class);
      client.execute(
          new RedactingRequest(Request.POST, "/api/2.0/sql/statements/", interpolatedBody),
          Void.class);
      client.execute(
          new RedactingRequest(
              Request.POST, "/api/2.0/sql/statements/" + statementId + "/cancel", cancelBody),
          Void.class);
      assertThrows(
          DatabricksError.class,
          () ->
              client.execute(
                  new RedactingRequest(
                      Request.DELETE, "/api/2.0/sql/sessions/" + sessionId, deleteBody),
                  Void.class));
      client.execute(
          new RedactingRequest(
              Request.GET,
              "/api/2.0/sql/statements/"
                  + statementId
                  + "?backend_compute.all_purpose_cluster_compute.cluster_id="
                  + cluster,
              null),
          Void.class);
      client.execute(
          new RedactingRequest(
              Request.GET,
              "/api/2.0/sql/statements/"
                  + statementId
                  + "/result/chunks/2?row_offset=3&"
                  + "backend_compute.all_purpose_cluster_compute.cluster_id="
                  + cluster,
              null),
          Void.class);
      client.execute(
          new RedactingRequest(
              Request.DELETE,
              "/api/2.0/sql/statements/"
                  + statementId
                  + "?backend_compute.all_purpose_cluster_compute.cluster_id="
                  + cluster,
              null),
          Void.class);
      assertThrows(
          DatabricksError.class,
          () ->
              client.execute(
                  new RedactingRequest(Request.POST, "/api/2.0/sql/statements/echo-error", body),
                  Void.class));
    } finally {
      logger.removeHandler(handler);
      logger.setLevel(previousLevel);
    }
    assertEquals(9, wireBodies.size());
    assertEquals(createBody, wireBodies.get(0));
    assertEquals(body, wireBodies.get(1));
    assertEquals(deleteBody, wireBodies.get(4));
    assertEquals(body, wireBodies.get(8));
    assertEquals(interpolatedBody, wireBodies.get(2));
    assertEquals(cancelBody, wireBodies.get(3));
    assertTrue(wireUrls.get(5).contains(cluster));
    assertTrue(wireUrls.get(5).contains(statementId));
    assertTrue(wireUrls.get(6).contains(statementId));
    assertTrue(wireUrls.get(7).contains(statementId));
    assertTrue(logged.toString().contains("[REDACTED]"));
    assertFalse(logged.toString().contains(cluster));
    assertTrue(logged.toString().contains(statementId));
    assertFalse(logged.toString().contains(sessionId));
    assertFalse(logged.toString().contains("native-secret"));
    assertFalse(logged.toString().contains("batch-secret"));
    assertFalse(logged.toString().contains("interpolated-secret"));
    assertFalse(logged.toString().contains(echoed));
    assertFalse(logged.toString().contains("secret-catalog"));
    assertFalse(logged.toString().contains("secret-schema"));
    assertFalse(logged.toString().contains("secret-config"));
  }
}
