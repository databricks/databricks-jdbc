package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.impl.ImmutableSessionInfo;
import com.databricks.jdbc.api.internal.IDatabricksStatementInternal;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.StatementType;
import com.databricks.jdbc.exception.DatabricksTimeoutException;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.StatementExecutionService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ApcRunningPollFakeServiceTest {
  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void timeoutCoversSlowInitialRequestAndSlowRunningPoll(boolean slowInitialRequest)
      throws Exception {
    List<String> cancellationBodies = new ArrayList<>();
    AtomicInteger polls = new AtomicInteger();
    HttpClient transport =
        request -> {
          String path = request.getUri().getPath();
          if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          if (path.endsWith("/statements/statement/cancel")) {
            cancellationBodies.add(request.getBodyString());
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          if (Request.DELETE.equals(request.getMethod())) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          if (slowInitialRequest && Request.POST.equals(request.getMethod())) {
            pausePastDeadline();
          }
          if (!slowInitialRequest && Request.GET.equals(request.getMethod())) {
            polls.incrementAndGet();
            pausePastDeadline();
          }
          // The deadline is only enforced while the statement is PENDING/RUNNING; late SUCCEEDED
          // is covered by DatabricksSdkClientTest#testApcSucceededPollAfterDeadlineIsNotTimedOut.
          String state = Request.GET.equals(request.getMethod()) ? "RUNNING" : "PENDING";
          return new Response(
              request,
              200,
              "OK",
              Map.of(),
              "{\"statement_id\":\"statement\",\"status\":{\"state\":\"" + state + "\"}}");
        };
    ApiClient api =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(transport),
            null,
            true);
    DatabricksConnectionContext context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(
                "jdbc:databricks://example.com:443/default;AuthMech=3;"
                    + "httpPath=/sql/protocolv1/o/private-org/private-cluster;"
                    + "UseThriftClient=0;TelemetryLogLevel=OFF;PollInterval=1",
                new Properties());
    AllPurposeCluster cluster = new AllPurposeCluster("private-org", "private-cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("private-session");
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .sessionId("private-session")
                .computeResource(cluster)
                .clientType(DatabricksClientType.SEA)
                .build());
    IDatabricksStatementInternal parent = mock(IDatabricksStatementInternal.class);
    java.sql.Statement statement = mock(java.sql.Statement.class);
    when(statement.getQueryTimeout()).thenReturn(1);
    when(parent.getStatement()).thenReturn(statement);
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, mock(StatementExecutionService.class), api);

    DatabricksTimeoutException failure =
        assertThrows(
            DatabricksTimeoutException.class,
            () ->
                client.executeStatement(
                    "SELECT 1", cluster, Map.of(), StatementType.QUERY, session, parent, null));

    assertEquals("STATEMENT_EXECUTION_TIMEOUT", failure.getSQLState());
    assertEquals(slowInitialRequest ? 0 : 1, polls.get());
    assertEquals(1, cancellationBodies.size());
    assertEquals(
        "private-cluster",
        new ObjectMapper()
            .readTree(cancellationBodies.get(0))
            .path("backend_compute")
            .path("all_purpose_cluster_compute")
            .path("cluster_id")
            .asText());
    assertFalse(failure.getMessage().contains("private-session"));
  }

  private static void pausePastDeadline() {
    try {
      Thread.sleep(1100);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new AssertionError(e);
    }
  }

  @Test
  void threeRunningPollsCarryNoResultPayload() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    List<Request> sent = new ArrayList<>();
    List<String> runningBodies = new ArrayList<>();
    String statusOnly = "{\"statement_id\":\"statement\",\"status\":{\"state\":\"RUNNING\"}}";
    HttpClient transport =
        request -> {
          if (request.getUri().getPath().equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          sent.add(request);
          String body;
          if (PathConstants.STATEMENT_PATH.equals(request.getUri().getPath())
              && Request.POST.equals(request.getMethod())) {
            body = "{\"statement_id\":\"statement\",\"status\":{\"state\":\"PENDING\"}}";
          } else if (request.getUri().getPath().endsWith("/statements/statement")
              && Request.GET.equals(request.getMethod())) {
            if (polls.getAndIncrement() < 3) {
              body = statusOnly;
              runningBodies.add(body);
            } else {
              body = "{\"statement_id\":\"statement\",\"status\":{\"state\":\"SUCCEEDED\"}}";
            }
          } else {
            throw new AssertionError("Unexpected APC request: " + request.getUri().getPath());
          }
          return new Response(request, 200, "OK", Map.of(), body);
        };
    ApiClient api =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("https://example.com")
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(transport),
            null,
            true);
    DatabricksConnectionContext context =
        (DatabricksConnectionContext)
            DatabricksConnectionContext.parse(
                "jdbc:databricks://example.com:443/default;AuthMech=3;"
                    + "httpPath=/sql/protocolv1/o/private-org/private-cluster;"
                    + "UseThriftClient=0;TelemetryLogLevel=OFF;PollInterval=1",
                new Properties());
    AllPurposeCluster cluster = new AllPurposeCluster("private-org", "private-cluster");
    DatabricksSession session = mock(DatabricksSession.class);
    when(session.getSessionId()).thenReturn("session");
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    when(session.getSessionInfo())
        .thenReturn(
            ImmutableSessionInfo.builder()
                .sessionId("session")
                .computeResource(cluster)
                .clientType(DatabricksClientType.SEA)
                .build());
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, mock(StatementExecutionService.class), api);

    client.executeStatement(
        "SELECT 1", cluster, Map.of(), StatementType.QUERY, session, null, null);

    assertEquals(4, polls.get());
    assertEquals(3, runningBodies.size());
    ObjectMapper mapper = new ObjectMapper();
    for (String body : runningBodies) {
      assertEquals(2, mapper.readTree(body).size());
      assertFalse(body.contains("attachment"));
      assertFalse(body.contains("arrow_batches"));
      assertFalse(body.contains("columnar_data"));
      assertFalse(body.contains("external_links"));
    }
    for (Request request : sent.subList(1, sent.size())) {
      assertEquals(Request.GET, request.getMethod());
      assertNull(request.getBodyString());
      assertFalse(request.getUrl().contains("/status"));
      assertEquals(
          "backend_compute.all_purpose_cluster_compute.cluster_id=private-cluster",
          request.getUri().getRawQuery());
    }
  }
}
