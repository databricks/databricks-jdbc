package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.dbclient.MetadataAsyncHandle;
import com.databricks.jdbc.dbclient.MetadataAsyncRequest;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.StatementExecutionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

/** Exercises the async metadata path through a session and a fake HTTP service. */
class DatabricksMetadataHttpFakeTest {
  @Test
  void notFoundServerMarkerReturnsEmptySyncAndAsyncMetadataWithNonstandardSqlState()
      throws Exception {
    for (String sqlState : new String[] {"42P01", ""}) {
      AtomicInteger submissions = new AtomicInteger();
      AtomicInteger polls = new AtomicInteger();
      List<Request> sent = new ArrayList<>();
      String failed =
          "{\"statement_id\":\"statement\",\"status\":{\"state\":\"FAILED\","
              + (sqlState.isEmpty() ? "" : "\"sql_state\":\"" + sqlState + "\",")
              + "\"error\":{\"message\":\"[TABLE_OR_VIEW_NOT_FOUND] secret table name\"}}}";
      HttpClient transport =
          request -> {
            String path = request.getUri().getPath();
            if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
              return new Response(request, 200, "OK", Map.of(), "{}");
            }
            sent.add(request);
            String body;
            if (path.equals(PathConstants.SESSION_PATH)) {
              body = "{\"session_id\":\"session\",\"negotiated_protocol_version\":42248}";
            } else if (path.equals(PathConstants.STATEMENT_PATH)
                && Request.POST.equals(request.getMethod())) {
              body =
                  submissions.incrementAndGet() == 1
                      ? failed
                      : "{\"statement_id\":\"statement\",\"status\":{\"state\":\"PENDING\"}}";
            } else if (path.equals(String.format(PathConstants.STATEMENT_PATH_WITH_ID, "statement"))
                && Request.GET.equals(request.getMethod())) {
              polls.incrementAndGet();
              body = failed;
            } else if (Request.DELETE.equals(request.getMethod())) {
              body = "{}";
            } else {
              throw new AssertionError("Unexpected fake service request: " + path);
            }
            return new Response(request, 200, "OK", Map.of(), body);
          };
      DatabricksConfig config =
          new DatabricksConfig()
              .setHost("https://example.com")
              .setAuthType("pat")
              .setToken("test-token")
              .setHttpClient(transport);
      ApiClient api = DatabricksSdkClient.buildApiClient(config, null, true);
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(
              TestConstants.VALID_CLUSTER_URL
                  + ";UseThriftClient=0;EnableMultipleCatalogSupport=1;TelemetryLogLevel=OFF;",
              new Properties());
      context.setClientType(DatabricksClientType.SEA);
      DatabricksSdkClient client =
          new DatabricksSdkClient(context, mock(StatementExecutionService.class), api);
      DatabricksSession session = new DatabricksSession(context, client);
      session.open();

      try (DatabricksResultSet sync =
          session
              .getDatabricksMetadataClient()
              .listTables(session, "main", "s%", "t%", new String[] {"TABLE"})) {
        assertTrue(sync.getMetaData().getColumnCount() > 0);
        assertFalse(sync.next());
      }
      MetadataAsyncHandle handle =
          session
              .getDatabricksMetadataClient()
              .executeMetadataAsync(
                  MetadataAsyncRequest.forTables("main", "s%", "t%", new String[] {"TABLE"}),
                  session);
      try (DatabricksResultSet async =
          session.getDatabricksMetadataClient().getMetadataExecutionResult(handle, session)) {
        assertTrue(async.getMetaData().getColumnCount() > 0);
        assertFalse(async.next());
        assertEquals(0, async.getUpdateCount());
      }
      assertEquals(2, submissions.get());
      assertEquals(1, polls.get());
      assertEquals(
          2,
          sent.stream()
              .filter(
                  request ->
                      Request.POST.equals(request.getMethod())
                          && "GetTables"
                              .equals(
                                  request.getHeaders().get("X-Databricks-Metadata-Operation-Type")))
              .count());
    }
  }

  @Test
  void asyncCatalogsPollAndDecodeColumnarNativeMetadata() throws Exception {
    List<Request> sent = new ArrayList<>();
    AtomicInteger polls = new AtomicInteger();
    AtomicReference<String> submitBody = new AtomicReference<>();
    String completed =
        "{\"statement_id\":\"statement\",\"status\":{\"state\":\"SUCCEEDED\"},"
            + "\"manifest\":{\"format\":\"COLUMN_BASED_SET\","
            + "\"is_native_metadata_result\":true,\"total_row_count\":1,"
            + "\"total_chunk_count\":1,\"schema\":{\"column_count\":1,"
            + "\"columns\":[{\"name\":\"TABLE_CAT\",\"position\":0,"
            + "\"type_name\":\"STRING\",\"type_text\":\"STRING\"}]}},"
            + "\"result\":{\"row_count\":1,\"columnar_data\":{\"columns\":["
            + "{\"string_values\":{\"values\":[\"main\"]}}]}}}";
    HttpClient transport =
        request -> {
          String path = request.getUri().getPath();
          if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
            return new Response(request, 200, "OK", Map.of(), "{}");
          }
          sent.add(request);
          String body;
          if (path.equals(PathConstants.SESSION_PATH)) {
            body = "{\"session_id\":\"session\",\"negotiated_protocol_version\":42248}";
          } else if (path.equals(PathConstants.STATEMENT_PATH)
              && Request.POST.equals(request.getMethod())) {
            submitBody.set(request.getBodyString());
            body = "{\"statement_id\":\"statement\",\"status\":{\"state\":\"PENDING\"}}";
          } else if (path.equals(String.format(PathConstants.STATEMENT_PATH_WITH_ID, "statement"))
              && Request.GET.equals(request.getMethod())) {
            body =
                polls.getAndIncrement() == 0
                    ? "{\"statement_id\":\"statement\",\"status\":{\"state\":\"RUNNING\"}}"
                    : completed;
          } else if (Request.DELETE.equals(request.getMethod())) {
            body = "{}";
          } else {
            throw new AssertionError("Unexpected fake service request: " + path);
          }
          return new Response(request, 200, "OK", Map.of(), body);
        };
    DatabricksConfig config =
        new DatabricksConfig()
            .setHost("https://example.com")
            .setAuthType("pat")
            .setToken("test-token")
            .setHttpClient(transport);
    ApiClient api = DatabricksSdkClient.buildApiClient(config, null, true);
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(
            TestConstants.VALID_CLUSTER_URL
                + ";UseThriftClient=0;EnableMultipleCatalogSupport=1;"
                + "EnableSQLExecHybridResults=0;TelemetryLogLevel=OFF;",
            new Properties());
    context.setClientType(DatabricksClientType.SEA);
    DatabricksSdkClient client =
        new DatabricksSdkClient(context, mock(StatementExecutionService.class), api);
    DatabricksSession session = new DatabricksSession(context, client);
    session.open();

    MetadataAsyncHandle handle =
        session
            .getDatabricksMetadataClient()
            .executeMetadataAsync(MetadataAsyncRequest.forCatalogs(), session);
    DatabricksResultSet result =
        session.getDatabricksMetadataClient().getMetadataExecutionResult(handle, session);
    assertEquals("statement", handle.getStatementId().toSQLExecStatementId());
    assertEquals(2, polls.get());
    assertTrue(result.next());
    assertEquals("main", result.getString("TABLE_CAT"));
    assertFalse(result.next());
    assertEquals(0, result.getUpdateCount());

    Request submitted =
        sent.stream()
            .filter(
                request ->
                    Request.POST.equals(request.getMethod())
                        && request.getUri().getPath().equals(PathConstants.STATEMENT_PATH))
            .findFirst()
            .orElseThrow();
    JsonNode body = new ObjectMapper().readTree(submitBody.get());
    assertEquals("SHOW CATALOGS", body.path("statement").asText());
    assertEquals("ARROW_STREAM", body.path("format").asText());
    assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
    assertEquals("GetCatalogs", submitted.getHeaders().get("X-Databricks-Metadata-Operation-Type"));
    assertFalse(submitted.getHeaders().containsKey("x-databricks-sea-can-run-fully-sync"));
  }
}
