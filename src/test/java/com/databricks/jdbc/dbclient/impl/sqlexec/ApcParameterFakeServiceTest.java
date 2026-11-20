package com.databricks.jdbc.dbclient.impl.sqlexec;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.databricks.jdbc.api.impl.*;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.api.internal.IDatabricksSession;
import com.databricks.jdbc.common.AllPurposeCluster;
import com.databricks.jdbc.common.CompressionCodec;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.exception.DatabricksSQLException;
import com.databricks.jdbc.model.telemetry.enums.DatabricksDriverErrorCode;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Response;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import java.net.URI;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.sql.BatchUpdateException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Captures actual HTTP requests after prepared execution passes through the SEA SDK client. */
class ApcParameterFakeServiceTest {
  private static final AllPurposeCluster CLUSTER = new AllPurposeCluster("org", "cluster");
  private static final String INSERT = "INSERT INTO t VALUES (?)";
  private static final String RESPONSE =
      "{\"statement_id\":\"statement-1\",\"status\":{\"state\":\"SUCCEEDED\"},"
          + "\"manifest\":{\"format\":\"JSON_ARRAY\",\"schema\":{\"column_count\":1,"
          + "\"columns\":[{\"name\":\"num_affected_rows\",\"type_name\":\"LONG\",\"type_text\":\"BIGINT\",\"position\":0}]},"
          + "\"total_chunk_count\":1,\"total_row_count\":1},"
          + "\"result\":{\"chunk_index\":0,\"row_offset\":0,\"row_count\":1,"
          + "\"data_array\":[[\"1\"]]}}";

  private WireMockServer server;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void startFakeService() {
    server =
        new WireMockServer(
            com.github.tomakehurst.wiremock.core.WireMockConfiguration.wireMockConfig()
                .dynamicPort());
    server.start();
    server.stubFor(post(urlPathEqualTo(PathConstants.STATEMENT_PATH)).willReturn(okJson(RESPONSE)));
    server.stubFor(
        delete(urlPathMatching(PathConstants.STATEMENT_PATH + ".*")).willReturn(okJson("{}")));
  }

  @AfterEach
  void stopFakeService() {
    server.stop();
  }

  private DatabricksPreparedStatement prepared(Long version, String sql, boolean nativeBatch)
      throws Exception {
    return prepared(version, sql, nativeBatch, false);
  }

  private DatabricksPreparedStatement prepared(
      Long version, String sql, boolean nativeBatch, boolean supportManyParameters)
      throws Exception {
    String jdbcUrl =
        "jdbc:databricks://localhost:443/default;transportMode=http;ssl=0;AuthMech=3;"
            + "httpPath=/sql/1.0/warehouses/warehouse;TelemetryLogLevel=OFF;"
            + "EnableNativeBatching="
            + (nativeBatch ? "1" : "0")
            + ";supportManyParameters="
            + (supportManyParameters ? "1" : "0")
            + ";";
    IDatabricksConnectionContext context =
        DatabricksConnectionContext.parse(jdbcUrl, new Properties());
    java.net.http.HttpClient http = java.net.http.HttpClient.newHttpClient();
    HttpClient transport =
        request -> {
          HttpRequest.Builder wire = HttpRequest.newBuilder(URI.create(request.getUrl()));
          request
              .getHeaders()
              .forEach(
                  (name, value) -> {
                    if (!name.equalsIgnoreCase("host")
                        && !name.equalsIgnoreCase("content-length")) {
                      wire.header(name, value);
                    }
                  });
          String body = request.getBodyString();
          wire.method(
              request.getMethod(),
              body == null
                  ? HttpRequest.BodyPublishers.noBody()
                  : HttpRequest.BodyPublishers.ofString(body));
          try {
            HttpResponse<String> received =
                http.send(wire.build(), HttpResponse.BodyHandlers.ofString());
            return new Response(
                request, received.statusCode(), "OK", received.headers().map(), received.body());
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new java.io.IOException(e);
          }
        };
    ApiClient apiClient =
        DatabricksSdkClient.buildApiClient(
            new DatabricksConfig()
                .setHost("http://localhost:" + server.port())
                .setAuthType("pat")
                .setToken("test-token")
                .setHttpClient(transport),
            null,
            true);
    DatabricksSdkClient client =
        new DatabricksSdkClient(
            context,
            mock(com.databricks.sdk.service.sql.StatementExecutionService.class),
            apiClient);
    IDatabricksSession session = mock(IDatabricksSession.class);
    DatabricksConnection connection = mock(DatabricksConnection.class);
    ImmutableSessionInfo info =
        ImmutableSessionInfo.builder()
            .sessionId("session")
            .computeResource(CLUSTER)
            .clientType(DatabricksClientType.SEA)
            .negotiatedProtocolVersion(version)
            .liveProtocolVersion(true)
            .build();
    when(connection.getConnectionContext()).thenReturn(context);
    when(connection.getSession()).thenReturn(session);
    when(session.getConnectionContext()).thenReturn(context);
    when(session.getDatabricksClient()).thenReturn(client);
    when(session.getSessionInfo()).thenReturn(info);
    when(session.getComputeResource()).thenReturn(CLUSTER);
    when(session.getSessionId()).thenReturn("session");
    when(session.getCompressionCodec()).thenReturn(CompressionCodec.NONE);
    return new DatabricksPreparedStatement(connection, sql);
  }

  private List<JsonNode> executeBodies() throws Exception {
    java.util.ArrayList<JsonNode> bodies = new java.util.ArrayList<>();
    for (com.github.tomakehurst.wiremock.verification.LoggedRequest request :
        server.findAll(postRequestedFor(urlPathEqualTo(PathConstants.STATEMENT_PATH)))) {
      bodies.add(mapper.readTree(request.getBodyAsString()));
    }
    return bodies;
  }

  private static String setValue(JsonNode body, int set, String field) {
    return body.path("parameter_sets").get(set).path("parameters").get(0).path(field).asText();
  }

  private static final long[] NO_INFO_PAIR = {Statement.SUCCESS_NO_INFO, Statement.SUCCESS_NO_INFO};

  private void verifyOperationClosed(String statementId) {
    server.verify(
        1, deleteRequestedFor(urlPathEqualTo(PathConstants.STATEMENT_PATH + statementId)));
  }

  @Test
  void nativeBinaryValuesArePreservedAndCopiedFromCallerArray() throws Exception {
    DatabricksPreparedStatement single = prepared(42248L, INSERT, false, true);
    byte[] bytes = new byte[] {0x00, (byte) 0xFF, 0x41};
    single.setBytes(1, bytes);
    bytes[0] = 0x7F;
    single.executeUpdate();

    JsonNode singleBody = executeBodies().get(0);
    assertEquals("INSERT INTO t VALUES (unhex(?))", singleBody.path("statement").asText());
    assertEquals("STRING", singleBody.path("parameters").get(0).path("type").asText());
    assertEquals("00FF41", singleBody.path("parameters").get(0).path("value").asText());

    DatabricksPreparedStatement batch = prepared(42250L, INSERT, true);
    batch.setObject(1, new byte[] {(byte) 0x80, 0x00}, Types.BINARY);
    batch.addBatch();
    batch.setBytes(1, new byte[] {(byte) 0xFF, 0x7F});
    batch.addBatch();
    assertArrayEquals(NO_INFO_PAIR, batch.executeLargeBatch());

    JsonNode batchBody = executeBodies().get(1);
    assertEquals("INSERT INTO t VALUES (unhex(?))", batchBody.path("statement").asText());
    assertFalse(batchBody.has("parameters"));
    assertEquals("8000", setValue(batchBody, 0, "value"));
    assertEquals("FF7F", setValue(batchBody, 1, "value"));
  }

  @Test
  void mixedBinaryAndNumericSetsUseOneNativeBatch() throws Exception {
    DatabricksPreparedStatement prepared = prepared(42250L, INSERT, true);
    prepared.setBytes(1, new byte[] {(byte) 0xFF});
    prepared.addBatch();
    prepared.setInt(1, 7);
    prepared.addBatch();

    assertArrayEquals(NO_INFO_PAIR, prepared.executeLargeBatch());

    List<JsonNode> bodies = executeBodies();
    assertEquals(1, bodies.size());
    JsonNode body = bodies.get(0);
    assertEquals(INSERT, body.path("statement").asText());
    assertFalse(body.has("parameters"));
    assertEquals("BINARY", setValue(body, 0, "type"));
    assertEquals("/w==", setValue(body, 0, "value"));
    assertEquals("INT", setValue(body, 1, "type"));
    assertEquals("7", setValue(body, 1, "value"));
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {42247L})
  void unboundOrSparseFallbackBindingsFailBeforeAnyRequest(Long version) throws Exception {
    assertThrows(
        com.databricks.jdbc.exception.DatabricksValidationException.class,
        prepared(version, "SELECT ?", false)::executeQuery);

    DatabricksPreparedStatement sparse = prepared(version, INSERT, false);
    sparse.setInt(2, 7);
    assertThrows(
        com.databricks.jdbc.exception.DatabricksValidationException.class, sparse::executeUpdate);
    sparse.addBatch();
    assertThrows(BatchUpdateException.class, sparse::executeLargeBatch);
    assertTrue(executeBodies().isEmpty());
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {42247L, 42248L})
  void preparedBatchCapturesOneEscapedRequestPerSet(Long version) throws Exception {
    server.stubFor(
        post(urlPathEqualTo(PathConstants.STATEMENT_PATH))
            .withRequestBody(containing("marker-b"))
            .atPriority(1)
            .willReturn(okJson(RESPONSE.replace("statement-1", "statement-2"))));
    DatabricksPreparedStatement prepared = prepared(version, INSERT, false);
    for (String value : List.of("a' ; --", "marker-b\\c")) {
      prepared.setString(1, value);
      prepared.addBatch();
    }
    assertArrayEquals(new long[] {1, 1}, prepared.executeLargeBatch());
    List<JsonNode> bodies = executeBodies();
    assertEquals(2, bodies.size());
    verifyOperationClosed("statement-1");
    verifyOperationClosed("statement-2");
    for (JsonNode body : bodies) {
      assertFalse(body.has("parameter_sets"));
    }
    if (version != null && version >= 42248L) {
      assertEquals(INSERT, bodies.get(0).path("statement").asText());
      assertEquals("a' ; --", bodies.get(0).path("parameters").get(0).path("value").asText());
      assertEquals("marker-b\\c", bodies.get(1).path("parameters").get(0).path("value").asText());
    } else {
      assertEquals("INSERT INTO t VALUES ('a'' ; --')", bodies.get(0).path("statement").asText());
      assertEquals(
          "INSERT INTO t VALUES ('marker-b\\\\c')", bodies.get(1).path("statement").asText());
      assertFalse(bodies.get(0).has("parameters"));
      assertFalse(bodies.get(1).has("parameters"));
    }
  }

  @Test
  void nativeBatchClosesEveryOperationAndOmitsTopLevelParameters() throws Exception {
    server.stubFor(
        post(urlPathEqualTo(PathConstants.STATEMENT_PATH))
            .inScenario("native batch")
            .whenScenarioStateIs(Scenario.STARTED)
            .willReturn(okJson(RESPONSE))
            .willSetStateTo("second"));
    server.stubFor(
        post(urlPathEqualTo(PathConstants.STATEMENT_PATH))
            .inScenario("native batch")
            .whenScenarioStateIs("second")
            .willReturn(okJson(RESPONSE.replace("statement-1", "statement-2"))));
    DatabricksPreparedStatement prepared = prepared(42250L, INSERT, true);
    for (int batch = 0; batch < 2; batch++) {
      prepared.setInt(1, batch * 2 + 1);
      prepared.addBatch();
      prepared.setInt(1, batch * 2 + 2);
      prepared.addBatch();
      assertArrayEquals(NO_INFO_PAIR, prepared.executeLargeBatch());
    }
    List<JsonNode> bodies = executeBodies();
    assertEquals(2, bodies.size());
    for (JsonNode body : bodies) {
      assertFalse(body.has("parameters"));
      assertEquals(2, body.path("parameter_sets").size());
    }
    verifyOperationClosed("statement-1");
    verifyOperationClosed("statement-2");
  }

  @Test
  void complexBindingAfterScalarSetSendsNoRequest() throws Exception {
    DatabricksPreparedStatement prepared = prepared(42250L, INSERT, true);
    prepared.setInt(1, 1);
    prepared.addBatch();
    prepared.setObject(1, "struct", Types.STRUCT);
    prepared.addBatch();
    BatchUpdateException error =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertEquals(DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.name(), error.getSQLState());
    assertEquals(
        DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.getCode(), error.getErrorCode());
    assertTrue(error.getMessage().contains("complex logical type STRUCT"));
    assertArrayEquals(
        new long[] {Statement.EXECUTE_FAILED, Statement.EXECUTE_FAILED},
        error.getLargeUpdateCounts());

    DatabricksPreparedStatement single = prepared(42248L, "SELECT ?", false);
    single.setObject(1, new Object[] {1}, Types.ARRAY);
    DatabricksSQLException singleError =
        assertThrows(DatabricksSQLException.class, single::executeQuery);
    assertEquals(
        DatabricksDriverErrorCode.UNSUPPORTED_APC_PARAMETER.name(), singleError.getSQLState());
    assertTrue(executeBodies().isEmpty());
  }

  @Test
  void interpolatedBatchFailureDoesNotEchoBoundValueInException() throws Exception {
    String secret = "sensitive-value-42";
    stubFailure("Failed near " + secret, "42000");
    DatabricksPreparedStatement prepared = prepared(null, INSERT, false);
    prepared.setString(1, secret);
    prepared.addBatch();

    BatchUpdateException error =
        assertThrows(BatchUpdateException.class, prepared::executeLargeBatch);
    assertFalse(error.getMessage().contains(secret));
    assertEquals(1, executeBodies().size());
    assertTrue(executeBodies().get(0).path("statement").asText().contains(secret));
  }

  private void stubFailure(String message, String sqlState) {
    String response =
        "{\"statement_id\":\"failed\",\"status\":{\"state\":\"FAILED\","
            + "\"sql_state\":\""
            + sqlState
            + "\",\"error\":{\"message\":\""
            + message
            + "\",\"error_code\":\"BAD_SQL\"}}}";
    server.stubFor(
        post(urlPathEqualTo(PathConstants.STATEMENT_PATH))
            .atPriority(1)
            .willReturn(okJson(response)));
  }
}
