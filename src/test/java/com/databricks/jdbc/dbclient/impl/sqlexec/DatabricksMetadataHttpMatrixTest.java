package com.databricks.jdbc.dbclient.impl.sqlexec;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import com.databricks.jdbc.TestConstants;
import com.databricks.jdbc.api.impl.DatabricksConnectionContext;
import com.databricks.jdbc.api.impl.DatabricksResultSet;
import com.databricks.jdbc.api.impl.DatabricksSession;
import com.databricks.jdbc.api.internal.IDatabricksConnectionContext;
import com.databricks.jdbc.common.DatabricksClientType;
import com.databricks.jdbc.common.MetadataResultConstants;
import com.databricks.jdbc.dbclient.MetadataAsyncHandle;
import com.databricks.jdbc.dbclient.MetadataAsyncRequest;
import com.databricks.jdbc.model.core.ResultColumn;
import com.databricks.sdk.core.ApiClient;
import com.databricks.sdk.core.DatabricksConfig;
import com.databricks.sdk.core.http.HttpClient;
import com.databricks.sdk.core.http.Request;
import com.databricks.sdk.core.http.Response;
import com.databricks.sdk.service.sql.StatementExecutionService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/** HTTP-boundary request and conversion matrix for APC SHOW metadata. */
class DatabricksMetadataHttpMatrixTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String OPERATION_HEADER = "X-Databricks-Metadata-Operation-Type";
  private static final String SYNC_HEADER = "x-databricks-sea-can-run-fully-sync";

  @Test
  void procedureSelectsDecodeApcArrowBatchesIntoJdbcMetadata() throws Exception {
    Fixture fake = new Fixture();
    DatabricksSession session = fake.open(true);
    fake.configureProcedure(
        List.of(
            sourceColumn("routine_catalog", Types.VARCHAR),
            sourceColumn("routine_schema", Types.VARCHAR),
            sourceColumn("routine_name", Types.VARCHAR),
            sourceColumn("comment", Types.VARCHAR),
            sourceColumn("specific_name", Types.VARCHAR)),
        Map.of(
            "routine_catalog", "main",
            "routine_schema", "s",
            "routine_name", "p",
            "comment", "procedure remark",
            "specific_name", "p"));
    try (ResultSet result =
        session.getDatabricksMetadataClient().listProcedures(session, "main", "s%", "p%")) {
      assertEquals(
          MetadataResultConstants.PROCEDURES_COLUMNS.size(), result.getMetaData().getColumnCount());
      assertTrue(result.next());
      assertEquals("main", result.getString("PROCEDURE_CAT"));
      assertEquals("s", result.getString("PROCEDURE_SCHEM"));
      assertEquals("p", result.getString("PROCEDURE_NAME"));
      assertEquals("procedure remark", result.getString("REMARKS"));
      assertEquals(DatabaseMetaData.procedureNoResult, result.getShort("PROCEDURE_TYPE"));
      assertEquals("p", result.getString("SPECIFIC_NAME"));
      assertFalse(result.next());
    }
    fake.assertProcedureSubmission("information_schema.routines", 2);

    fake.configureProcedure(
        List.of(
            sourceColumn("specific_catalog", Types.VARCHAR),
            sourceColumn("specific_schema", Types.VARCHAR),
            sourceColumn("specific_name", Types.VARCHAR),
            sourceColumn("parameter_name", Types.VARCHAR),
            sourceColumn("parameter_mode", Types.VARCHAR),
            sourceColumn("is_result", Types.VARCHAR),
            sourceColumn("data_type", Types.VARCHAR),
            sourceColumn("numeric_precision", Types.INTEGER),
            sourceColumn("numeric_precision_radix", Types.INTEGER),
            sourceColumn("numeric_scale", Types.INTEGER),
            sourceColumn("character_maximum_length", Types.INTEGER),
            sourceColumn("character_octet_length", Types.INTEGER),
            sourceColumn("ordinal_position", Types.INTEGER),
            sourceColumn("parameter_default", Types.VARCHAR),
            sourceColumn("comment", Types.VARCHAR)),
        Map.ofEntries(
            Map.entry("specific_catalog", "main"),
            Map.entry("specific_schema", "s"),
            Map.entry("specific_name", "p"),
            Map.entry("parameter_name", "arg"),
            Map.entry("parameter_mode", "IN"),
            Map.entry("is_result", "NO"),
            Map.entry("data_type", "INT"),
            Map.entry("numeric_precision", 10),
            Map.entry("numeric_precision_radix", 10),
            Map.entry("numeric_scale", 0),
            Map.entry("character_maximum_length", 0),
            Map.entry("character_octet_length", 0),
            Map.entry("ordinal_position", 1),
            Map.entry("parameter_default", "7"),
            Map.entry("comment", "parameter remark")));
    try (ResultSet result =
        session
            .getDatabricksMetadataClient()
            .listProcedureColumns(session, "main", "s%", "p%", "a%")) {
      assertEquals(
          MetadataResultConstants.PROCEDURE_COLUMNS_COLUMNS.size(),
          result.getMetaData().getColumnCount());
      assertTrue(result.next());
      assertEquals("main", result.getString("PROCEDURE_CAT"));
      assertEquals("s", result.getString("PROCEDURE_SCHEM"));
      assertEquals("p", result.getString("PROCEDURE_NAME"));
      assertEquals("arg", result.getString("COLUMN_NAME"));
      assertEquals(DatabaseMetaData.procedureColumnIn, result.getShort("COLUMN_TYPE"));
      assertEquals(Types.INTEGER, result.getInt("DATA_TYPE"));
      assertEquals("INT", result.getString("TYPE_NAME"));
      assertEquals(10, result.getInt("PRECISION"));
      assertEquals(1, result.getInt("ORDINAL_POSITION"));
      assertEquals("7", result.getString("COLUMN_DEF"));
      assertEquals("parameter remark", result.getString("REMARKS"));
      assertFalse(result.next());
    }
    fake.assertProcedureSubmission("information_schema.parameters", 3);
    assertEquals(2, fake.closedIds.size());
  }

  private static ResultColumn sourceColumn(String name, int type) {
    return new ResultColumn(name, name, type);
  }

  @Test
  void showMetadataUsesBothResponseFormatsAndPreservesAllJdbcCells() throws Exception {
    for (boolean syncEnabled : new boolean[] {true, false}) {
      Fixture fake = new Fixture();
      DatabricksSession session = fake.open(syncEnabled);
      for (MetadataAsyncRequest request : requests()) {
        for (boolean syncArrow : new boolean[] {true, false}) {
          List<ResultColumn> columns = columns(request);
          Map<String, Object> row = row(request);
          fake.configure(columns, row, syncArrow, false);
          List<List<Object>> expected;
          List<String> expectedColumns;
          try (ResultSet sync = executeSync(session, request)) {
            expectedColumns = columnNames(sync);
            expected = rows(sync);
          }
          assertFalse(expected.isEmpty(), request.getMethod().toString());
          fake.assertSubmission(request, syncEnabled, false);

          // Reverse the representation on the same connection before async completion.
          fake.configure(columns, row, !syncArrow, true);
          MetadataAsyncHandle handle =
              session.getDatabricksMetadataClient().executeMetadataAsync(request, session);
          try (ResultSet async =
              session.getDatabricksMetadataClient().getMetadataExecutionResult(handle, session)) {
            assertEquals(0, ((DatabricksResultSet) async).getUpdateCount());
            assertEquals(expectedColumns, columnNames(async), request.getMethod().toString());
            assertEquals(expected, rows(async), request.getMethod().toString());
          }
          fake.assertSubmission(request, false, true);
          assertEquals(2, fake.pollsForLastStatement);
        }
      }
    }
  }

  @Test
  void boundedSeaApcArrowMetadataReadsRowsWithoutChunkRowCount() throws Exception {
    Fixture fake = new Fixture();
    DatabricksSession session = fake.open(true, true);
    MetadataAsyncRequest request = MetadataAsyncRequest.forCatalogs();
    fake.configure(columns(request), row(request), true, false);
    fake.omitArrowRowCount = true;

    try (ResultSet sync = executeSync(session, request)) {
      List<List<Object>> values = rows(sync);
      assertEquals(1, values.size());
      assertEquals("main", values.get(0).get(0));
    }
    fake.configure(columns(request), row(request), true, true);
    MetadataAsyncHandle handle =
        session.getDatabricksMetadataClient().executeMetadataAsync(request, session);
    try (ResultSet async =
        session.getDatabricksMetadataClient().getMetadataExecutionResult(handle, session)) {
      List<List<Object>> values = rows(async);
      assertEquals(1, values.size());
      assertEquals("main", values.get(0).get(0));
    }
  }

  @Test
  void nullCatalogFallbackSubmitsAsyncPerCatalogAndMatchesSyncRows() throws Exception {
    for (boolean schemas : new boolean[] {true, false}) {
      Fixture fake = new Fixture();
      DatabricksSession session = fake.open(true);
      MetadataAsyncRequest request =
          schemas
              ? MetadataAsyncRequest.forSchemas(null, "s%")
              : MetadataAsyncRequest.forColumns(null, "s%", "t%", "c%");
      fake.configureFallback(schemas, false);
      List<List<Object>> expected;
      List<String> expectedColumns;
      try (ResultSet sync =
          schemas
              ? session.getDatabricksMetadataClient().listSchemas(session, null, "s%")
              : session
                  .getDatabricksMetadataClient()
                  .listColumns(session, null, "s%", "t%", "c%")) {
        expectedColumns = columnNames(sync);
        expected = rows(sync);
      }
      assertEquals(2, expected.size());
      int previousSubmissions = fake.submitted.size();

      fake.configureFallback(schemas, true);
      MetadataAsyncHandle handle =
          session.getDatabricksMetadataClient().executeMetadataAsync(request, session);
      try (ResultSet async =
          session.getDatabricksMetadataClient().getMetadataExecutionResult(handle, session)) {
        assertEquals(0, ((DatabricksResultSet) async).getUpdateCount());
        assertEquals(expectedColumns, columnNames(async));
        assertEquals(expected, rows(async));
      }
      assertEquals(4, fake.submitted.size() - previousSubmissions);
      assertEquals(3, handle.getFallbackStatementIds().size());
      List<String> sql = new ArrayList<>();
      for (int i = previousSubmissions; i < fake.submitted.size(); i++) {
        Request submitted = fake.submitted.get(i);
        JsonNode body = JSON.readTree(fake.submittedBodies.get(i));
        sql.add(body.path("statement").asText());
        assertEquals("ARROW_STREAM", body.path("format").asText());
        assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
        assertEquals(
            i == previousSubmissions + 1
                ? MetadataAsyncRequest.forCatalogs().getOperation().getHeaderValue()
                : request.getOperation().getHeaderValue(),
            submitted.getHeaders().get(OPERATION_HEADER));
        assertFalse(submitted.getHeaders().containsKey(SYNC_HEADER));
      }
      assertEquals("SHOW CATALOGS", sql.get(1));
      for (String catalog : List.of("first", "second")) {
        assertTrue(sql.contains(fallbackSql(schemas, catalog)));
      }
    }
  }

  private static String fallbackSql(boolean schemas, String catalog) {
    return schemas
        ? "SHOW SCHEMAS IN `" + catalog + "` LIKE 's*'"
        : "SHOW COLUMNS IN CATALOG `" + catalog + "` SCHEMA LIKE 's*' TABLE LIKE 't*' LIKE 'c*'";
  }

  private static List<MetadataAsyncRequest> requests() {
    return List.of(
        MetadataAsyncRequest.forCatalogs(),
        MetadataAsyncRequest.forSchemas("main", "s%"),
        MetadataAsyncRequest.forTables("main", "s%", "t%", new String[] {"TABLE"}),
        MetadataAsyncRequest.forColumns("main", "s%", "t%", "c%"),
        MetadataAsyncRequest.forFunctions("main", "s%", "f%"),
        MetadataAsyncRequest.forPrimaryKeys("main", "s", "t"),
        MetadataAsyncRequest.forImportedKeys("main", "s", "t"),
        MetadataAsyncRequest.forCrossReferences("main", "s", "p", "main", "s", "t"));
  }

  private static ResultSet executeSync(DatabricksSession session, MetadataAsyncRequest request)
      throws Exception {
    switch (request.getMethod()) {
      case CATALOGS:
        return session.getDatabricksMetadataClient().listCatalogs(session);
      case SCHEMAS:
        return session.getDatabricksMetadataClient().listSchemas(session, "main", "s%");
      case TABLES:
        return session
            .getDatabricksMetadataClient()
            .listTables(session, "main", "s%", "t%", new String[] {"TABLE"});
      case COLUMNS:
        return session.getDatabricksMetadataClient().listColumns(session, "main", "s%", "t%", "c%");
      case FUNCTIONS:
        return session.getDatabricksMetadataClient().listFunctions(session, "main", "s%", "f%");
      case PRIMARY_KEYS:
        return session.getDatabricksMetadataClient().listPrimaryKeys(session, "main", "s", "t");
      case IMPORTED_KEYS:
        return session.getDatabricksMetadataClient().listImportedKeys(session, "main", "s", "t");
      case CROSS_REFERENCES:
        return session
            .getDatabricksMetadataClient()
            .listCrossReferences(session, "main", "s", "p", "main", "s", "t");
      default:
        throw new AssertionError(request.getMethod());
    }
  }

  private static List<ResultColumn> columns(MetadataAsyncRequest request) {
    switch (request.getMethod()) {
      case CATALOGS:
        return MetadataResultConstants.CATALOG_COLUMNS;
      case SCHEMAS:
        return MetadataResultConstants.SCHEMA_COLUMNS;
      case TABLES:
        return MetadataResultConstants.TABLE_COLUMNS;
      case COLUMNS:
        return MetadataResultConstants.COLUMN_COLUMNS;
      case FUNCTIONS:
        return MetadataResultConstants.FUNCTION_COLUMNS;
      case PRIMARY_KEYS:
        return MetadataResultConstants.PRIMARY_KEYS_COLUMNS;
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        return MetadataResultConstants.CROSS_REFERENCE_COLUMNS;
      default:
        throw new AssertionError(request.getMethod());
    }
  }

  private static Map<String, Object> row(MetadataAsyncRequest request) {
    Map<String, Object> values = new HashMap<>();
    values.put("TABLE_CAT", "main");
    values.put("TABLE_CATALOG", "main");
    values.put("TABLE_SCHEM", "s");
    values.put("TABLE_NAME", "t");
    values.put("TABLE_TYPE", "TABLE");
    values.put("COLUMN_NAME", "c");
    values.put("FUNCTION_CAT", "main");
    values.put("FUNCTION_SCHEM", "s");
    values.put("FUNCTION_NAME", "f");
    values.put("PKTABLE_CAT", "main");
    values.put("PKTABLE_SCHEM", "s");
    values.put("PKTABLE_NAME", "p");
    values.put("FKTABLE_CAT", "main");
    values.put("FKTABLE_SCHEM", "s");
    values.put("FKTABLE_NAME", "t");
    values.put("TYPE_NAME", "STRING");
    values.put("DATA_TYPE", Types.VARCHAR);
    values.put("KEY_SEQ", 1);
    return values;
  }

  private static List<String> columnNames(ResultSet result) throws Exception {
    ResultSetMetaData metadata = result.getMetaData();
    List<String> names = new ArrayList<>();
    for (int i = 1; i <= metadata.getColumnCount(); i++) {
      names.add(metadata.getColumnName(i));
    }
    return names;
  }

  private static List<List<Object>> rows(ResultSet result) throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    int count = result.getMetaData().getColumnCount();
    while (result.next()) {
      List<Object> row = new ArrayList<>();
      for (int i = 1; i <= count; i++) {
        row.add(result.getObject(i));
      }
      rows.add(row);
    }
    return rows;
  }

  private static Object value(ResultColumn column, Map<String, Object> overrides) {
    Object specific = overrides.get(column.getColumnName());
    if (specific != null) {
      return specific;
    }
    return column.getColumnTypeInt() == Types.VARCHAR ? "v" : 1;
  }

  private static String response(
      String id, List<ResultColumn> columns, Map<String, Object> row, boolean arrow)
      throws Exception {
    List<Map<String, Object>> schema = new ArrayList<>();
    List<Map<String, Object>> vectors = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      ResultColumn column = columns.get(i);
      String type =
          column.getColumnTypeInt() == Types.VARCHAR
              ? "STRING"
              : column.getColumnTypeInt() == Types.SMALLINT ? "SHORT" : "INT";
      schema.add(
          Map.of(
              "name", column.getColumnName(),
              "position", i,
              "type_name", type,
              "type_text", type));
      String arm =
          type.equals("STRING")
              ? "string_values"
              : type.equals("SHORT") ? "i16_values" : "i32_values";
      vectors.add(Map.of(arm, Map.of("values", List.of(value(column, row)))));
    }
    Map<String, Object> manifest = new HashMap<>();
    manifest.put("format", arrow ? "ARROW_STREAM" : "COLUMN_BASED_SET");
    manifest.put("is_native_metadata_result", true);
    manifest.put("total_row_count", 1);
    manifest.put("total_chunk_count", 1);
    manifest.put("schema", Map.of("column_count", columns.size(), "columns", schema));
    Map<String, Object> result =
        arrow
            ? Map.of(
                "row_count",
                1,
                "attachment",
                Base64.getEncoder().encodeToString(arrow(columns, row)))
            : Map.of("row_count", 1, "columnar_data", Map.of("columns", vectors));
    return JSON.writeValueAsString(
        Map.of(
            "statement_id", id,
            "status", Map.of("state", "SUCCEEDED"),
            "manifest", manifest,
            "result", result));
  }

  private static String procedureResponse(
      String id, List<ResultColumn> columns, Map<String, Object> row) throws Exception {
    List<Map<String, Object>> schema = new ArrayList<>();
    for (int i = 0; i < columns.size(); i++) {
      ResultColumn column = columns.get(i);
      String type = column.getColumnTypeInt() == Types.VARCHAR ? "STRING" : "INT";
      schema.add(
          Map.of(
              "name", column.getColumnName(),
              "position", i,
              "type_name", type,
              "type_text", type));
    }
    byte[] stream = arrow(columns, row);
    int schemaLength =
        8 + java.nio.ByteBuffer.wrap(stream, 4, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    byte[] batch = Arrays.copyOfRange(stream, schemaLength, stream.length - 8);
    return JSON.writeValueAsString(
        Map.of(
            "statement_id", id,
            "status", Map.of("state", "SUCCEEDED"),
            "manifest",
                Map.of(
                    "format",
                    "ARROW_BATCH",
                    "total_row_count",
                    1,
                    "total_chunk_count",
                    1,
                    "schema",
                    Map.of("column_count", columns.size(), "columns", schema)),
            "result",
                Map.of(
                    "row_count",
                    1,
                    "arrow_batches",
                    List.of(
                        Map.of(
                            "row_count", 1, "batch", Base64.getEncoder().encodeToString(batch))))));
  }

  private static String catalogsResponse(String id) throws Exception {
    return JSON.writeValueAsString(
        Map.of(
            "statement_id", id,
            "status", Map.of("state", "SUCCEEDED"),
            "manifest",
                Map.of(
                    "format",
                    "COLUMN_BASED_SET",
                    "is_native_metadata_result",
                    true,
                    "total_row_count",
                    2,
                    "total_chunk_count",
                    1,
                    "schema",
                    Map.of(
                        "column_count",
                        1,
                        "columns",
                        List.of(
                            Map.of(
                                "name", "TABLE_CAT",
                                "position", 0,
                                "type_name", "STRING",
                                "type_text", "STRING")))),
            "result",
                Map.of(
                    "row_count",
                    2,
                    "columnar_data",
                    Map.of(
                        "columns",
                        List.of(
                            Map.of(
                                "string_values", Map.of("values", List.of("first", "second"))))))));
  }

  private static byte[] arrow(List<ResultColumn> columns, Map<String, Object> row)
      throws Exception {
    List<Field> fields = new ArrayList<>();
    for (ResultColumn column : columns) {
      int type = column.getColumnTypeInt();
      ArrowType arrowType =
          type == Types.VARCHAR
              ? ArrowType.Utf8.INSTANCE
              : new ArrowType.Int(type == Types.SMALLINT ? 16 : 32, true);
      fields.add(new Field(column.getColumnName(), FieldType.nullable(arrowType), null));
    }
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (RootAllocator allocator = new RootAllocator();
        VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), allocator);
        ArrowStreamWriter writer = new ArrowStreamWriter(root, null, bytes)) {
      root.allocateNew();
      for (int i = 0; i < columns.size(); i++) {
        ResultColumn column = columns.get(i);
        Object value = value(column, row);
        if (column.getColumnTypeInt() == Types.VARCHAR) {
          ((VarCharVector) root.getVector(i))
              .setSafe(0, value.toString().getBytes(StandardCharsets.UTF_8));
        } else if (column.getColumnTypeInt() == Types.SMALLINT) {
          ((SmallIntVector) root.getVector(i)).setSafe(0, ((Number) value).intValue());
        } else {
          ((IntVector) root.getVector(i)).setSafe(0, ((Number) value).intValue());
        }
      }
      root.setRowCount(1);
      writer.start();
      writer.writeBatch();
      writer.end();
    }
    return bytes.toByteArray();
  }

  private static final class Fixture implements HttpClient {
    private final List<Request> submitted = new ArrayList<>();
    private final List<String> submittedBodies = new ArrayList<>();
    private final List<String> closedIds = new ArrayList<>();
    private final Map<String, String> responses = new HashMap<>();
    private final Map<String, Integer> polls = new HashMap<>();
    private List<ResultColumn> columns;
    private Map<String, Object> row;
    private boolean arrow;
    private boolean async;
    private boolean fallback;
    private boolean fallbackSchemas;
    private boolean procedure;
    private boolean omitArrowRowCount;
    private int pollsForLastStatement;
    private int nextId;

    DatabricksSession open(boolean syncEnabled) throws Exception {
      return open(syncEnabled, false);
    }

    DatabricksSession open(boolean syncEnabled, boolean bounded) throws Exception {
      DatabricksConfig config =
          new DatabricksConfig()
              .setHost("https://example.com")
              .setAuthType("pat")
              .setToken("test-token")
              .setHttpClient(this);
      ApiClient api = DatabricksSdkClient.buildApiClient(config, null, true);
      IDatabricksConnectionContext context =
          DatabricksConnectionContext.parse(
              TestConstants.VALID_CLUSTER_URL
                  + ";UseThriftClient=0;EnableMultipleCatalogSupport=1;EnableSQLExecHybridResults=0;"
                  + "EnableThriftNativeMetadata=1;EnableSeaSyncMetadata="
                  + (syncEnabled ? "1" : "0")
                  + ";UseBoundedSeaApi="
                  + (bounded ? "1" : "0")
                  + ";TelemetryLogLevel=OFF;",
              new Properties());
      context.setClientType(DatabricksClientType.SEA);
      DatabricksSdkClient client =
          new DatabricksSdkClient(context, mock(StatementExecutionService.class), api);
      DatabricksSession session = new DatabricksSession(context, client);
      session.open();
      return session;
    }

    void configure(
        List<ResultColumn> columns, Map<String, Object> row, boolean arrow, boolean async) {
      this.columns = columns;
      this.row = row;
      this.arrow = arrow;
      this.async = async;
      this.fallback = false;
      this.procedure = false;
      this.pollsForLastStatement = 0;
    }

    void configureProcedure(List<ResultColumn> columns, Map<String, Object> row) {
      this.columns = columns;
      this.row = row;
      this.procedure = true;
      this.fallback = false;
      this.async = false;
    }

    void assertProcedureSubmission(String table, int parameterCount) throws Exception {
      Request request = submitted.get(submitted.size() - 1);
      JsonNode body = JSON.readTree(submittedBodies.get(submittedBodies.size() - 1));
      assertTrue(body.path("statement").asText().contains(table));
      assertEquals("ARROW_BATCH", body.path("format").asText());
      assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
      assertEquals(parameterCount, body.path("parameters").size());
      assertFalse(request.getHeaders().containsKey(OPERATION_HEADER));
      assertFalse(request.getHeaders().containsKey(SYNC_HEADER));
    }

    void configureFallback(boolean schemas, boolean async) {
      this.fallback = true;
      this.fallbackSchemas = schemas;
      this.async = async;
      this.pollsForLastStatement = 0;
    }

    void assertSubmission(MetadataAsyncRequest expected, boolean syncEnabled, boolean async)
        throws Exception {
      Request request = submitted.get(submitted.size() - 1);
      JsonNode body = JSON.readTree(submittedBodies.get(submittedBodies.size() - 1));
      assertEquals("ARROW_STREAM", body.path("format").asText());
      assertEquals("INLINE_OR_EXTERNAL_LINKS", body.path("disposition").asText());
      assertEquals(
          expected.getOperation().getHeaderValue(), request.getHeaders().get(OPERATION_HEADER));
      assertEquals(syncEnabled && !async, "true".equals(request.getHeaders().get(SYNC_HEADER)));
      assertEquals("true", request.getHeaders().get("X-Databricks-Require-Thrift-Native-Metadata"));
      assertEquals(
          expectedSql(expected),
          body.path("statement").asText(),
          expected.getMethod() + " SQL: " + body.path("statement").asText());
    }

    @Override
    public synchronized Response execute(Request request) {
      String path = request.getUri().getPath();
      if (path.equals("/oidc/.well-known/oauth-authorization-server")) {
        return new Response(request, 200, "OK", Map.of(), "{}");
      }
      String body;
      if (path.equals(PathConstants.SESSION_PATH)) {
        body = "{\"session_id\":\"session\",\"negotiated_protocol_version\":42248}";
      } else if (path.equals(PathConstants.STATEMENT_PATH)
          && Request.POST.equals(request.getMethod())) {
        submitted.add(request);
        submittedBodies.add(request.getBodyString());
        String id = "statement-" + ++nextId;
        String completed =
            fallback ? fallbackResponse(id, request.getBodyString()) : completedResponse(id);
        responses.put(id, completed);
        body =
            async
                ? "{\"statement_id\":\"" + id + "\",\"status\":{\"state\":\"PENDING\"}}"
                : completed;
      } else if (path.startsWith(PathConstants.STATEMENT_PATH)
          && Request.GET.equals(request.getMethod())) {
        String id = path.substring(PathConstants.STATEMENT_PATH.length());
        if (!responses.containsKey(id)) {
          throw new AssertionError("Unexpected statement: " + id);
        }
        int poll = polls.getOrDefault(id, 0);
        polls.put(id, poll + 1);
        pollsForLastStatement++;
        body =
            poll == 0
                ? "{\"statement_id\":\"" + id + "\",\"status\":{\"state\":\"RUNNING\"}}"
                : responses.get(id);
      } else if (Request.DELETE.equals(request.getMethod())) {
        closedIds.add(path);
        body = "{}";
      } else {
        throw new AssertionError("Unexpected request: " + path);
      }
      return new Response(request, 200, "OK", Map.of(), body);
    }

    private String completedResponse(String id) {
      try {
        String response =
            procedure ? procedureResponse(id, columns, row) : response(id, columns, row, arrow);
        if (omitArrowRowCount && arrow && !procedure) {
          ObjectNode root = (ObjectNode) JSON.readTree(response);
          ((ObjectNode) root.path("result")).remove("row_count");
          return JSON.writeValueAsString(root);
        }
        return response;
      } catch (Exception e) {
        throw new AssertionError("Could not build metadata response", e);
      }
    }

    private String fallbackResponse(String id, String requestBody) {
      try {
        String sql = JSON.readTree(requestBody).path("statement").asText();
        if (sql.contains("ALL CATALOGS")) {
          return JSON.writeValueAsString(
              Map.of(
                  "statement_id", id, "status", Map.of("state", "FAILED", "sql_state", "42601")));
        }
        if (sql.equals("SHOW CATALOGS")) {
          return catalogsResponse(id);
        }
        String catalog = sql.contains("`first`") ? "first" : "second";
        Map<String, Object> values =
            row(
                fallbackSchemas
                    ? MetadataAsyncRequest.forSchemas(catalog, "s%")
                    : MetadataAsyncRequest.forColumns(catalog, "s%", "t%", "c%"));
        values.put("TABLE_CAT", catalog);
        values.put("TABLE_CATALOG", catalog);
        return response(
            id,
            fallbackSchemas
                ? MetadataResultConstants.SCHEMA_COLUMNS
                : MetadataResultConstants.COLUMN_COLUMNS,
            values,
            false);
      } catch (Exception e) {
        throw new AssertionError("Could not build fallback metadata response", e);
      }
    }
  }

  private static String expectedSql(MetadataAsyncRequest request) {
    switch (request.getMethod()) {
      case CATALOGS:
        return "SHOW CATALOGS";
      case SCHEMAS:
        return "SHOW SCHEMAS IN `main` LIKE 's*'";
      case TABLES:
        return "SHOW TABLES IN CATALOG `main` SCHEMA LIKE 's*' LIKE 't*'";
      case COLUMNS:
        return "SHOW COLUMNS IN CATALOG `main` SCHEMA LIKE 's*' TABLE LIKE 't*' LIKE 'c*'";
      case FUNCTIONS:
        return "SHOW FUNCTIONS IN CATALOG `main` SCHEMA LIKE 's*' LIKE 'f*'";
      case PRIMARY_KEYS:
        return "SHOW KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`";
      case IMPORTED_KEYS:
      case CROSS_REFERENCES:
        return "SHOW FOREIGN KEYS IN CATALOG `main` IN SCHEMA `s` IN TABLE `t`";
      default:
        throw new AssertionError(request.getMethod());
    }
  }
}
