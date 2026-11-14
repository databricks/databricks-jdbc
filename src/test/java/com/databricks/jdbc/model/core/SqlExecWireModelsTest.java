package com.databricks.jdbc.model.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.databricks.jdbc.model.client.sqlexec.AllPurposeClusterCompute;
import com.databricks.jdbc.model.client.sqlexec.BackendCompute;
import com.databricks.jdbc.model.client.sqlexec.CancelStatementRequest;
import com.databricks.jdbc.model.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.model.client.sqlexec.CreateSessionResponse;
import com.databricks.jdbc.model.client.sqlexec.DeleteSessionRequest;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import org.junit.jupiter.api.Test;

class SqlExecWireModelsTest {
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  void formatsKeepExactWireNamesAndAcceptUnknownResponses() throws Exception {
    for (SqlExecResultFormat format :
        List.of(
            SqlExecResultFormat.JSON_ARRAY,
            SqlExecResultFormat.ARROW_STREAM,
            SqlExecResultFormat.ARROW_BATCH,
            SqlExecResultFormat.COLUMN_BASED_SET)) {
      String json = mapper.writeValueAsString(new ResultManifest().setFormat(format));
      assertEquals(format.name(), mapper.readTree(json).get("format").asText());
      assertEquals(format, mapper.readValue(json, ResultManifest.class).getFormat());
    }
    assertEquals(
        SqlExecResultFormat.UNKNOWN,
        mapper.readValue("{\"format\":\"FUTURE_FORMAT\"}", ResultManifest.class).getFormat());
    assertEquals(
        SqlExecResultFormat.COLUMN_BASED_SET,
        mapper.readValue("{\"format\":6}", ResultManifest.class).getFormat());
  }

  @Test
  void apcBodyModelsUseBackendComputeAndOmitWarehouseId() throws Exception {
    BackendCompute compute =
        new BackendCompute()
            .setAllPurposeClusterCompute(new AllPurposeClusterCompute().setClusterId("cluster-1"));
    List<Object> requests =
        List.of(
            new CreateSessionRequest().setBackendCompute(compute),
            new ExecuteStatementRequest()
                .setStatement("SELECT 1")
                .setFormat(SqlExecResultFormat.ARROW_BATCH)
                .setBackendCompute(compute),
            new CancelStatementRequest().setStatementId("statement-1").setBackendCompute(compute),
            new DeleteSessionRequest().setSessionId("session-1").setBackendCompute(compute));
    for (Object request : requests) {
      JsonNode json = mapper.readTree(mapper.writeValueAsString(request));
      assertEquals(
          "cluster-1",
          json.path("backend_compute")
              .path("all_purpose_cluster_compute")
              .path("cluster_id")
              .asText());
      assertFalse(json.has("warehouse_id"));
    }
    assertEquals(
        "ARROW_BATCH",
        mapper.readTree(mapper.writeValueAsString(requests.get(1))).get("format").asText());
    assertEquals(
        compute,
        mapper
            .readValue(mapper.writeValueAsString(requests.get(1)), ExecuteStatementRequest.class)
            .getBackendCompute());
  }

  @Test
  void warehouseBodiesDoNotGainBackendCompute() throws Exception {
    JsonNode create =
        mapper.readTree(mapper.writeValueAsString(new CreateSessionRequest().setWarehouseId("w")));
    JsonNode execute =
        mapper.readTree(
            mapper.writeValueAsString(
                new ExecuteStatementRequest()
                    .setWarehouseId("w")
                    .setFormat(SqlExecResultFormat.JSON_ARRAY)));
    assertEquals("w", create.get("warehouse_id").asText());
    assertEquals("w", execute.get("warehouse_id").asText());
    assertFalse(create.has("backend_compute"));
    assertFalse(execute.has("backend_compute"));
  }

  @Test
  void responseModelsReadNewOptionalFields() throws Exception {
    ResultData data =
        mapper.readValue(
            "{\"arrow_batches\":[{\"batch\":\"AQID\",\"row_count\":2}],"
                + "\"columnar_data\":{\"columns\":[{\"string_values\":{\"values\":[\"x\"]}}]}}",
            ResultData.class);
    assertEquals(2L, data.getArrowBatches().get(0).getRowCount());
    assertEquals(
        "x", data.getColumnarData().getColumns().get(0).getStringValues().getValues().get(0));
    JsonNode serialized = mapper.readTree(mapper.writeValueAsString(data));
    assertTrue(serialized.has("columnar_data"));
    assertFalse(serialized.path("columnar_data").path("columns").get(0).has("values"));
    assertEquals(data, mapper.readValue(mapper.writeValueAsString(data), ResultData.class));
    assertEquals(
        42250L,
        mapper
            .readValue("{\"negotiated_protocol_version\":42250}", CreateSessionResponse.class)
            .getNegotiatedProtocolVersion());
    assertEquals(
        "{\"type\":\"array\"}",
        mapper
            .readValue("{\"type_json\":\"{\\\"type\\\":\\\"array\\\"}\"}", ColumnInfo.class)
            .getTypeJson());
  }
}
