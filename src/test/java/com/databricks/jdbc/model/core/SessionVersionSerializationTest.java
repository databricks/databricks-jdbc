package com.databricks.jdbc.model.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.databricks.jdbc.model.client.sqlexec.CreateSessionRequest;
import com.databricks.jdbc.model.client.sqlexec.CreateSessionResponse;
import com.databricks.jdbc.model.client.sqlexec.ExecuteStatementRequest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class SessionVersionSerializationTest {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testSessionExecutionModeAndVersionUseProtoJsonFieldNames() throws Exception {
    JsonNode createRequest =
        objectMapper.valueToTree(
            new CreateSessionRequest()
                .setWarehouseId("warehouse")
                .setExecutionMode(SessionExecutionMode.FAST));
    JsonNode executeRequest =
        objectMapper.valueToTree(
            new ExecuteStatementRequest()
                .setSessionVersion(new SessionVersion().setVersionId(42L)));

    assertEquals("FAST", createRequest.get("execution_mode").asText());
    assertEquals(42L, executeRequest.get("session_version").get("version_id").asLong());
  }

  @Test
  public void testSessionVersionsDeserializeFromCreateAndStatusResponses() throws Exception {
    CreateSessionResponse createResponse =
        objectMapper.readValue(
            "{\"session_id\":\"session\",\"session_version\":{\"version_id\":7}}",
            CreateSessionResponse.class);
    StatementStatus status =
        objectMapper.readValue(
            "{\"state\":\"SUCCEEDED\",\"session_version\":{\"version_id\":9}}",
            StatementStatus.class);

    assertEquals(7L, createResponse.getSessionVersion().getVersionId());
    assertEquals(9L, status.getSessionVersion().getVersionId());
  }
}
