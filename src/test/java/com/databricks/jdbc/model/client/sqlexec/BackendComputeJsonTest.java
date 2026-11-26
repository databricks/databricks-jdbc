package com.databricks.jdbc.model.client.sqlexec;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class BackendComputeJsonTest {
  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void testAllPurposeClusterUsesProtoJsonFieldNames() {
    BackendCompute backendCompute =
        new BackendCompute()
            .setAllPurposeClusterCompute(
                new AllPurposeClusterCompute().setClusterId("cluster-123"));

    JsonNode json = objectMapper.valueToTree(backendCompute);

    assertEquals("cluster-123", json.get("all_purpose_cluster_compute").get("cluster_id").asText());
  }

  @Test
  public void testAllPurposeClusterDeserializesFromProtoJson() throws Exception {
    BackendCompute backendCompute =
        objectMapper.readValue(
            "{\"all_purpose_cluster_compute\":{\"cluster_id\":\"cluster-123\"}}",
            BackendCompute.class);

    assertEquals("cluster-123", backendCompute.getAllPurposeClusterCompute().getClusterId());
  }
}
