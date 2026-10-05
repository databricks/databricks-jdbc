package com.databricks.jdbc.model.core;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

public class ResultManifestTest {

  @Test
  void testNativeMetadataResultJsonRoundTrip() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    ResultManifest manifest =
        mapper.readValue("{\"is_native_metadata_result\":true}", ResultManifest.class);

    assertEquals(Boolean.TRUE, manifest.getIsNativeMetadataResult());
    assertEquals(
        Boolean.TRUE,
        mapper
            .readTree(mapper.writeValueAsString(manifest))
            .get("is_native_metadata_result")
            .booleanValue());
  }
}
