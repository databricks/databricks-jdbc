package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Unit tests for {@link ProtocolFeatureUtil}. */
public class ProtocolFeatureUtilTest {

  @Test
  public void testSupportsGetInfosInOpenSession() {
    // Test versions that should support the feature
    assertTrue(
        ProtocolFeatureUtil.supportsGetInfosInOpenSession(
            SPARK_CLI_SERVICE_PROTOCOL_V1.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsGetInfosInOpenSession(
            SPARK_CLI_SERVICE_PROTOCOL_V2.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsGetInfosInOpenSession(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Test versions that should not support the feature
    assertFalse(
        ProtocolFeatureUtil.supportsGetInfosInOpenSession(
            SPARK_CLI_SERVICE_PROTOCOL_V1.getValue() - 1));
  }

  @Test
  public void testSupportsDirectResults() {
    assertTrue(ProtocolFeatureUtil.supportsDirectResults(SPARK_CLI_SERVICE_PROTOCOL_V1.getValue()));
    assertTrue(ProtocolFeatureUtil.supportsDirectResults(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    assertFalse(
        ProtocolFeatureUtil.supportsDirectResults(SPARK_CLI_SERVICE_PROTOCOL_V1.getValue() - 1));
  }

  @Test
  public void testSupportsModifiedHasMoreRowsSemantics() {
    assertTrue(
        ProtocolFeatureUtil.supportsModifiedHasMoreRowsSemantics(
            SPARK_CLI_SERVICE_PROTOCOL_V1.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsModifiedHasMoreRowsSemantics(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    assertFalse(
        ProtocolFeatureUtil.supportsModifiedHasMoreRowsSemantics(
            SPARK_CLI_SERVICE_PROTOCOL_V1.getValue() - 1));
  }

  @Test
  public void testSupportsCloudFetch() {
    // Should support from V3 onwards
    assertTrue(ProtocolFeatureUtil.supportsCloudFetch(SPARK_CLI_SERVICE_PROTOCOL_V3.getValue()));
    assertTrue(ProtocolFeatureUtil.supportsCloudFetch(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V3
    assertFalse(ProtocolFeatureUtil.supportsCloudFetch(SPARK_CLI_SERVICE_PROTOCOL_V2.getValue()));
    assertFalse(ProtocolFeatureUtil.supportsCloudFetch(SPARK_CLI_SERVICE_PROTOCOL_V1.getValue()));
  }

  @Test
  public void testSupportsMultipleCatalogs() {
    // Should support from V4 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsMultipleCatalogs(SPARK_CLI_SERVICE_PROTOCOL_V4.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsMultipleCatalogs(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V4
    assertFalse(
        ProtocolFeatureUtil.supportsMultipleCatalogs(SPARK_CLI_SERVICE_PROTOCOL_V3.getValue()));
  }

  @Test
  public void testSupportsArrowMetadata() {
    // Should support from V5 onwards
    assertTrue(ProtocolFeatureUtil.supportsArrowMetadata(SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()));
    assertTrue(ProtocolFeatureUtil.supportsArrowMetadata(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V5
    assertFalse(
        ProtocolFeatureUtil.supportsArrowMetadata(SPARK_CLI_SERVICE_PROTOCOL_V4.getValue()));
  }

  @Test
  public void testSupportsResultSetMetadataFromFetch() {
    // Should support from V5 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(
            SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V5
    assertFalse(
        ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(
            SPARK_CLI_SERVICE_PROTOCOL_V4.getValue()));
  }

  @Test
  public void testSupportsAdvancedArrowTypes() {
    // Should support from V5 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsAdvancedArrowTypes(SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsAdvancedArrowTypes(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V5
    assertFalse(
        ProtocolFeatureUtil.supportsAdvancedArrowTypes(SPARK_CLI_SERVICE_PROTOCOL_V4.getValue()));
  }

  @Test
  public void testSupportsCompressedArrowBatches() {
    // Should support from V6 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsCompressedArrowBatches(
            SPARK_CLI_SERVICE_PROTOCOL_V6.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsCompressedArrowBatches(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V6
    assertFalse(
        ProtocolFeatureUtil.supportsCompressedArrowBatches(
            SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()));
  }

  @Test
  public void testSupportsAsyncMetadataExecution() {
    // Should support from V6 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsAsyncMetadataExecution(
            SPARK_CLI_SERVICE_PROTOCOL_V6.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsAsyncMetadataExecution(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V6
    assertFalse(
        ProtocolFeatureUtil.supportsAsyncMetadataExecution(
            SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()));
  }

  @Test
  public void testSupportsResultPersistenceMode() {
    // Should support from V7 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsResultPersistenceMode(
            SPARK_CLI_SERVICE_PROTOCOL_V7.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsResultPersistenceMode(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V7
    assertFalse(
        ProtocolFeatureUtil.supportsResultPersistenceMode(
            SPARK_CLI_SERVICE_PROTOCOL_V6.getValue()));
  }

  @Test
  public void testSupportsParameterizedQueries() {
    // Should support from V8 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsParameterizedQueries(SPARK_CLI_SERVICE_PROTOCOL_V8.getValue()));
    assertTrue(
        ProtocolFeatureUtil.supportsParameterizedQueries(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V8
    assertFalse(
        ProtocolFeatureUtil.supportsParameterizedQueries(SPARK_CLI_SERVICE_PROTOCOL_V7.getValue()));
  }

  @Test
  public void testSupportsAsyncMetadataOperations() {
    // Should support from V9 onwards
    assertTrue(
        ProtocolFeatureUtil.supportsAsyncMetadataOperations(
            SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));

    // Should not support before V9
    assertFalse(
        ProtocolFeatureUtil.supportsAsyncMetadataOperations(
            SPARK_CLI_SERVICE_PROTOCOL_V8.getValue()));
  }
}
