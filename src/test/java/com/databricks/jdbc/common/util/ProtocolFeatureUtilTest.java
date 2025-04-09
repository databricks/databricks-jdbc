package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit tests for {@link ProtocolFeatureUtil}. */
public class ProtocolFeatureUtilTest {

  // Store minimum protocol version for each feature
  private static final int MIN_VERSION_GET_INFOS = SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  private static final int MIN_VERSION_DIRECT_RESULTS = SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  private static final int MIN_VERSION_MODIFIED_MORE_ROWS =
      SPARK_CLI_SERVICE_PROTOCOL_V1.getValue();
  private static final int MIN_VERSION_CLOUD_FETCH = SPARK_CLI_SERVICE_PROTOCOL_V3.getValue();
  private static final int MIN_VERSION_MULTIPLE_CATALOGS = SPARK_CLI_SERVICE_PROTOCOL_V4.getValue();
  private static final int MIN_VERSION_ARROW_METADATA = SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  private static final int MIN_VERSION_RESULTSET_METADATA =
      SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  private static final int MIN_VERSION_ADVANCED_ARROW = SPARK_CLI_SERVICE_PROTOCOL_V5.getValue();
  private static final int MIN_VERSION_COMPRESSED_ARROW = SPARK_CLI_SERVICE_PROTOCOL_V6.getValue();
  private static final int MIN_VERSION_ASYNC_METADATA = SPARK_CLI_SERVICE_PROTOCOL_V6.getValue();
  private static final int MIN_VERSION_RESULT_PERSISTENCE =
      SPARK_CLI_SERVICE_PROTOCOL_V7.getValue();
  private static final int MIN_VERSION_PARAMETERIZED = SPARK_CLI_SERVICE_PROTOCOL_V8.getValue();
  private static final int MIN_VERSION_ASYNC_OPERATIONS = SPARK_CLI_SERVICE_PROTOCOL_V9.getValue();

  private static Stream<Arguments> protocolVersionProvider() {
    return Stream.of(
        Arguments.of(HIVE_CLI_SERVICE_PROTOCOL_V1.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V1.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V2.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V3.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V4.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V5.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V6.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V7.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V8.getValue()),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V9.getValue()));
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsGetInfosInOpenSession(int version) {
    boolean expected = version >= MIN_VERSION_GET_INFOS;
    boolean actual = ProtocolFeatureUtil.supportsGetInfosInOpenSession(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsDirectResults(int version) {
    boolean expected = version >= MIN_VERSION_DIRECT_RESULTS;
    boolean actual = ProtocolFeatureUtil.supportsDirectResults(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsModifiedHasMoreRowsSemantics(int version) {
    boolean expected = version >= MIN_VERSION_MODIFIED_MORE_ROWS;
    boolean actual = ProtocolFeatureUtil.supportsModifiedHasMoreRowsSemantics(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsCloudFetch(int version) {
    boolean expected = version >= MIN_VERSION_CLOUD_FETCH;
    boolean actual = ProtocolFeatureUtil.supportsCloudFetch(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsMultipleCatalogs(int version) {
    boolean expected = version >= MIN_VERSION_MULTIPLE_CATALOGS;
    boolean actual = ProtocolFeatureUtil.supportsMultipleCatalogs(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsArrowMetadata(int version) {
    boolean expected = version >= MIN_VERSION_ARROW_METADATA;
    boolean actual = ProtocolFeatureUtil.supportsArrowMetadata(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsResultSetMetadataFromFetch(int version) {
    boolean expected = version >= MIN_VERSION_RESULTSET_METADATA;
    boolean actual = ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAdvancedArrowTypes(int version) {
    boolean expected = version >= MIN_VERSION_ADVANCED_ARROW;
    boolean actual = ProtocolFeatureUtil.supportsAdvancedArrowTypes(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsCompressedArrowBatches(int version) {
    boolean expected = version >= MIN_VERSION_COMPRESSED_ARROW;
    boolean actual = ProtocolFeatureUtil.supportsCompressedArrowBatches(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAsyncMetadataExecution(int version) {
    boolean expected = version >= MIN_VERSION_ASYNC_METADATA;
    boolean actual = ProtocolFeatureUtil.supportsAsyncMetadataExecution(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsResultPersistenceMode(int version) {
    boolean expected = version >= MIN_VERSION_RESULT_PERSISTENCE;
    boolean actual = ProtocolFeatureUtil.supportsResultPersistenceMode(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsParameterizedQueries(int version) {
    boolean expected = version >= MIN_VERSION_PARAMETERIZED;
    boolean actual = ProtocolFeatureUtil.supportsParameterizedQueries(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAsyncMetadataOperations(int version) {
    boolean expected = version >= MIN_VERSION_ASYNC_OPERATIONS;
    boolean actual = ProtocolFeatureUtil.supportsAsyncMetadataOperations(version);
    assertEquals(expected, actual);
  }
}
