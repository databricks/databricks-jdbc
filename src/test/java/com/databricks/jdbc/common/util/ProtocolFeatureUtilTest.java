package com.databricks.jdbc.common.util;

import static com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion.*;
import static org.junit.jupiter.api.Assertions.*;

import com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/** Unit tests for {@link ProtocolFeatureUtil}. */
public class ProtocolFeatureUtilTest {

  private static final TProtocolVersion MIN_VERSION_DATABRICKS_COMPUTE =
      SPARK_CLI_SERVICE_PROTOCOL_V1;
  // Store minimum protocol version for each feature
  private static final TProtocolVersion MIN_VERSION_GET_INFOS = SPARK_CLI_SERVICE_PROTOCOL_V1;
  private static final TProtocolVersion MIN_VERSION_DIRECT_RESULTS = SPARK_CLI_SERVICE_PROTOCOL_V1;
  private static final TProtocolVersion MIN_VERSION_MODIFIED_MORE_ROWS =
      SPARK_CLI_SERVICE_PROTOCOL_V1;
  private static final TProtocolVersion MIN_VERSION_CLOUD_FETCH = SPARK_CLI_SERVICE_PROTOCOL_V3;
  private static final TProtocolVersion MIN_VERSION_MULTIPLE_CATALOGS =
      SPARK_CLI_SERVICE_PROTOCOL_V4;
  private static final TProtocolVersion MIN_VERSION_ARROW_METADATA = SPARK_CLI_SERVICE_PROTOCOL_V5;
  private static final TProtocolVersion MIN_VERSION_RESULTSET_METADATA =
      SPARK_CLI_SERVICE_PROTOCOL_V5;
  private static final TProtocolVersion MIN_VERSION_ADVANCED_ARROW = SPARK_CLI_SERVICE_PROTOCOL_V5;
  private static final TProtocolVersion MIN_VERSION_COMPRESSED_ARROW =
      SPARK_CLI_SERVICE_PROTOCOL_V6;
  private static final TProtocolVersion MIN_VERSION_ASYNC_METADATA = SPARK_CLI_SERVICE_PROTOCOL_V6;
  private static final TProtocolVersion MIN_VERSION_RESULT_PERSISTENCE =
      SPARK_CLI_SERVICE_PROTOCOL_V7;
  private static final TProtocolVersion MIN_VERSION_PARAMETERIZED = SPARK_CLI_SERVICE_PROTOCOL_V8;
  private static final TProtocolVersion MIN_VERSION_ASYNC_OPERATIONS =
      SPARK_CLI_SERVICE_PROTOCOL_V9;

  private static Stream<Arguments> protocolVersionProvider() {
    return Stream.of(
        Arguments.of(HIVE_CLI_SERVICE_PROTOCOL_V1),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V1),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V2),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V3),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V4),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V5),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V6),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V7),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V8),
        Arguments.of(SPARK_CLI_SERVICE_PROTOCOL_V9));
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsGetInfosInOpenSession(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_GET_INFOS) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsGetInfosInOpenSession(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsDirectResults(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_DIRECT_RESULTS) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsDirectResults(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsModifiedHasMoreRowsSemantics(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_MODIFIED_MORE_ROWS) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsModifiedHasMoreRowsSemantics(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsCloudFetch(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_CLOUD_FETCH) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsCloudFetch(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsMultipleCatalogs(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_MULTIPLE_CATALOGS) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsMultipleCatalogs(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsArrowMetadata(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_ARROW_METADATA) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsArrowMetadata(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsResultSetMetadataFromFetch(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_RESULTSET_METADATA) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsResultSetMetadataFromFetch(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAdvancedArrowTypes(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_ADVANCED_ARROW) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsAdvancedArrowTypes(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsCompressedArrowBatches(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_COMPRESSED_ARROW) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsCompressedArrowBatches(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAsyncMetadataExecution(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_ASYNC_METADATA) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsAsyncMetadataExecution(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsResultPersistenceMode(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_RESULT_PERSISTENCE) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsResultPersistenceMode(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsParameterizedQueries(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_PARAMETERIZED) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsParameterizedQueries(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testSupportsAsyncMetadataOperations(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_ASYNC_OPERATIONS) >= 0;
    boolean actual = ProtocolFeatureUtil.supportsAsyncMetadataOperations(version);
    assertEquals(expected, actual);
  }

  @ParameterizedTest
  @MethodSource("protocolVersionProvider")
  public void testIsNonDatabricksCompute(TProtocolVersion version) {
    boolean expected = version.compareTo(MIN_VERSION_DATABRICKS_COMPUTE) < 0;
    boolean actual = ProtocolFeatureUtil.isNonDatabricksCompute(version);
    assertEquals(expected, actual);
  }
}
