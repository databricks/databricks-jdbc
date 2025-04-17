package com.databricks.jdbc.common.util;

import com.databricks.jdbc.model.client.thrift.generated.TProtocolVersion;

/**
 * Utility class for checking Spark protocol version features. Provides methods to determine if
 * specific protocol features are supported.
 */
public final class ProtocolFeatureUtil {
  // Prevent instantiation
  private ProtocolFeatureUtil() {}

  /**
   * Checks if the given protocol version supports getting additional information in OpenSession.
   *
   * @param protocolVersion The protocol version to check
   * @return true if getInfos in OpenSession is supported, false otherwise
   */
  public static boolean supportsGetInfosInOpenSession(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1) >= 0;
  }

  /**
   * Checks if the given protocol version supports direct results.
   *
   * @param protocolVersion The protocol version to check
   * @return true if direct results are supported, false otherwise
   */
  public static boolean supportsDirectResults(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1) >= 0;
  }

  /**
   * Checks if the given protocol version supports modified hasMoreRows semantics.
   *
   * @param protocolVersion The protocol version to check
   * @return true if modified hasMoreRows semantics are supported, false otherwise
   */
  public static boolean supportsModifiedHasMoreRowsSemantics(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V1) >= 0;
  }

  /**
   * Checks if the given protocol version supports cloud result fetching.
   *
   * @param protocolVersion The protocol version to check
   * @return true if cloud fetch is supported, false otherwise
   */
  public static boolean supportsCloudFetch(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V3) >= 0;
  }

  /**
   * Checks if the given protocol version supports multiple catalogs in metadata operations.
   *
   * @param protocolVersion The protocol version to check
   * @return true if multiple catalogs are supported, false otherwise
   */
  public static boolean supportsMultipleCatalogs(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V4) >= 0;
  }

  /**
   * Checks if the given protocol version supports Arrow metadata in result sets.
   *
   * @param protocolVersion The protocol version to check
   * @return true if Arrow metadata is supported, false otherwise
   */
  public static boolean supportsArrowMetadata(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5) >= 0;
  }

  /**
   * Checks if the given protocol version supports getting result set metadata from fetch results.
   *
   * @param protocolVersion The protocol version to check
   * @return true if getting result set metadata from fetch is supported, false otherwise
   */
  public static boolean supportsResultSetMetadataFromFetch(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5) >= 0;
  }

  /**
   * Checks if the given protocol version supports advanced Arrow types.
   *
   * @param protocolVersion The protocol version to check
   * @return true if advanced Arrow types are supported, false otherwise
   */
  public static boolean supportsAdvancedArrowTypes(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V5) >= 0;
  }

  /**
   * Checks if the given protocol version supports compressed Arrow batches.
   *
   * @param protocolVersion The protocol version to check
   * @return true if compressed Arrow batches are supported, false otherwise
   */
  public static boolean supportsCompressedArrowBatches(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6) >= 0;
  }

  /**
   * Checks if the given protocol version supports async metadata execution.
   *
   * @param protocolVersion The protocol version to check
   * @return true if async metadata execution is supported, false otherwise
   */
  public static boolean supportsAsyncMetadataExecution(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V6) >= 0;
  }

  /**
   * Checks if the given protocol version supports result persistence mode.
   *
   * @param protocolVersion The protocol version to check
   * @return true if result persistence mode is supported, false otherwise
   */
  public static boolean supportsResultPersistenceMode(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V7) >= 0;
  }

  /**
   * Checks if the given protocol version supports parameterized queries.
   *
   * @param protocolVersion The protocol version to check
   * @return true if parameterized queries are supported, false otherwise
   */
  public static boolean supportsParameterizedQueries(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V8) >= 0;
  }

  /**
   * Checks if the given protocol version supports async metadata operations.
   *
   * @param protocolVersion The protocol version to check
   * @return true if async metadata operations are supported, false otherwise
   */
  public static boolean supportsAsyncMetadataOperations(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.SPARK_CLI_SERVICE_PROTOCOL_V9) >= 0;
  }

  /**
   * Checks if the given protocol version indicates a non-Databricks compute.
   *
   * @param protocolVersion The protocol version to check
   * @return true if this is a non-Databricks compute, false otherwise
   */
  public static boolean isNonDatabricksCompute(TProtocolVersion protocolVersion) {
    return protocolVersion.compareTo(TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V10) <= 0;
  }
}
